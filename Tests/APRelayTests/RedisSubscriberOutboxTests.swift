import Foundation
@preconcurrency @unsafe import RediStack
import Testing
import Vapor
@testable import APRelay

/// Integration tests for the subscriber outbox scripts. Need a live Redis;
/// see ``RedisRelayRepositoryTests``.
@Suite(
    "Redis Subscriber Outbox Tests",
    .serialized,
    .enabled(if: ProcessInfo.processInfo.environment["REDIS_TEST_URL"] != nil)
)
struct RedisSubscriberOutboxTests {
    private static let domain = "outbox.redis-test.example"
    private static let queueKey: RedisKey = "subscriber_outbox"
    private static let entriesKey: RedisKey = "subscriber_outbox:entries"

    private static func withRepository(
        _ body: (RedisRelayRepository, RedisConnection) async throws -> Void
    ) async throws {
        let url = try #require(ProcessInfo.processInfo.environment["REDIS_TEST_URL"])
        let connection = try await RedisConnection.make(
            configuration: .init(url: url),
            boundEventLoop: MultiThreadedEventLoopGroup.singleton.next()
        ).get()
        let repository = RedisRelayRepository(redis: connection)
        do {
            try await cleanUp(connection)
            try await body(repository, connection)
            try await cleanUp(connection)
        } catch {
            try? await cleanUp(connection)
            try? await connection.close().get()
            throw error
        }
        try await connection.close().get()
    }

    private static func cleanUp(_ connection: RedisConnection) async throws {
        _ = try await connection.delete([
            "subscriber:\(domain)",
            RedisSubscriberLock.lockKey(domain),
            RedisSubscriberLock.fenceKey(domain),
            queueKey,
            entriesKey,
        ]).get()
        _ = try await connection.srem(domain, from: "subscribers:all").get()
        _ = try await connection.srem(domain, from: "blocked_domains").get()
        for state in SubscriberState.allCases {
            _ = try await connection.srem(domain, from: "subscribers:state:\(state.rawValue)").get()
        }
    }

    private static let subscriber = Subscriber(
        domain: domain,
        inboxURL: "https://\(domain)/inbox",
        actorID: "https://\(domain)/actor",
        state: .rejected,
        followActivityID: "https://\(domain)/follow/1",
        createdAt: nil,
        updatedAt: nil
    )

    private static func lease(on connection: RedisConnection) async throws -> SubscriberLease {
        let token = UUID().uuidString
        let sequence = try await RedisSubscriberLock(redis: connection)
            .acquire(domain: domain, token: token, ttlSeconds: 60)
        return SubscriberLease(domain: domain, token: token, sequence: try #require(sequence))
    }

    private static func entries() -> [SubscriberOutboxEntry] {
        [
            SubscriberOutboxEntry(
                notification: .reject(
                    RejectPayload(
                        inboxURL: subscriber.inboxURL,
                        followActivityID: subscriber.followActivityID,
                        followerActorID: subscriber.actorID,
                        followObjectURI: nil,
                        activityID: "https://relay.redis-test.example/activities/reject-1"
                    )
                )
            ),
            SubscriberOutboxEntry(
                notification: .undoFollow(
                    UndoFollowPayload(
                        inboxURL: subscriber.inboxURL,
                        targetActorID: subscriber.actorID,
                        followActivityID: "https://relay.redis-test.example/activities/outbound-1",
                        activityID: "https://relay.redis-test.example/activities/undo-1"
                    )
                )
            ),
        ]
    }

    @Test("A write records its notifications reserved for the writer")
    func writeReservesNotifications() async throws {
        try await Self.withRepository { repository, connection in
            let lease = try await Self.lease(on: connection)
            let entries = Self.entries()
            let saved = try await repository.saveSubscriber(
                Self.subscriber, lease: lease, outbox: entries, leaseSeconds: 60
            )
            #expect(saved)

            let stored = try await connection.hlen(of: Self.entriesKey).get()
            let claimed = try await repository.claimOutboxEntries(limit: 10, leaseSeconds: 60)
            #expect(stored == 2)
            #expect(claimed.isEmpty)
        }
    }

    @Test("Lapsed notifications are claimed once, in order, and removed when completed")
    func claimAndComplete() async throws {
        try await Self.withRepository { repository, connection in
            let lease = try await Self.lease(on: connection)
            let entries = Self.entries()
            let saved = try await repository.saveSubscriber(
                Self.subscriber, lease: lease, outbox: entries, leaseSeconds: 0
            )
            try #require(saved)

            let claimed = try await repository.claimOutboxEntries(limit: 10, leaseSeconds: 60)
            #expect(claimed.map(\.id) == entries.map(\.id))
            if case .reject(let payload) = claimed.first?.notification {
                #expect(payload.followActivityID == Self.subscriber.followActivityID)
                #expect(payload.activityID == "https://relay.redis-test.example/activities/reject-1")
            } else {
                Issue.record("First claimed entry is not the Reject")
            }

            // Claimed entries are leased away from other claims.
            let again = try await repository.claimOutboxEntries(limit: 10, leaseSeconds: 60)
            #expect(again.isEmpty)

            for entry in claimed {
                try await repository.completeOutboxEntry(id: entry.id)
            }
            let queued = try await connection.zcard(of: Self.queueKey).get()
            let stored = try await connection.hlen(of: Self.entriesKey).get()
            #expect(queued == 0)
            #expect(stored == 0)
        }
    }

    @Test("A delete records its notifications")
    func deleteRecordsNotifications() async throws {
        try await Self.withRepository { repository, connection in
            let lease = try await Self.lease(on: connection)
            let entries = Self.entries()
            try await repository.deleteSubscriber(
                domain: Self.domain, lease: lease, outbox: entries, leaseSeconds: 0
            )

            let claimed = try await repository.claimOutboxEntries(limit: 10, leaseSeconds: 60)
            #expect(claimed.map(\.id) == entries.map(\.id))
        }
    }

    @Test("A write refused as superseded or blocked records no notifications")
    func refusedWriteRecordsNothing() async throws {
        try await Self.withRepository { repository, connection in
            let stale = try await Self.lease(on: connection)
            _ = try await connection.delete(RedisSubscriberLock.lockKey(Self.domain)).get()
            let later = try await Self.lease(on: connection)
            let written = try await repository.saveSubscriber(
                Self.subscriber, lease: later, outbox: [], leaseSeconds: 0
            )
            try #require(written)

            await #expect(throws: SubscriberLockError.self) {
                try await repository.saveSubscriber(
                    Self.subscriber, lease: stale, outbox: Self.entries(), leaseSeconds: 0
                )
            }

            _ = try await connection.sadd(Self.domain, to: "blocked_domains").get()
            let blocked = try await repository.saveSubscriber(
                Self.subscriber, lease: later, outbox: Self.entries(), leaseSeconds: 0
            )
            #expect(!blocked)

            let queued = try await connection.zcard(of: Self.queueKey).get()
            let stored = try await connection.hlen(of: Self.entriesKey).get()
            #expect(queued == 0)
            #expect(stored == 0)
        }
    }
}
