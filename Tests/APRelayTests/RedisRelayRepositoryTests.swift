import Foundation
@preconcurrency @unsafe import RediStack
import Testing
import Vapor
@testable import APRelay

/// Integration tests for the Lua-scripted subscriber writes.
///
/// These need a live Redis and only run when `REDIS_TEST_URL` is set, e.g.
/// `REDIS_TEST_URL=redis://127.0.0.1:6379/15 swift test`. They only touch
/// keys for the `*.redis-test.example` domains and delete them on exit.
@Suite(
    "Redis Relay Repository Tests",
    .serialized,
    .enabled(if: ProcessInfo.processInfo.environment["REDIS_TEST_URL"] != nil)
)
struct RedisRelayRepositoryTests {
    private static let domain = "sub.redis-test.example"
    private static let otherDomain = "other.redis-test.example"
    private static let settingKey = "redis-test.setting"

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
        for domain in [domain, otherDomain] {
            _ = try await connection.delete("subscriber:\(domain)").get()
            _ = try await connection.srem(domain, from: "subscribers:all").get()
            _ = try await connection.srem(domain, from: "blocked_domains").get()
            for state in SubscriberState.allCases {
                _ = try await connection.srem(domain, from: "subscribers:state:\(state.rawValue)").get()
            }
        }
        _ = try await connection.hdel(settingKey, from: "relay_settings").get()
    }

    private static func makeSubscriber(
        state: SubscriberState,
        createdAt: Date? = nil
    ) -> Subscriber {
        Subscriber(
            domain: domain,
            inboxURL: "https://\(domain)/inbox",
            actorID: "https://\(domain)/actor",
            state: state,
            followActivityID: "https://\(domain)/follow/1",
            followObjectURI: "https://www.w3.org/ns/activitystreams#Public",
            outboundFollowActivityID: nil,
            createdAt: createdAt,
            updatedAt: nil
        )
    }

    private static func isMember(
        _ domain: String,
        of key: RedisKey,
        on connection: RedisConnection
    ) async throws -> Bool {
        try await connection.sismember(domain, of: key).get()
    }

    @Test("Save writes the hash and every index, and moves between state sets")
    func saveAndMoveState() async throws {
        try await Self.withRepository { repository, connection in
            let created = Date(timeIntervalSince1970: 1_700_000_000)
            let saved = try await repository.saveSubscriber(
                Self.makeSubscriber(state: .pending, createdAt: created)
            )
            #expect(saved)

            let pending = try await repository.getSubscriber(domain: Self.domain)
            #expect(pending?.state == .pending)
            #expect(pending?.createdAt == created)
            let inAll = try await Self.isMember(Self.domain, of: "subscribers:all", on: connection)
            let inPending = try await Self.isMember(Self.domain, of: "subscribers:state:pending", on: connection)
            #expect(inAll)
            #expect(inPending)

            // Re-saving with another state moves the index entry and keeps createdAt.
            let resaved = try await repository.saveSubscriber(
                Self.makeSubscriber(state: .accepted, createdAt: Date())
            )
            #expect(resaved)

            let accepted = try await repository.getSubscriber(domain: Self.domain)
            #expect(accepted?.state == .accepted)
            #expect(accepted?.createdAt == created)
            let stillPending = try await Self.isMember(Self.domain, of: "subscribers:state:pending", on: connection)
            let inAccepted = try await Self.isMember(Self.domain, of: "subscribers:state:accepted", on: connection)
            #expect(!stillPending)
            #expect(inAccepted)
            let inboxURLs = try await repository.getAcceptedInboxURLs()
            #expect(inboxURLs == ["https://\(Self.domain)/inbox"])
        }
    }

    @Test("Save is refused as a whole for a blocked domain")
    func saveRefusedWhenBlocked() async throws {
        try await Self.withRepository { repository, connection in
            _ = try await connection.sadd(Self.domain, to: "blocked_domains").get()

            let saved = try await repository.saveSubscriber(Self.makeSubscriber(state: .accepted))
            #expect(!saved)

            let stored = try await repository.getSubscriber(domain: Self.domain)
            #expect(stored == nil)
            let inAll = try await Self.isMember(Self.domain, of: "subscribers:all", on: connection)
            let inAccepted = try await Self.isMember(Self.domain, of: "subscribers:state:accepted", on: connection)
            #expect(!inAll)
            #expect(!inAccepted)
        }
    }

    @Test("Delete removes the hash and every index entry, including stale ones")
    func deleteRemovesEverything() async throws {
        try await Self.withRepository { repository, connection in
            let saved = try await repository.saveSubscriber(Self.makeSubscriber(state: .accepted))
            #expect(saved)
            // A stale entry left by an interrupted write is cleaned up too.
            _ = try await connection.sadd(Self.domain, to: "subscribers:state:rejected").get()

            try await repository.deleteSubscriber(domain: Self.domain)

            let stored = try await repository.getSubscriber(domain: Self.domain)
            #expect(stored == nil)
            let inAll = try await Self.isMember(Self.domain, of: "subscribers:all", on: connection)
            #expect(!inAll)
            for state in SubscriberState.allCases {
                let inState = try await Self.isMember(
                    Self.domain, of: "subscribers:state:\(state.rawValue)", on: connection
                )
                #expect(!inState, "still listed in \(state.rawValue)")
            }
        }
    }

    @Test("Delete of an unknown domain is a no-op")
    func deleteUnknownDomain() async throws {
        try await Self.withRepository { repository, _ in
            try await repository.deleteSubscriber(domain: Self.otherDomain)
            let stored = try await repository.getSubscriber(domain: Self.otherDomain)
            #expect(stored == nil)
        }
    }

    @Test("setSettingIfAbsent stores only the first value")
    func setSettingIfAbsent() async throws {
        try await Self.withRepository { repository, _ in
            let first = try await repository.setSettingIfAbsent(key: Self.settingKey, value: "first")
            let second = try await repository.setSettingIfAbsent(key: Self.settingKey, value: "second")
            let stored = try await repository.getSetting(key: Self.settingKey)
            #expect(first)
            #expect(!second)
            #expect(stored == "first")
        }
    }
}
