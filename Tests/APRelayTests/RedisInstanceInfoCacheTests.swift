import Foundation
@preconcurrency @unsafe import RediStack
import Testing
import Vapor
@testable import APRelay

/// Integration tests for the instance info coordination keys and the
/// failure-recording script. Need a live Redis; see ``RedisRelayRepositoryTests``.
@Suite(
    "Redis Instance Info Cache Tests",
    .serialized,
    .enabled(if: ProcessInfo.processInfo.environment["REDIS_TEST_URL"] != nil)
)
struct RedisInstanceInfoCacheTests {
    private static let domain = "info.redis-test.example"

    private static func withCache(
        _ body: (RedisInstanceInfoCache, RedisConnection) async throws -> Void
    ) async throws {
        let url = try #require(ProcessInfo.processInfo.environment["REDIS_TEST_URL"])
        let connection = try await RedisConnection.make(
            configuration: .init(url: url),
            boundEventLoop: MultiThreadedEventLoopGroup.singleton.next()
        ).get()
        let cache = RedisInstanceInfoCache(redis: connection, ttlSeconds: 3600)
        do {
            try await cleanUp(connection)
            try await body(cache, connection)
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
            "instanceinfo:\(domain)",
            "instanceinfo:fetch:\(domain)",
            "instanceinfo:check:tick",
        ]).get()
    }

    @Test("Only one replica acquires the tick claim until it expires")
    func tickClaim() async throws {
        try await Self.withCache { cache, connection in
            let first = try await cache.acquireCheckTick(ttlSeconds: 60)
            let second = try await cache.acquireCheckTick(ttlSeconds: 60)
            let ttl = try await connection.ttl("instanceinfo:check:tick").get()
            #expect(first)
            #expect(!second)
            #expect(ttl.timeAmount != nil)
        }
    }

    @Test("Fetch claim is exclusive, renewed only while queued, and released only by its token")
    func fetchClaim() async throws {
        try await Self.withCache { cache, connection in
            let claimKey: RedisKey = "instanceinfo:fetch:\(Self.domain)"

            let claimed = try await cache.claimFetch(domain: Self.domain, token: "one", ttlSeconds: 60)
            let claimedAgain = try await cache.claimFetch(domain: Self.domain, token: "two", ttlSeconds: 60)
            #expect(claimed)
            #expect(!claimedAgain)
            let queued = try await cache.pendingFetch(domain: Self.domain)
            #expect(queued == PendingFetch(token: "one", isRunning: false))

            // Only the holder's token renews the queued claim.
            try await cache.renewFetch(domain: Self.domain, token: "two", ttlSeconds: 600)
            let ttlAfterWrongToken = try await connection.ttl(claimKey).get()
            #expect((ttlAfterWrongToken.timeAmount ?? .zero) <= .seconds(60))
            try await cache.renewFetch(domain: Self.domain, token: "one", ttlSeconds: 600)
            let ttlAfterRenew = try await connection.ttl(claimKey).get()
            #expect((ttlAfterRenew.timeAmount ?? .zero) > .seconds(60))

            // Only the holder starts the fetch; the claim then carries a short running lease.
            let otherStarts = try await cache.startFetch(domain: Self.domain, token: "two", leaseSeconds: 300)
            #expect(!otherStarts)
            let starts = try await cache.startFetch(domain: Self.domain, token: "one", leaseSeconds: 300)
            #expect(starts)
            let running = try await cache.pendingFetch(domain: Self.domain)
            #expect(running == PendingFetch(token: "one", isRunning: true))
            let leaseTTL = try await connection.ttl(claimKey).get()
            #expect((leaseTTL.timeAmount ?? .zero) <= .seconds(300))

            // A running claim is not renewed by the tick's queued-claim renewal.
            try await cache.renewFetch(domain: Self.domain, token: "one", ttlSeconds: 3600)
            let ttlAfterRunningRenew = try await connection.ttl(claimKey).get()
            #expect((ttlAfterRunningRenew.timeAmount ?? .zero) <= .seconds(300))

            // Release: wrong token leaves the running claim; the holder's token clears it.
            try await cache.releaseFetch(domain: Self.domain, token: "two")
            let stillHeld = try await cache.claimFetch(domain: Self.domain, token: "three", ttlSeconds: 60)
            #expect(!stillHeld)
            try await cache.releaseFetch(domain: Self.domain, token: "one")
            let released = try await cache.pendingFetch(domain: Self.domain)
            #expect(released == nil)

            // With no claim held, a lapsed job may start straight into a running lease.
            let retaken = try await cache.startFetch(domain: Self.domain, token: "four", leaseSeconds: 300)
            #expect(retaken)
            let retakenClaim = try await cache.pendingFetch(domain: Self.domain)
            #expect(retakenClaim == PendingFetch(token: "four", isRunning: true))
        }
    }

    @Test("Recording a failure keeps metadata, counts up, and picks the next attempt for the count")
    func recordFailure() async throws {
        try await Self.withCache { cache, connection in
            let now = Date(timeIntervalSince1970: 1_800_000_000)
            let schedule = [60.0, 120.0, 240.0].map { now.addingTimeInterval($0) }

            // No entry yet: first failure.
            let first = try await cache.recordFailure(domain: Self.domain, at: now, nextAttemptAt: schedule)
            #expect(first.consecutiveFailures == 1)
            #expect(!first.isReachable)
            #expect(first.lastCheckedAt == now)
            #expect(first.nextAttemptAt == schedule[0])
            #expect(first.softwareName == nil)

            // Existing entry with metadata, an empty staff list and null fields.
            try await cache.setInstanceInfo(
                domain: Self.domain,
                info: InstanceInfo(
                    softwareName: "mastodon",
                    softwareVersion: "4.3.0",
                    openRegistrations: false,
                    staffAccounts: [],
                    faviconURL: nil,
                    isReachable: true,
                    lastCheckedAt: now.addingTimeInterval(-60),
                    consecutiveFailures: 4
                )
            )
            let later = now.addingTimeInterval(30)
            let fifth = try await cache.recordFailure(domain: Self.domain, at: later, nextAttemptAt: schedule)
            #expect(fifth.consecutiveFailures == 5)
            #expect(!fifth.isReachable)
            #expect(fifth.lastCheckedAt == later)
            // Beyond the schedule: the last entry applies.
            #expect(fifth.nextAttemptAt == schedule[2])
            #expect(fifth.softwareName == "mastodon")
            #expect(fifth.softwareVersion == "4.3.0")
            #expect(fifth.openRegistrations == false)
            #expect(fifth.faviconURL == nil)
            #expect(fifth.staffAccounts ?? [] == [])

            // What the script stored decodes through the regular read path and keeps its TTL.
            let stored = try #require(try await cache.getInstanceInfo(domain: Self.domain))
            #expect(stored.consecutiveFailures == 5)
            #expect(stored.nextAttemptAt == schedule[2])
            let ttl = try await connection.ttl("instanceinfo:\(Self.domain)").get()
            #expect(ttl.timeAmount != nil)
        }
    }
}
