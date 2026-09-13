import Foundation
@preconcurrency @unsafe import RediStack
import Testing
import Vapor
@testable import APRelay

/// Integration tests for the subscriber lock key. Need a live Redis; see
/// ``RedisRelayRepositoryTests``.
@Suite(
    "Redis Subscriber Lock Tests",
    .serialized,
    .enabled(if: ProcessInfo.processInfo.environment["REDIS_TEST_URL"] != nil)
)
struct RedisSubscriberLockTests {
    private static let domain = "lock.redis-test.example"
    private static let key: RedisKey = "subscriber_lock:\(domain)"
    private static let fenceKey: RedisKey = "subscriber_fence:\(domain)"

    private static func withLock(
        _ body: (RedisSubscriberLock, RedisConnection) async throws -> Void
    ) async throws {
        let url = try #require(ProcessInfo.processInfo.environment["REDIS_TEST_URL"])
        let connection = try await RedisConnection.make(
            configuration: .init(url: url),
            boundEventLoop: MultiThreadedEventLoopGroup.singleton.next()
        ).get()
        let lock = RedisSubscriberLock(redis: connection)
        do {
            _ = try await connection.delete([key, fenceKey]).get()
            try await body(lock, connection)
            _ = try await connection.delete([key, fenceKey]).get()
        } catch {
            _ = try? await connection.delete([key, fenceKey]).get()
            try? await connection.close().get()
            throw error
        }
        try await connection.close().get()
    }

    @Test("Only one holder gets the lock, and it expires")
    func acquire() async throws {
        try await Self.withLock { lock, connection in
            let first = try await lock.acquire(domain: Self.domain, token: "a", ttlSeconds: 60)
            let second = try await lock.acquire(domain: Self.domain, token: "b", ttlSeconds: 60)
            let ttl = try await connection.ttl(Self.key).get()
            #expect(first == 1)
            #expect(second == nil)
            let seconds = try #require(ttl.timeAmount).nanoseconds / 1_000_000_000
            #expect(seconds > 0 && seconds <= 60)
        }
    }

    @Test("Every acquisition gets a higher sequence, even after the lock expires")
    func sequencesIncrease() async throws {
        try await Self.withLock { lock, connection in
            let first = try await lock.acquire(domain: Self.domain, token: "a", ttlSeconds: 60)
            // The lock lapses rather than being released.
            _ = try await connection.delete(Self.key).get()
            let second = try await lock.acquire(domain: Self.domain, token: "b", ttlSeconds: 60)
            let fenceTTL = try await connection.ttl(Self.fenceKey).get()

            #expect(first == 1)
            #expect(second == 2)
            // The fence never expires, so no sequence is issued twice.
            #expect(fenceTTL == .unlimited)
        }
    }

    @Test("Release frees the lock only for the holder's token")
    func release() async throws {
        try await Self.withLock { lock, connection in
            _ = try await lock.acquire(domain: Self.domain, token: "a", ttlSeconds: 60)

            try await lock.release(domain: Self.domain, token: "b")
            let afterForeignRelease = try await connection.get(Self.key, as: String.self).get()
            #expect(afterForeignRelease == "a")

            try await lock.release(domain: Self.domain, token: "a")
            let reacquired = try await lock.acquire(domain: Self.domain, token: "b", ttlSeconds: 60)
            #expect(reacquired == 2)
        }
    }
}
