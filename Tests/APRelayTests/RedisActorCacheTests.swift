import Foundation
@preconcurrency @unsafe import RediStack
import Testing
import Vapor
@testable import APRelay

/// Integration tests for the actor cache keys. Need a live Redis; see
/// ``RedisRelayRepositoryTests``.
@Suite(
    "Redis Actor Cache Tests",
    .serialized,
    .enabled(if: ProcessInfo.processInfo.environment["REDIS_TEST_URL"] != nil)
)
struct RedisActorCacheTests {
    private static let actorID = "https://actor.redis-test.example/users/alice"
    private static let aliasURL = "https://actor.redis-test.example/users/alice-old"
    private static let actor = VerifiedActor(
        id: actorID,
        inbox: "https://actor.redis-test.example/users/alice/inbox",
        sharedInbox: "https://actor.redis-test.example/inbox",
        publicKeyPEM: "-----BEGIN PUBLIC KEY-----\ntest\n-----END PUBLIC KEY-----\n"
    )

    private static func withCache(
        _ body: (RedisActorCache, RedisConnection) async throws -> Void
    ) async throws {
        let url = try #require(ProcessInfo.processInfo.environment["REDIS_TEST_URL"])
        let connection = try await RedisConnection.make(
            configuration: .init(url: url),
            boundEventLoop: MultiThreadedEventLoopGroup.singleton.next()
        ).get()
        let cache = RedisActorCache(redis: connection)
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
            "actor:\(actorID)",
            "actor:alias:\(aliasURL)",
            "actor:negative:document:\(aliasURL)",
            "actor:negative:authority:\(aliasURL)",
            "actor:fetching:\(aliasURL)",
            "actor:refetch:\(actorID)",
        ]).get()
    }

    @Test("A stored actor round-trips and its alias resolves to it")
    func storeAndLookup() async throws {
        try await Self.withCache { cache, connection in
            try await cache.store(Self.actor, ttlSeconds: 60)
            try await cache.recordAlias(url: Self.aliasURL, canonicalID: Self.actorID, ttlSeconds: 60)

            let direct = try await cache.actor(id: Self.actorID)
            let alias = try await cache.canonicalID(forURL: Self.aliasURL)
            #expect(direct == Self.actor)
            #expect(alias == Self.actorID)

            let actorTTL = try await connection.ttl("actor:\(Self.actorID)").get()
            let aliasTTL = try await connection.ttl("actor:alias:\(Self.aliasURL)").get()
            #expect(actorTTL.timeAmount != nil)
            #expect(aliasTTL.timeAmount != nil)
        }
    }

    @Test("Recording an alias leaves the actor entry's TTL alone")
    func aliasDoesNotRenewActor() async throws {
        try await Self.withCache { cache, connection in
            try await cache.store(Self.actor, ttlSeconds: 60)
            try await cache.recordAlias(url: Self.aliasURL, canonicalID: Self.actorID, ttlSeconds: 3600)

            let actorTTL = try await connection.ttl("actor:\(Self.actorID)").get()
            let seconds = try #require(actorTTL.timeAmount).nanoseconds / 1_000_000_000
            #expect(seconds <= 60)
        }
    }

    @Test("Evicting drops the actor and leaves the alias unresolved")
    func evict() async throws {
        try await Self.withCache { cache, _ in
            try await cache.store(Self.actor, ttlSeconds: 60)
            try await cache.recordAlias(url: Self.aliasURL, canonicalID: Self.actorID, ttlSeconds: 60)
            try await cache.evict(id: Self.actorID)

            let direct = try await cache.actor(id: Self.actorID)
            let alias = try await cache.canonicalID(forURL: Self.aliasURL)
            #expect(direct == nil)
            #expect(alias == Self.actorID)
        }
    }

    @Test("Removing an alias leaves the actor entry in place")
    func removeAlias() async throws {
        try await Self.withCache { cache, _ in
            try await cache.store(Self.actor, ttlSeconds: 60)
            try await cache.recordAlias(url: Self.aliasURL, canonicalID: Self.actorID, ttlSeconds: 60)
            try await cache.removeAlias(url: Self.aliasURL)

            let alias = try await cache.canonicalID(forURL: Self.aliasURL)
            let direct = try await cache.actor(id: Self.actorID)
            #expect(alias == nil)
            #expect(direct == Self.actor)
        }
    }

    @Test("Negative entries are visible per scope until their TTL passes")
    func negativeEntry() async throws {
        try await Self.withCache { cache, connection in
            let before = try await cache.isNegative(url: Self.aliasURL, scope: .document)
            #expect(!before)

            try await cache.markNegative(url: Self.aliasURL, scope: .authority, ttlSeconds: 60)
            let authority = try await cache.isNegative(url: Self.aliasURL, scope: .authority)
            let document = try await cache.isNegative(url: Self.aliasURL, scope: .document)
            let ttl = try await connection.ttl("actor:negative:authority:\(Self.aliasURL)").get()
            #expect(authority)
            #expect(!document)
            #expect(ttl.timeAmount != nil)

            try await cache.markNegative(url: Self.aliasURL, scope: .document, ttlSeconds: 60)
            let documentAfter = try await cache.isNegative(url: Self.aliasURL, scope: .document)
            let documentKey = try await connection.exists("actor:negative:document:\(Self.aliasURL)").get()
            #expect(documentAfter)
            #expect(documentKey == 1)
        }
    }

    @Test("The fetch claim is exclusive and released only by its holder")
    func fetchLock() async throws {
        try await Self.withCache { cache, _ in
            let first = try await cache.acquireFetchLock(url: Self.aliasURL, token: "a", ttlSeconds: 60)
            let second = try await cache.acquireFetchLock(url: Self.aliasURL, token: "b", ttlSeconds: 60)
            #expect(first)
            #expect(!second)

            try await cache.releaseFetchLock(url: Self.aliasURL, token: "b")
            let stillHeld = try await cache.acquireFetchLock(url: Self.aliasURL, token: "c", ttlSeconds: 60)
            #expect(!stillHeld)

            try await cache.releaseFetchLock(url: Self.aliasURL, token: "a")
            let released = try await cache.acquireFetchLock(url: Self.aliasURL, token: "c", ttlSeconds: 60)
            #expect(released)
        }
    }

    @Test("A settled hold blocks the re-fetch claim, which is then exclusive and refreshing")
    func refetchHold() async throws {
        try await Self.withCache { cache, _ in
            let none = try await cache.refetchState(id: Self.actorID)
            #expect(none == nil)

            try await cache.settleRefetch(id: Self.actorID, holdSeconds: 60)
            let settled = try await cache.refetchState(id: Self.actorID)
            let heldByFetch = try await cache.acquireRefetch(id: Self.actorID, holdSeconds: 60)
            #expect(settled == .settled)
            #expect(!heldByFetch)

            try await Self.cleanUp(try #require(cache.redis as? RedisConnection))
            let first = try await cache.acquireRefetch(id: Self.actorID, holdSeconds: 60)
            let refreshing = try await cache.refetchState(id: Self.actorID)
            let second = try await cache.acquireRefetch(id: Self.actorID, holdSeconds: 60)
            #expect(first)
            #expect(refreshing == .refreshing)
            #expect(!second)

            try await cache.settleRefetch(id: Self.actorID, holdSeconds: 60)
            let settledAgain = try await cache.refetchState(id: Self.actorID)
            #expect(settledAgain == .settled)
        }
    }

    @Test("Releasing a hold gives back a refresh in progress but keeps a settled one")
    func releaseRefetch() async throws {
        try await Self.withCache { cache, _ in
            _ = try await cache.acquireRefetch(id: Self.actorID, holdSeconds: 60)
            try await cache.releaseRefetch(id: Self.actorID)
            let released = try await cache.refetchState(id: Self.actorID)
            #expect(released == nil)

            try await cache.settleRefetch(id: Self.actorID, holdSeconds: 60)
            try await cache.releaseRefetch(id: Self.actorID)
            let kept = try await cache.refetchState(id: Self.actorID)
            #expect(kept == .settled)
        }
    }
}
