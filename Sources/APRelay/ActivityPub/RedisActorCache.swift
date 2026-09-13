import Foundation
@preconcurrency @unsafe import RediStack
import Vapor

/// Redis-backed implementation of ``ActorCaching``.
///
/// Key schema:
/// - `actor:{id}` — JSON ``VerifiedActor`` for a canonical actor id
/// - `actor:alias:{url}` — canonical id a key id URL resolved to
/// - `actor:negative:document:{url}` — present while a URL is known to yield no usable document
/// - `actor:negative:authority:{url}` — present while a URL is known not to vouch for itself
/// - `actor:fetching:{url}` — token of the replica currently fetching the URL
/// - `actor:refetch:{id}` — ``RefetchState`` while a re-fetch of the actor is not allowed
struct RedisActorCache: ActorCaching, Sendable {
    let redis: any RedisClient & Sendable

    private static let encoder = JSONEncoder()
    private static let decoder = JSONDecoder()

    private func actorKey(_ id: String) -> RedisKey { "actor:\(id)" }
    private func aliasKey(_ url: String) -> RedisKey { "actor:alias:\(url)" }
    /// Both scopes carry their own segment, so no URL can spell out the key
    /// of another URL's entry in the other scope.
    private func negativeKey(_ url: String, _ scope: NegativeScope) -> RedisKey {
        switch scope {
        case .document: "actor:negative:document:\(url)"
        case .authority: "actor:negative:authority:\(url)"
        }
    }
    private func fetchLockKey(_ url: String) -> RedisKey { "actor:fetching:\(url)" }
    private func refetchKey(_ id: String) -> RedisKey { "actor:refetch:\(id)" }

    func actor(id: String) async throws -> VerifiedActor? {
        guard let json = try await redis.get(actorKey(id), as: String.self).get(),
            let data = json.data(using: .utf8)
        else {
            return nil
        }
        return try Self.decoder.decode(VerifiedActor.self, from: data)
    }

    func canonicalID(forURL url: String) async throws -> String? {
        try await redis.get(aliasKey(url), as: String.self).get()
    }

    func store(_ actor: VerifiedActor, ttlSeconds: Int) async throws {
        let data = try Self.encoder.encode(actor)
        guard let json = String(data: data, encoding: .utf8) else { return }
        try await redis.setex(actorKey(actor.id), to: json, expirationInSeconds: ttlSeconds).get()
    }

    func recordAlias(url: String, canonicalID: String, ttlSeconds: Int) async throws {
        try await redis.setex(aliasKey(url), to: canonicalID, expirationInSeconds: ttlSeconds).get()
    }

    func removeAlias(url: String) async throws {
        _ = try await redis.delete(aliasKey(url)).get()
    }

    func evict(id: String) async throws {
        _ = try await redis.delete(actorKey(id)).get()
    }

    func isNegative(url: String, scope: NegativeScope) async throws -> Bool {
        try await redis.exists(negativeKey(url, scope)).get() > 0
    }

    func markNegative(url: String, scope: NegativeScope, ttlSeconds: Int) async throws {
        try await redis.setex(
            negativeKey(url, scope), to: "1", expirationInSeconds: ttlSeconds
        ).get()
    }

    func acquireFetchLock(url: String, token: String, ttlSeconds: Int) async throws -> Bool {
        let result = try await redis.set(
            fetchLockKey(url),
            to: token,
            onCondition: .keyDoesNotExist,
            expiration: .seconds(ttlSeconds)
        ).get()
        return result == .ok
    }

    func releaseFetchLock(url: String, token: String) async throws {
        _ = try await redis.evaluate(
            Self.releaseFetchLockScript,
            keys: [fetchLockKey(url)],
            arguments: [token]
        )
    }

    func acquireRefetch(id: String, holdSeconds: Int) async throws -> Bool {
        let result = try await redis.set(
            refetchKey(id),
            to: RefetchState.refreshing.rawValue,
            onCondition: .keyDoesNotExist,
            expiration: .seconds(holdSeconds)
        ).get()
        return result == .ok
    }

    func settleRefetch(id: String, holdSeconds: Int) async throws {
        try await redis.setex(
            refetchKey(id), to: RefetchState.settled.rawValue, expirationInSeconds: holdSeconds
        ).get()
    }

    func releaseRefetch(id: String) async throws {
        _ = try await redis.evaluate(
            Self.releaseRefetchScript,
            keys: [refetchKey(id)],
            arguments: [RefetchState.refreshing.rawValue]
        )
    }

    func refetchState(id: String) async throws -> RefetchState? {
        guard let raw = try await redis.get(refetchKey(id), as: String.self).get() else {
            return nil
        }
        guard let state = RefetchState(rawValue: raw) else {
            throw ActorCacheError.unexpectedValue(raw)
        }
        return state
    }

    /// KEYS: [1] re-fetch hold. ARGV: [1] the refreshing state. Deletes the
    /// hold only while it is still the claim of a refresh in progress, so a
    /// hold settled by a fetch in the meantime is kept.
    private static let releaseRefetchScript = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        end
        return 0
        """

    /// KEYS: [1] fetch claim. ARGV: [1] token. Deletes the claim only while it
    /// is still held under this token, so a holder that outlived its claim
    /// does not release the next holder's.
    private static let releaseFetchLockScript = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        end
        return 0
        """
}

enum ActorCacheError: Error {
    /// A cache key held a value this version does not understand.
    case unexpectedValue(String)
}
