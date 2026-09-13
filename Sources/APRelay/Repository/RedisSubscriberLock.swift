@preconcurrency @unsafe import RediStack
import Vapor

/// Redis-backed implementation of ``SubscriberLocking``.
///
/// Key schema:
/// - `subscriber_lock:{domain}` — token of the holder currently changing the subscriber
/// - `subscriber_fence:{domain}` — hash with `issued`, the last sequence handed to a
///   holder, and `written`, the sequence of the last holder that wrote the subscriber
///
/// The fence hash has no expiry: a sequence must never be reissued, or a
/// stalled holder could pass the fence again. It holds two integers per domain
/// that ever changed a subscriber.
struct RedisSubscriberLock: SubscriberLocking, Sendable {
    let redis: any RedisClient & Sendable

    static func lockKey(_ domain: String) -> RedisKey { "subscriber_lock:\(domain)" }

    /// The fence hash for `domain`, shared with the repository scripts that
    /// fence subscriber writes against it.
    static func fenceKey(_ domain: String) -> RedisKey { "subscriber_fence:\(domain)" }

    func acquire(domain: String, token: String, ttlSeconds: Int) async throws -> Int? {
        let result = try await redis.evaluate(
            Self.acquireScript,
            keys: [Self.lockKey(domain), Self.fenceKey(domain)],
            arguments: [token, String(ttlSeconds)]
        )
        guard let sequence = result.int, sequence > 0 else {
            return nil
        }
        return sequence
    }

    func release(domain: String, token: String) async throws {
        try await redis.delete(Self.lockKey(domain), ifEqualTo: token)
    }

    /// KEYS: [1] lock, [2] fence hash. ARGV: [1] token, [2] TTL in seconds.
    ///
    /// Takes the lock and issues the next sequence in one step, so sequences
    /// follow the order in which holders got the lock. Returns the sequence,
    /// or 0 when the lock is held.
    private static let acquireScript = """
        if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2]) then
            return redis.call('HINCRBY', KEYS[2], 'issued', 1)
        end
        return 0
        """
}
