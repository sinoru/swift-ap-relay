@preconcurrency @unsafe import RediStack
import Vapor

/// Redis-backed implementation of ``ActivityDeduplicating``.
///
/// Uses `SET key value NX EX ttl` for atomic check-and-set with
/// automatic TTL expiration. Key schema: `activity_dedup:{activityID}`.
struct RedisActivityDeduplicator: ActivityDeduplicating, Sendable {
    let redis: any RedisClient & Sendable
    let ttl: Int

    init(redis: any RedisClient & Sendable, ttl: Int = 3600) {
        self.redis = redis
        self.ttl = ttl
    }

    func isDuplicate(_ activityID: String) async throws -> Bool {
        let key: RedisKey = "activity_dedup:\(activityID)"
        let result = try await redis.set(
            key,
            to: "1",
            onCondition: .keyDoesNotExist,
            expiration: .seconds(ttl)
        ).get()
        return result == .conditionNotMet
    }

    func forget(_ activityID: String) async throws {
        let key: RedisKey = "activity_dedup:\(activityID)"
        _ = try await redis.delete(key).get()
    }
}
