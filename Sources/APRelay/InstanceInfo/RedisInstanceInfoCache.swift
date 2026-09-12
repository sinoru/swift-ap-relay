import Foundation
@preconcurrency @unsafe import RediStack
import Vapor

/// Redis-backed implementation of ``InstanceInfoCaching``.
///
/// Key schema:
/// - `instanceinfo:{domain}` — JSON string with the domain's ``InstanceInfo``
/// - `instanceinfo:check:tick` — held by the replica running the current check tick
/// - `instanceinfo:fetch:{domain}` — job id of the pending tick-dispatched fetch,
///   suffixed with `:running` once that job has started
struct RedisInstanceInfoCache: InstanceInfoCaching, Sendable {
    /// Fixed TTL for cached entries. Decoupled from the check interval so that a
    /// tighter heartbeat cadence does not shrink the safety-net lifetime used to
    /// evict stale data for departed or long-offline instances.
    static let defaultTTLSeconds: Int = 3600

    let redis: any RedisClient & Sendable
    let ttlSeconds: Int

    private static let encoder: JSONEncoder = {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        return encoder
    }()

    private static let decoder: JSONDecoder = {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return decoder
    }()

    /// Matches the `.iso8601` encoding strategy, so dates written by the
    /// failure script decode like dates written by the encoder.
    private static let dateFormatStyle = Date.ISO8601FormatStyle()

    private func key(_ domain: String) -> RedisKey { "instanceinfo:\(domain)" }
    private var checkTickKey: RedisKey { "instanceinfo:check:tick" }
    private func fetchClaimKey(_ domain: String) -> RedisKey { "instanceinfo:fetch:\(domain)" }

    func getInstanceInfo(domain: String) async throws -> InstanceInfo? {
        let data = try await redis.get(key(domain), as: String.self).get()
        guard let json = data, let jsonData = json.data(using: .utf8) else { return nil }
        return try Self.decoder.decode(InstanceInfo.self, from: jsonData)
    }

    func setInstanceInfo(domain: String, info: InstanceInfo) async throws {
        let data = try Self.encoder.encode(info)
        guard let json = String(data: data, encoding: .utf8) else { return }
        try await redis.setex(key(domain), to: json, expirationInSeconds: ttlSeconds).get()
    }

    func getAllInstanceInfo(domains: [String]) async throws -> [String: InstanceInfo] {
        guard !domains.isEmpty else { return [:] }

        let keys = domains.map { key($0) }
        let values = try await redis.mget(keys, as: String.self).get()

        var result: [String: InstanceInfo] = [:]
        for (domain, value) in zip(domains, values) {
            guard let json = value, let jsonData = json.data(using: .utf8) else { continue }
            if let info = try? Self.decoder.decode(InstanceInfo.self, from: jsonData) {
                result[domain] = info
            }
        }
        return result
    }

    func recordFailure(domain: String, at now: Date, nextAttemptAt: [Date]) async throws -> InstanceInfo {
        precondition(!nextAttemptAt.isEmpty, "recordFailure needs at least one next-attempt date")
        let result = try await redis.evaluate(
            Self.recordFailureScript,
            keys: [key(domain)],
            arguments: [now.formatted(Self.dateFormatStyle), String(ttlSeconds)]
                + nextAttemptAt.map { $0.formatted(Self.dateFormatStyle) }
        )
        guard let json = result.string, let data = json.data(using: .utf8) else {
            throw InstanceInfoCacheError.unexpectedScriptResult
        }
        return try Self.decoder.decode(InstanceInfo.self, from: data)
    }

    // MARK: - Check Coordination

    func acquireCheckTick(ttlSeconds: Int) async throws -> Bool {
        let result = try await redis.set(
            checkTickKey,
            to: "1",
            onCondition: .keyDoesNotExist,
            expiration: .seconds(ttlSeconds)
        ).get()
        return result == .ok
    }

    func claimFetch(domain: String, token: String, ttlSeconds: Int) async throws -> Bool {
        let result = try await redis.set(
            fetchClaimKey(domain),
            to: token,
            onCondition: .keyDoesNotExist,
            expiration: .seconds(ttlSeconds)
        ).get()
        return result == .ok
    }

    func pendingFetch(domain: String) async throws -> PendingFetch? {
        guard let value = try await redis.get(fetchClaimKey(domain), as: String.self).get() else {
            return nil
        }
        if value.hasSuffix(Self.runningSuffix) {
            return PendingFetch(token: String(value.dropLast(Self.runningSuffix.count)), isRunning: true)
        }
        return PendingFetch(token: value, isRunning: false)
    }

    func renewFetch(domain: String, token: String, ttlSeconds: Int) async throws {
        _ = try await redis.evaluate(
            Self.renewFetchScript,
            keys: [fetchClaimKey(domain)],
            arguments: [token, String(ttlSeconds)]
        )
    }

    func startFetch(domain: String, token: String, leaseSeconds: Int) async throws -> Bool {
        let result = try await redis.evaluate(
            Self.startFetchScript,
            keys: [fetchClaimKey(domain)],
            arguments: [token, Self.runningSuffix, String(leaseSeconds)]
        )
        return result.int == 1
    }

    func releaseFetch(domain: String, token: String) async throws {
        _ = try await redis.evaluate(
            Self.releaseFetchScript,
            keys: [fetchClaimKey(domain)],
            arguments: [token, Self.runningSuffix]
        )
    }

    // MARK: - Atomic Scripts

    /// KEYS: [1] instance info entry. ARGV: [1] lastCheckedAt, [2] entry TTL in
    /// seconds, [3...] nextAttemptAt candidates indexed by the new failure
    /// count (the last one applies to every higher count).
    ///
    /// Reading, incrementing, and writing back in one script keeps two
    /// concurrent failures from losing an increment. Returns the stored JSON.
    private static let recordFailureScript = """
        local info = {}
        local raw = redis.call('GET', KEYS[1])
        if raw then
            info = cjson.decode(raw)
        end
        local failures = (info.consecutiveFailures or 0) + 1
        info.consecutiveFailures = failures
        info.isReachable = false
        info.lastCheckedAt = ARGV[1]
        info.nextAttemptAt = ARGV[2 + math.min(failures, #ARGV - 2)]
        -- cjson encodes an empty table as an object; drop an empty list so the
        -- decoder does not see {} where it expects an array. The renderer
        -- treats a missing list and an empty one the same.
        if type(info.staffAccounts) == 'table' and next(info.staffAccounts) == nil then
            info.staffAccounts = nil
        end
        local encoded = cjson.encode(info)
        redis.call('SET', KEYS[1], encoded, 'EX', ARGV[2])
        return encoded
        """

    /// A fetch claim holds the job's token while queued and the token plus
    /// this suffix once the job has started fetching.
    private static let runningSuffix = ":running"

    /// KEYS: [1] fetch claim. ARGV: [1] token, [2] TTL in seconds. Extends
    /// the claim only while it is the queued claim for this token.
    private static let renewFetchScript = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            redis.call('EXPIRE', KEYS[1], ARGV[2])
        end
        """

    /// KEYS: [1] fetch claim. ARGV: [1] token, [2] running suffix, [3] lease in
    /// seconds. Moves a queued claim for this token, or an absent claim, into
    /// the running state under a fresh lease. Returns 1 on success, 0 if the
    /// domain is claimed by another token.
    private static let startFetchScript = """
        local current = redis.call('GET', KEYS[1])
        if current == false or current == ARGV[1] then
            redis.call('SET', KEYS[1], ARGV[1] .. ARGV[2], 'EX', ARGV[3])
            return 1
        end
        return 0
        """

    /// KEYS: [1] fetch claim. ARGV: [1] token, [2] running suffix. Deletes the
    /// claim only if it still holds this token, queued or running
    /// (compare-and-delete).
    private static let releaseFetchScript = """
        local current = redis.call('GET', KEYS[1])
        if current == ARGV[1] or current == ARGV[1] .. ARGV[2] then
            redis.call('DEL', KEYS[1])
        end
        """
}

enum InstanceInfoCacheError: Error {
    /// The failure-recording script returned something other than the stored JSON entry.
    case unexpectedScriptResult
}
