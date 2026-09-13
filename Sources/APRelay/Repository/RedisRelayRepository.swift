@preconcurrency @unsafe import RediStack
import Vapor

/// Redis-backed implementation of ``RelayRepository``.
///
/// Key schema:
/// - `subscriber:{domain}` — Hash with subscriber fields
/// - `subscribers:state:{state}` — Set of domains per state
/// - `subscribers:all` — Set of all subscriber domains
/// - `blocked_domains` — Set of blocked domain strings
/// - `blocked_domain:{domain}` — Hash with reason/createdAt
/// - `allowed_domains` — Set of allowed domain strings
/// - `relay_settings` — Hash of key-value settings
struct RedisRelayRepository: RelayRepository, Sendable {
    let redis: any RedisClient & Sendable

    private static let dateFormatStyle = Date.ISO8601FormatStyle(includingFractionalSeconds: true)

    private func formatDate(_ date: Date) -> String {
        date.formatted(Self.dateFormatStyle)
    }

    private func parseDate(_ string: String) -> Date? {
        try? Self.dateFormatStyle.parse(string)
    }

    // MARK: - Key Helpers

    private func subscriberKey(_ domain: String) -> RedisKey { "subscriber:\(domain)" }
    private func stateSetKey(_ state: SubscriberState) -> RedisKey {
        "subscribers:state:\(state.rawValue)"
    }
    private var allSubscribersKey: RedisKey { "subscribers:all" }
    private var blockedDomainsSetKey: RedisKey { "blocked_domains" }
    private func blockedDomainKey(_ domain: String) -> RedisKey { "blocked_domain:\(domain)" }
    private var allowedDomainsSetKey: RedisKey { "allowed_domains" }
    private var settingsKey: RedisKey { "relay_settings" }

    // MARK: - Subscribers

    func getSubscriber(domain: String) async throws -> Subscriber? {
        let fields = try await redis.hgetall(from: subscriberKey(domain)).get()
        guard !fields.isEmpty else { return nil }
        return decodeSubscriber(domain: domain, fields: fields)
    }

    func getAllSubscribers(state: SubscriberState?) async throws -> [Subscriber] {
        let key = state.map { stateSetKey($0) } ?? allSubscribersKey
        let domainValues = try await redis.smembers(of: key).get()
        let domains = domainValues.compactMap(\.string)
        guard !domains.isEmpty else { return [] }

        let futures = domains.map { domain in
            redis.hgetall(from: subscriberKey(domain))
        }
        let results = try await EventLoopFuture.whenAllSucceed(futures, on: redis.eventLoop).get()

        return zip(domains, results).compactMap { (domain, fields) in
            guard !fields.isEmpty else { return nil }
            return decodeSubscriber(domain: domain, fields: fields)
        }
    }

    func getAcceptedInboxURLs() async throws -> [String] {
        let domainValues = try await redis.smembers(of: stateSetKey(.accepted)).get()
        let domains = domainValues.compactMap(\.string)
        guard !domains.isEmpty else { return [] }

        let futures = domains.map { domain in
            redis.hget("inboxURL", from: subscriberKey(domain))
        }
        let results = try await EventLoopFuture.whenAllSucceed(futures, on: redis.eventLoop).get()
        return results.compactMap(\.string)
    }

    func saveSubscriber(_ subscriber: Subscriber, lease: SubscriberLease) async throws -> Bool {
        let now = formatDate(Date())
        let otherStates = SubscriberState.allCases.filter { $0 != subscriber.state }
        let result = try await redis.evaluate(
            Self.saveSubscriberScript,
            keys: [
                subscriberKey(subscriber.domain),
                allSubscribersKey,
                blockedDomainsSetKey,
                RedisSubscriberLock.fenceKey(subscriber.domain),
                stateSetKey(subscriber.state),
            ] + otherStates.map { stateSetKey($0) },
            arguments: [
                subscriber.domain,
                String(lease.sequence),
                subscriber.createdAt.map { formatDate($0) } ?? now,
                now,
                "inboxURL", subscriber.inboxURL,
                "actorID", subscriber.actorID,
                "state", subscriber.state.rawValue,
                "followActivityID", subscriber.followActivityID,
                "followObjectURI", subscriber.followObjectURI ?? "",
                "outboundFollowActivityID", subscriber.outboundFollowActivityID ?? "",
            ]
        )
        switch result.int {
        case 1: return true
        case 0: return false
        default: throw SubscriberLockError.superseded(domain: subscriber.domain)
        }
    }

    func deleteSubscriber(domain: String, lease: SubscriberLease) async throws {
        let result = try await redis.evaluate(
            Self.deleteSubscriberScript,
            keys: [
                subscriberKey(domain),
                RedisSubscriberLock.fenceKey(domain),
                allSubscribersKey,
            ] + SubscriberState.allCases.map { stateSetKey($0) },
            arguments: [domain, String(lease.sequence)]
        )
        guard result.int == 1 else {
            throw SubscriberLockError.superseded(domain: domain)
        }
    }

    // MARK: - Blocked Domains

    func isBlocked(domain: String) async throws -> Bool {
        try await redis.sismember(domain, of: blockedDomainsSetKey).get()
    }

    func getAllBlockedDomains() async throws -> [BlockedDomain] {
        let domainValues = try await redis.smembers(of: blockedDomainsSetKey).get()
        let domains = domainValues.compactMap(\.string)
        guard !domains.isEmpty else { return [] }

        let futures = domains.map { domain in
            redis.hgetall(from: blockedDomainKey(domain))
        }
        let results = try await EventLoopFuture.whenAllSucceed(futures, on: redis.eventLoop).get()

        return zip(domains, results).map { (domain, fields) in
            let reason = fields["reason"]?.string
            let createdAt = fields["createdAt"]?.string.flatMap { parseDate($0) }
            return BlockedDomain(domain: domain, reason: reason, createdAt: createdAt)
        }
    }

    func blockDomain(_ domain: String, reason: String?) async throws -> Bool {
        let added = try await redis.sadd(domain, to: blockedDomainsSetKey).get()
        guard added > 0 else { return false }

        let now = formatDate(Date())
        var fields: [String: RESPValue] = [
            "createdAt": .init(from: now)
        ]
        if let reason {
            fields["reason"] = .init(from: reason)
        }
        _ = try await redis.hmset(fields, in: blockedDomainKey(domain)).get()
        return true
    }

    func unblockDomain(_ domain: String) async throws -> Bool {
        let removed = try await redis.srem(domain, from: blockedDomainsSetKey).get()
        guard removed > 0 else { return false }
        _ = try await redis.send(
            command: "DEL",
            with: [.init(from: blockedDomainKey(domain).rawValue)]
        ).get()
        return true
    }

    // MARK: - Allowed Domains

    func isAllowed(domain: String) async throws -> Bool {
        try await redis.sismember(domain, of: allowedDomainsSetKey).get()
    }

    // MARK: - Settings

    func getSetting(key: String) async throws -> String? {
        let value = try await redis.hget(key, from: settingsKey).get()
        return value.string
    }

    func setSetting(key: String, value: String) async throws {
        _ = try await redis.hset(key, to: value, in: settingsKey).get()
    }

    func setSettingIfAbsent(key: String, value: String) async throws -> Bool {
        try await redis.hsetnx(key, to: value, in: settingsKey).get()
    }

    // MARK: - Atomic Scripts

    /// Every multi-key subscriber write runs as a single Lua script so that a
    /// concurrent write from another replica (or an admin block) cannot
    /// interleave with it and leave the hash and the index sets disagreeing.
    ///
    /// KEYS: [1] subscriber hash, [2] all-subscribers set, [3] blocked-domains
    /// set, [4] subscriber fence hash, [5] the set for the subscriber's state,
    /// [6...] the other state sets.
    /// ARGV: [1] domain, [2] lease sequence, [3] createdAt to use for a new
    /// record, [4] updatedAt, [5...] alternating field names and values for
    /// the hash.
    ///
    /// Returns 1 when written, 0 when refused because the domain is blocked,
    /// -1 when refused because a holder with a later sequence has written.
    private static let saveSubscriberScript = """
        if tonumber(ARGV[2]) < (tonumber(redis.call('HGET', KEYS[4], 'written')) or 0) then
            return -1
        end
        if redis.call('SISMEMBER', KEYS[3], ARGV[1]) == 1 then
            return 0
        end
        redis.call('HSET', KEYS[4], 'written', ARGV[2])
        local createdAt = redis.call('HGET', KEYS[1], 'createdAt') or ARGV[3]
        redis.call('HSET', KEYS[1], 'createdAt', createdAt, 'updatedAt', ARGV[4], unpack(ARGV, 5))
        redis.call('SADD', KEYS[2], ARGV[1])
        for i = 6, #KEYS do
            redis.call('SREM', KEYS[i], ARGV[1])
        end
        redis.call('SADD', KEYS[5], ARGV[1])
        return 1
        """

    /// KEYS: [1] subscriber hash, [2] subscriber fence hash, [3] all-subscribers
    /// set, [4...] every state set. ARGV: [1] domain, [2] lease sequence.
    ///
    /// The domain is removed from every state set rather than only the one
    /// recorded in the hash, so a record left inconsistent by an interrupted
    /// pre-script write is cleaned up as well.
    ///
    /// Returns 1 when deleted, -1 when refused because a holder with a later
    /// sequence has written.
    private static let deleteSubscriberScript = """
        if tonumber(ARGV[2]) < (tonumber(redis.call('HGET', KEYS[2], 'written')) or 0) then
            return -1
        end
        redis.call('HSET', KEYS[2], 'written', ARGV[2])
        redis.call('DEL', KEYS[1])
        for i = 3, #KEYS do
            redis.call('SREM', KEYS[i], ARGV[1])
        end
        return 1
        """

    // MARK: - Decoding Helpers

    private func decodeSubscriber(domain: String, fields: [String: RESPValue]) -> Subscriber? {
        guard
            let inboxURL = fields["inboxURL"]?.string,
            let actorID = fields["actorID"]?.string,
            let stateRaw = fields["state"]?.string,
            let state = SubscriberState(rawValue: stateRaw),
            let followActivityID = fields["followActivityID"]?.string
        else {
            return nil
        }

        let followObjectURI = fields["followObjectURI"]?.string.flatMap { $0.isEmpty ? nil : $0 }
        let outboundFollowActivityID = fields["outboundFollowActivityID"]?.string.flatMap {
            $0.isEmpty ? nil : $0
        }
        let createdAt = fields["createdAt"]?.string.flatMap { parseDate($0) }
        let updatedAt = fields["updatedAt"]?.string.flatMap { parseDate($0) }

        return Subscriber(
            domain: domain,
            inboxURL: inboxURL,
            actorID: actorID,
            state: state,
            followActivityID: followActivityID,
            followObjectURI: followObjectURI,
            outboundFollowActivityID: outboundFollowActivityID,
            createdAt: createdAt,
            updatedAt: updatedAt
        )
    }
}
