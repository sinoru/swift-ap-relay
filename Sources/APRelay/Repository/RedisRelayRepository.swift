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
/// - `subscriber_outbox` — Sorted set of outbox entry ids, scored by the time
///   (ms since 1970) from which they may be claimed
/// - `subscriber_outbox:entries` — Hash of outbox entry id to JSON ``SubscriberNotification``
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
    private var outboxQueueKey: RedisKey { "subscriber_outbox" }
    private var outboxEntriesKey: RedisKey { "subscriber_outbox:entries" }

    private static let notificationEncoder = JSONEncoder()
    private static let notificationDecoder = JSONDecoder()

    private static func milliseconds(_ date: Date) -> String {
        String(Int64(date.timeIntervalSince1970 * 1000))
    }

    /// The outbox entries as one JSON array of alternating ids and
    /// notification JSON, for a script to decode with cjson.
    private static func encodeOutbox(_ entries: [SubscriberOutboxEntry]) throws -> String {
        var flat: [String] = []
        for entry in entries {
            let json = try notificationEncoder.encode(entry.notification)
            flat.append(entry.id)
            flat.append(String(decoding: json, as: UTF8.self))
        }
        return String(decoding: try JSONEncoder().encode(flat), as: UTF8.self)
    }

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

    func saveSubscriber(
        _ subscriber: Subscriber,
        lease: SubscriberLease,
        outbox: [SubscriberOutboxEntry],
        leaseSeconds: Int
    ) async throws -> Bool {
        let date = Date()
        let now = formatDate(date)
        let otherStates = SubscriberState.allCases.filter { $0 != subscriber.state }
        let result = try await redis.evaluate(
            Self.saveSubscriberScript,
            keys: [
                subscriberKey(subscriber.domain),
                allSubscribersKey,
                blockedDomainsSetKey,
                RedisSubscriberLock.fenceKey(subscriber.domain),
                outboxQueueKey,
                outboxEntriesKey,
                stateSetKey(subscriber.state),
            ] + otherStates.map { stateSetKey($0) },
            arguments: [
                subscriber.domain,
                String(lease.sequence),
                subscriber.createdAt.map { formatDate($0) } ?? now,
                now,
                try Self.encodeOutbox(outbox),
                Self.milliseconds(date.addingTimeInterval(TimeInterval(leaseSeconds))),
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

    func deleteSubscriber(
        domain: String,
        lease: SubscriberLease,
        outbox: [SubscriberOutboxEntry],
        leaseSeconds: Int
    ) async throws {
        let result = try await redis.evaluate(
            Self.deleteSubscriberScript,
            keys: [
                subscriberKey(domain),
                RedisSubscriberLock.fenceKey(domain),
                outboxQueueKey,
                outboxEntriesKey,
                allSubscribersKey,
            ] + SubscriberState.allCases.map { stateSetKey($0) },
            arguments: [
                domain,
                String(lease.sequence),
                try Self.encodeOutbox(outbox),
                Self.milliseconds(Date().addingTimeInterval(TimeInterval(leaseSeconds))),
            ]
        )
        guard result.int == 1 else {
            throw SubscriberLockError.superseded(domain: domain)
        }
    }

    // MARK: - Subscriber Outbox

    func claimOutboxEntries(limit: Int, leaseSeconds: Int) async throws -> [SubscriberOutboxEntry] {
        let result = try await redis.evaluate(
            Self.claimOutboxScript,
            keys: [outboxQueueKey, outboxEntriesKey],
            arguments: [Self.milliseconds(Date()), String(leaseSeconds * 1000), String(limit)]
        )
        let values = result.array ?? []
        var entries: [SubscriberOutboxEntry] = []
        for index in stride(from: 0, to: values.count - 1, by: 2) {
            guard let id = values[index].string,
                let json = values[index + 1].string,
                let notification = try? Self.notificationDecoder.decode(
                    SubscriberNotification.self, from: Data(json.utf8)
                )
            else {
                // An entry this version cannot read stays in the outbox for
                // the version that wrote it.
                continue
            }
            entries.append(SubscriberOutboxEntry(id: id, notification: notification))
        }
        return entries
    }

    func completeOutboxEntry(id: String) async throws {
        _ = try await redis.evaluate(
            Self.completeOutboxScript,
            keys: [outboxQueueKey, outboxEntriesKey],
            arguments: [id]
        )
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
    /// Lua that records outbox entries, given the outbox sorted set and entry
    /// hash keys, the JSON array of alternating ids and notifications, and the
    /// time from which they may be claimed.
    ///
    /// Entries keep their order through one-millisecond steps in their scores,
    /// counted back from the last entry so that none becomes claimable later
    /// than the given time.
    private static func recordOutboxLua(
        queueKey: String, entriesKey: String, entriesArg: String, availableAtArg: String
    ) -> String {
        """
        local outbox = cjson.decode(\(entriesArg))
        local firstScore = tonumber(\(availableAtArg)) - (#outbox / 2 - 1)
        for i = 1, #outbox, 2 do
            redis.call('HSET', \(entriesKey), outbox[i], outbox[i + 1])
            redis.call('ZADD', \(queueKey), firstScore + (i - 1) / 2, outbox[i])
        end
        """
    }

    /// KEYS: [1] subscriber hash, [2] all-subscribers set, [3] blocked-domains
    /// set, [4] subscriber fence hash, [5] outbox sorted set, [6] outbox entry
    /// hash, [7] the set for the subscriber's state, [8...] the other state
    /// sets.
    /// ARGV: [1] domain, [2] lease sequence, [3] createdAt to use for a new
    /// record, [4] updatedAt, [5] outbox entries (see ``encodeOutbox(_:)``),
    /// [6] time in ms from which the entries may be claimed, [7...]
    /// alternating field names and values for the hash.
    ///
    /// The outbox entries are recorded only when the record is written.
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
        redis.call('HSET', KEYS[1], 'createdAt', createdAt, 'updatedAt', ARGV[4], unpack(ARGV, 7))
        redis.call('SADD', KEYS[2], ARGV[1])
        for i = 8, #KEYS do
            redis.call('SREM', KEYS[i], ARGV[1])
        end
        redis.call('SADD', KEYS[7], ARGV[1])
        \(recordOutboxLua(queueKey: "KEYS[5]", entriesKey: "KEYS[6]", entriesArg: "ARGV[5]", availableAtArg: "ARGV[6]"))
        return 1
        """

    /// KEYS: [1] subscriber hash, [2] subscriber fence hash, [3] outbox sorted
    /// set, [4] outbox entry hash, [5] all-subscribers set, [6...] every state
    /// set. ARGV: [1] domain, [2] lease sequence, [3] outbox entries, [4] time
    /// in ms from which the entries may be claimed.
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
        for i = 5, #KEYS do
            redis.call('SREM', KEYS[i], ARGV[1])
        end
        \(recordOutboxLua(queueKey: "KEYS[3]", entriesKey: "KEYS[4]", entriesArg: "ARGV[3]", availableAtArg: "ARGV[4]"))
        return 1
        """

    /// KEYS: [1] outbox sorted set, [2] outbox entry hash. ARGV: [1] now in ms,
    /// [2] lease in ms, [3] limit.
    ///
    /// Returns alternating ids and notification JSON for up to `limit` entries
    /// whose time has come, each moved `lease` into the future so no other
    /// claim returns it until then. An id without an entry is dropped.
    private static let claimOutboxScript = """
        local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[3])
        local claimed = {}
        local leasedUntil = tonumber(ARGV[1]) + tonumber(ARGV[2])
        for _, id in ipairs(ids) do
            local json = redis.call('HGET', KEYS[2], id)
            if json then
                redis.call('ZADD', KEYS[1], leasedUntil, id)
                table.insert(claimed, id)
                table.insert(claimed, json)
            else
                redis.call('ZREM', KEYS[1], id)
            end
        end
        return claimed
        """

    /// KEYS: [1] outbox sorted set, [2] outbox entry hash. ARGV: [1] entry id.
    private static let completeOutboxScript = """
        redis.call('ZREM', KEYS[1], ARGV[1])
        redis.call('HDEL', KEYS[2], ARGV[1])
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
