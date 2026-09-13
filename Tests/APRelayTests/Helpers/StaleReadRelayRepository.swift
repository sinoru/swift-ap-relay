@testable import APRelay

/// Wraps a ``MockRelayRepository`` so that selected reads report stale state,
/// simulating a write from another replica that lands between a check and
/// the write that follows it.
///
/// - `isBlocked` reports `false` for every domain in `staleUnblockedDomains`
///   regardless of the base state, so a Follow can pass the inbox check while
///   the repository write still sees the block.
/// - `getSetting` reports `nil` for the first read of each key in
///   `staleAbsentSettingKeys`, so a bootstrap can believe a setting is absent
///   while the conditional write still sees the stored value.
///
/// Every other operation is forwarded to the base repository unchanged.
actor StaleReadRelayRepository: RelayRepository {
    private let base: MockRelayRepository
    private let staleUnblockedDomains: Set<String>
    private var staleAbsentSettingKeys: Set<String>

    init(
        base: MockRelayRepository,
        staleUnblockedDomains: Set<String> = [],
        staleAbsentSettingKeys: Set<String> = []
    ) {
        self.base = base
        self.staleUnblockedDomains = staleUnblockedDomains
        self.staleAbsentSettingKeys = staleAbsentSettingKeys
    }

    // MARK: - Subscribers

    func getSubscriber(domain: String) async throws -> Subscriber? {
        try await base.getSubscriber(domain: domain)
    }

    func getAllSubscribers(state: SubscriberState?) async throws -> [Subscriber] {
        try await base.getAllSubscribers(state: state)
    }

    func getAcceptedInboxURLs() async throws -> [String] {
        try await base.getAcceptedInboxURLs()
    }

    func saveSubscriber(
        _ subscriber: Subscriber,
        lease: SubscriberLease,
        outbox: [SubscriberOutboxEntry],
        leaseSeconds: Int
    ) async throws -> Bool {
        return try await base.saveSubscriber(subscriber, lease: lease, outbox: outbox, leaseSeconds: leaseSeconds)
    }

    func deleteSubscriber(
        domain: String,
        lease: SubscriberLease,
        outbox: [SubscriberOutboxEntry],
        leaseSeconds: Int
    ) async throws {
        try await base.deleteSubscriber(domain: domain, lease: lease, outbox: outbox, leaseSeconds: leaseSeconds)
    }

    // MARK: - Subscriber Outbox

    func claimOutboxEntries(limit: Int, leaseSeconds: Int) async throws -> [SubscriberOutboxEntry] {
        try await base.claimOutboxEntries(limit: limit, leaseSeconds: leaseSeconds)
    }

    func completeOutboxEntry(id: String) async throws {
        try await base.completeOutboxEntry(id: id)
    }

    // MARK: - Blocked Domains

    func isBlocked(domain: String) async throws -> Bool {
        if staleUnblockedDomains.contains(domain) {
            return false
        }
        return try await base.isBlocked(domain: domain)
    }

    func getAllBlockedDomains() async throws -> [BlockedDomain] {
        try await base.getAllBlockedDomains()
    }

    func blockDomain(_ domain: String, reason: String?) async throws -> Bool {
        try await base.blockDomain(domain, reason: reason)
    }

    func unblockDomain(_ domain: String) async throws -> Bool {
        try await base.unblockDomain(domain)
    }

    // MARK: - Allowed Domains

    func isAllowed(domain: String) async throws -> Bool {
        try await base.isAllowed(domain: domain)
    }

    // MARK: - Settings

    func getSetting(key: String) async throws -> String? {
        if staleAbsentSettingKeys.remove(key) != nil {
            return nil
        }
        return try await base.getSetting(key: key)
    }

    func setSetting(key: String, value: String) async throws {
        try await base.setSetting(key: key, value: value)
    }

    func setSettingIfAbsent(key: String, value: String) async throws -> Bool {
        try await base.setSettingIfAbsent(key: key, value: value)
    }
}
