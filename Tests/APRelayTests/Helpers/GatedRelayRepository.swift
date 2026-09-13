@testable import APRelay

/// Wraps a ``MockRelayRepository`` and holds the first call of one write
/// until the test opens the gate, so another request can be made to arrive
/// while a handler is between its read and its write.
///
/// Every other call, and every later call of the gated write, is forwarded
/// to the base repository unchanged.
actor GatedRelayRepository: RelayRepository {
    enum Write {
        case saveSubscriber
        case deleteSubscriber
    }

    let gate = FetchGate()
    private let base: MockRelayRepository
    private let gatedWrite: Write
    private var hasGated = false

    /// Whether a call is currently held at the gate.
    private(set) var isHeldAtGate = false

    init(base: MockRelayRepository, gating gatedWrite: Write) {
        self.base = base
        self.gatedWrite = gatedWrite
    }

    private func pass(_ write: Write) async {
        guard write == gatedWrite, !hasGated else { return }
        hasGated = true
        isHeldAtGate = true
        await gate.wait()
        isHeldAtGate = false
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

    func saveSubscriber(_ subscriber: Subscriber, lease: SubscriberLease) async throws -> Bool {
        await pass(.saveSubscriber)
        return try await base.saveSubscriber(subscriber, lease: lease)
    }

    func deleteSubscriber(domain: String, lease: SubscriberLease) async throws {
        await pass(.deleteSubscriber)
        try await base.deleteSubscriber(domain: domain, lease: lease)
    }

    // MARK: - Blocked Domains

    func isBlocked(domain: String) async throws -> Bool {
        try await base.isBlocked(domain: domain)
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
        try await base.getSetting(key: key)
    }

    func setSetting(key: String, value: String) async throws {
        try await base.setSetting(key: key, value: value)
    }

    func setSettingIfAbsent(key: String, value: String) async throws -> Bool {
        try await base.setSettingIfAbsent(key: key, value: value)
    }
}
