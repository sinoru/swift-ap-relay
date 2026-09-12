import Foundation
@testable import APRelay

/// In-memory implementation of ``RelayRepository`` for testing.
actor MockRelayRepository: RelayRepository {
    private var subscribers: [String: Subscriber] = [:]
    private var blockedDomains: Set<String> = []
    private var blockedDomainMeta: [String: (reason: String?, createdAt: Date?)] = [:]
    private var allowedDomains: Set<String> = []
    private var settings: [String: String] = [:]

    // MARK: - Subscribers

    func getSubscriber(domain: String) async throws -> Subscriber? {
        subscribers[domain]
    }

    func getAllSubscribers(state: SubscriberState?) async throws -> [Subscriber] {
        if let state {
            return subscribers.values.filter { $0.state == state }
        }
        return Array(subscribers.values)
    }

    func getAcceptedInboxURLs() async throws -> [String] {
        subscribers.values
            .filter { $0.state == .accepted }
            .map(\.inboxURL)
    }

    func saveSubscriber(_ subscriber: Subscriber) async throws -> Bool {
        guard !blockedDomains.contains(subscriber.domain) else { return false }
        subscribers[subscriber.domain] = subscriber
        return true
    }

    func deleteSubscriber(domain: String) async throws {
        subscribers.removeValue(forKey: domain)
    }

    // MARK: - Blocked Domains

    func isBlocked(domain: String) async throws -> Bool {
        blockedDomains.contains(domain)
    }

    func getAllBlockedDomains() async throws -> [BlockedDomain] {
        blockedDomains.map { domain in
            let meta = blockedDomainMeta[domain]
            return BlockedDomain(domain: domain, reason: meta?.reason, createdAt: meta?.createdAt)
        }
    }

    func blockDomain(_ domain: String, reason: String?) async throws -> Bool {
        let inserted = blockedDomains.insert(domain).inserted
        if inserted {
            blockedDomainMeta[domain] = (reason: reason, createdAt: Date())
        }
        return inserted
    }

    func unblockDomain(_ domain: String) async throws -> Bool {
        let removed = blockedDomains.remove(domain) != nil
        if removed {
            blockedDomainMeta.removeValue(forKey: domain)
        }
        return removed
    }

    // MARK: - Allowed Domains

    func isAllowed(domain: String) async throws -> Bool {
        allowedDomains.contains(domain)
    }

    /// Test helper: add an allowed domain.
    func addAllowedDomain(_ domain: String) {
        allowedDomains.insert(domain)
    }

    // MARK: - Settings

    func getSetting(key: String) async throws -> String? {
        settings[key]
    }

    func setSetting(key: String, value: String) async throws {
        settings[key] = value
    }

    func setSettingIfAbsent(key: String, value: String) async throws -> Bool {
        guard settings[key] == nil else { return false }
        settings[key] = value
        return true
    }
}
