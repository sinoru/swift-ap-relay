import Foundation
@testable import APRelay

/// In-memory implementation of ``InstanceInfoCaching`` for testing.
actor MockInstanceInfoCache: InstanceInfoCaching {
    private var store: [String: InstanceInfo] = .init()
    private var checkTickHeldUntil: Date?
    private var fetchClaims: [String: (token: String, isRunning: Bool, expiresAt: Date)] = [:]

    func getInstanceInfo(domain: String) async throws -> InstanceInfo? {
        store[domain]
    }

    func setInstanceInfo(domain: String, info: InstanceInfo) async throws {
        store[domain] = info
    }

    func getAllInstanceInfo(domains: [String]) async throws -> [String: InstanceInfo] {
        var result = [String: InstanceInfo]()
        for domain in domains {
            if let info = store[domain] {
                result[domain] = info
            }
        }
        return result
    }

    func recordFailure(domain: String, at now: Date, nextAttemptAt: [Date]) async throws -> InstanceInfo {
        let existing = store[domain]
        let failures = (existing?.consecutiveFailures ?? 0) + 1
        let updated = InstanceInfo(
            softwareName: existing?.softwareName,
            softwareVersion: existing?.softwareVersion,
            openRegistrations: existing?.openRegistrations,
            staffAccounts: existing?.staffAccounts,
            faviconURL: existing?.faviconURL,
            isReachable: false,
            lastCheckedAt: now,
            consecutiveFailures: failures,
            nextAttemptAt: nextAttemptAt[min(failures, nextAttemptAt.count) - 1]
        )
        store[domain] = updated
        return updated
    }

    // MARK: - Check Coordination

    func acquireCheckTick(ttlSeconds: Int) async throws -> Bool {
        if let until = checkTickHeldUntil, until > Date() {
            return false
        }
        checkTickHeldUntil = Date().addingTimeInterval(TimeInterval(ttlSeconds))
        return true
    }

    func claimFetch(domain: String, token: String, ttlSeconds: Int) async throws -> Bool {
        if liveClaim(domain) != nil {
            return false
        }
        fetchClaims[domain] = (token, false, Date().addingTimeInterval(TimeInterval(ttlSeconds)))
        return true
    }

    func pendingFetch(domain: String) async throws -> PendingFetch? {
        liveClaim(domain).map { PendingFetch(token: $0.token, isRunning: $0.isRunning) }
    }

    func renewFetch(domain: String, token: String, ttlSeconds: Int) async throws {
        guard let claim = liveClaim(domain), claim.token == token, !claim.isRunning else { return }
        fetchClaims[domain] = (token, false, Date().addingTimeInterval(TimeInterval(ttlSeconds)))
    }

    func startFetch(domain: String, token: String, leaseSeconds: Int) async throws -> Bool {
        if let claim = liveClaim(domain), claim.token != token || claim.isRunning {
            return false
        }
        fetchClaims[domain] = (token, true, Date().addingTimeInterval(TimeInterval(leaseSeconds)))
        return true
    }

    func releaseFetch(domain: String, token: String) async throws {
        guard fetchClaims[domain]?.token == token else { return }
        fetchClaims[domain] = nil
    }

    private func liveClaim(_ domain: String) -> (token: String, isRunning: Bool, expiresAt: Date)? {
        guard let claim = fetchClaims[domain], claim.expiresAt > Date() else { return nil }
        return claim
    }

    // MARK: - Test Helpers

    /// Simulates the tick claim expiring, as it would before the next tick.
    func expireCheckTick() {
        checkTickHeldUntil = nil
    }

    /// Simulates a fetch claim or running lease expiring.
    func expireFetchClaim(domain: String) {
        fetchClaims[domain] = nil
    }

    /// When the pending fetch claim for `domain` expires, if one is held.
    func fetchClaimExpiresAt(domain: String) -> Date? {
        liveClaim(domain)?.expiresAt
    }
}
