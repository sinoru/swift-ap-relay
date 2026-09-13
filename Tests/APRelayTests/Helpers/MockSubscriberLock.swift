import Foundation
@testable import APRelay

/// In-memory implementation of ``SubscriberLocking`` for testing. Locks
/// expire by wall clock like their Redis counterparts; tests can hold a lock
/// on behalf of another replica and observe waiting requests.
actor MockSubscriberLock: SubscriberLocking {
    private static let externalToken = "another-replica"

    private var locks: [String: (token: String, expiresAt: Date)] = [:]
    private var issuedSequences: [String: Int] = [:]
    private var refusals: [String: Int] = [:]
    private var acquireFailure: (any Error)?

    func acquire(domain: String, token: String, ttlSeconds: Int) async throws -> Int? {
        if let failure = acquireFailure {
            acquireFailure = nil
            throw failure
        }
        if let held = locks[domain], held.expiresAt > Date() {
            refusals[domain, default: 0] += 1
            return nil
        }
        locks[domain] = (token, Date().addingTimeInterval(TimeInterval(ttlSeconds)))
        issuedSequences[domain, default: 0] += 1
        return issuedSequences[domain]
    }

    func release(domain: String, token: String) async throws {
        if locks[domain]?.token == token {
            locks[domain] = nil
        }
    }

    // MARK: - Test helpers

    /// Holds the lock on `domain` as another replica would, until
    /// ``releaseExternalHold(domain:)``.
    func holdExternally(domain: String) {
        locks[domain] = (Self.externalToken, .distantFuture)
    }

    func releaseExternalHold(domain: String) {
        if locks[domain]?.token == Self.externalToken {
            locks[domain] = nil
        }
    }

    func isHeld(domain: String) -> Bool {
        guard let held = locks[domain] else { return false }
        return held.expiresAt > Date()
    }

    /// Lets the current lock on `domain` lapse, as if its holder outlived
    /// its TTL, without anyone else taking it.
    func expire(domain: String) {
        locks[domain] = nil
    }

    /// Makes the next acquisition throw `error`, as a lost Redis connection would.
    func failNextAcquire(with error: any Error) {
        acquireFailure = error
    }

    /// How many times a request found the lock on `domain` taken.
    func refusalCount(domain: String) -> Int {
        refusals[domain, default: 0]
    }
}
