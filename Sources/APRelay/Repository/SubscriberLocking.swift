import Vapor

/// Mutual exclusion for changes to one subscriber, shared by every replica.
///
/// The repository writes a subscriber atomically, but a handler reads the
/// record, decides, writes it back, and dispatches jobs based on what it
/// decided. Two such handlers for the same domain (a Follow and an admin
/// accept, an Undo and a re-Follow) must not interleave, or one overwrites
/// the other's decision while the jobs it dispatched stay sent. Every handler
/// that changes a subscriber runs that sequence under this lock.
///
/// The lock expires if its holder stalls, so every acquisition is issued an
/// increasing sequence number for its domain, carried in its
/// ``SubscriberLease``, and the repository fences subscriber writes with it:
/// a write is refused once a holder with a higher sequence has written, so a
/// holder whose lock expired cannot overwrite a newer holder's decision. A
/// late write that no newer holder has overtaken still commits, so work that
/// merely outlived its lock (and may already have dispatched its jobs) is not
/// thrown away.
///
/// Implementations include ``RedisSubscriberLock`` for production and a mock
/// actor for tests.
protocol SubscriberLocking: Sendable {
    /// Takes the lock on `domain` under `token` and issues the acquisition's
    /// sequence number, higher than any issued before for the domain. Returns
    /// `nil` while another holder's lock is live.
    func acquire(domain: String, token: String, ttlSeconds: Int) async throws -> Int?

    /// Releases the lock on `domain` if it is still held under `token`.
    func release(domain: String, token: String) async throws
}

/// Lifetimes for the subscriber lock.
struct SubscriberLockPolicy: Sendable {
    /// How long a lock lives if its holder never releases it. The work under
    /// the lock is a few Redis commands and job dispatches, far below this.
    var ttlSeconds = 10

    /// How long a request waits for another holder before giving up.
    var waitTimeout: Duration = .seconds(5)

    /// How often a waiting request retries the lock.
    var pollInterval: Duration = .milliseconds(50)
}

/// Proof of having held the subscriber lock on a domain, passed to every
/// subscriber write made under it.
struct SubscriberLease: Sendable {
    let domain: String
    let token: String
    /// The acquisition's place in the order of holders of this domain's lock.
    let sequence: Int
}

enum SubscriberLockError: Error {
    /// Another holder kept the lock on the domain for the whole wait.
    case timedOut(domain: String)
    /// The lock could not be taken: the lock store failed or the wait was
    /// cancelled. Nothing was done under the lock.
    case unavailable(domain: String, underlying: any Error)
    /// A write under the lock was refused because a later holder of the lock
    /// has already written; this holder's lock expired while it was working.
    case superseded(domain: String)
}

extension Request {
    /// Runs `body` while holding the subscriber lock on `domain`, passing
    /// the lease its subscriber writes must carry.
    ///
    /// Waits for another holder up to the policy's timeout, then throws
    /// ``SubscriberLockError/timedOut(domain:)``. A failure to take the lock
    /// is thrown as ``SubscriberLockError/unavailable(domain:underlying:)``,
    /// so callers can tell that `body` never ran. The lock is released when
    /// `body` returns or throws.
    func withSubscriberLock<Result>(
        domain: String,
        _ body: (SubscriberLease) async throws -> Result
    ) async throws -> Result {
        let lock = application.subscriberLock
        let lease = try await acquireSubscriberLease(on: domain, from: lock)

        let result: Result
        do {
            result = try await body(lease)
        } catch {
            await releaseSubscriberLock(lock, domain: domain, token: lease.token)
            throw error
        }
        await releaseSubscriberLock(lock, domain: domain, token: lease.token)
        return result
    }

    /// Takes the lock on `domain`, polling while another holder has it.
    private func acquireSubscriberLease(
        on domain: String,
        from lock: any SubscriberLocking
    ) async throws -> SubscriberLease {
        let policy = application.subscriberLockPolicy
        let token = UUID().uuidString
        let deadline = ContinuousClock.now + policy.waitTimeout

        while true {
            let sequence: Int?
            do {
                sequence = try await lock.acquire(
                    domain: domain, token: token, ttlSeconds: policy.ttlSeconds
                )
            } catch {
                throw SubscriberLockError.unavailable(domain: domain, underlying: error)
            }
            if let sequence {
                return SubscriberLease(domain: domain, token: token, sequence: sequence)
            }
            guard ContinuousClock.now < deadline else {
                throw SubscriberLockError.timedOut(domain: domain)
            }
            do {
                try await Task.sleep(for: policy.pollInterval)
            } catch {
                throw SubscriberLockError.unavailable(domain: domain, underlying: error)
            }
        }
    }

    /// Best-effort: a lock that could not be released expires on its own, so
    /// the failure is logged rather than failing work that already finished.
    private func releaseSubscriberLock(
        _ lock: any SubscriberLocking,
        domain: String,
        token: String
    ) async {
        do {
            try await lock.release(domain: domain, token: token)
        } catch {
            logger.warning("Failed to release subscriber lock for \(domain): \(error)")
        }
    }
}

// MARK: - App Storage

private struct SubscriberLockOverrideKey: StorageKey {
    typealias Value = any SubscriberLocking
}

private struct SubscriberLockPolicyKey: StorageKey {
    typealias Value = SubscriberLockPolicy
}

extension Application {
    /// The subscriber lock.
    ///
    /// In production this returns a ``RedisSubscriberLock`` backed by
    /// `app.redis`. In tests, set `subscriberLockOverride` to inject a mock.
    var subscriberLock: any SubscriberLocking {
        if let override = storage[SubscriberLockOverrideKey.self] {
            return override
        }
        return RedisSubscriberLock(redis: self.redis)
    }

    /// Override the subscriber lock (used by tests to inject a mock).
    var subscriberLockOverride: (any SubscriberLocking)? {
        get { storage[SubscriberLockOverrideKey.self] }
        set { storage[SubscriberLockOverrideKey.self] = newValue }
    }

    /// Lifetimes used by the subscriber lock; defaults unless a test shortens them.
    var subscriberLockPolicy: SubscriberLockPolicy {
        get { storage[SubscriberLockPolicyKey.self] ?? SubscriberLockPolicy() }
        set { storage[SubscriberLockPolicyKey.self] = newValue }
    }
}
