import Foundation
@testable import APRelay

/// In-memory implementation of ``ActorCaching`` for testing. Entries expire
/// by wall clock like their Redis counterparts, and tests can move them
/// along with the helpers at the bottom.
actor MockActorCache: ActorCaching {
    private var actors: [String: (actor: VerifiedActor, expiresAt: Date)] = [:]
    private var aliases: [String: (id: String, expiresAt: Date)] = [:]
    private var negatives: [NegativeKey: Date] = [:]

    private struct NegativeKey: Hashable {
        let url: String
        let scope: NegativeScope
    }
    private var fetchLocks: [String: (token: String, expiresAt: Date)] = [:]
    private var refetchHolds: [String: (state: RefetchState, expiresAt: Date)] = [:]
    private var storedOnNextLockAcquire: VerifiedActor?
    private var negativeOnNextLockAcquire: (url: String, scope: NegativeScope)?
    private var negativeAtRelease: [String: Bool] = [:]
    private var runOnNextRefetchState: (@Sendable () async -> Void)?

    func actor(id: String) async throws -> VerifiedActor? {
        guard let entry = actors[id], entry.expiresAt > Date() else { return nil }
        return entry.actor
    }

    func canonicalID(forURL url: String) async throws -> String? {
        guard let entry = aliases[url], entry.expiresAt > Date() else { return nil }
        return entry.id
    }

    func store(_ actor: VerifiedActor, ttlSeconds: Int) async throws {
        actors[actor.id] = (actor, Date().addingTimeInterval(TimeInterval(ttlSeconds)))
    }

    func recordAlias(url: String, canonicalID: String, ttlSeconds: Int) async throws {
        aliases[url] = (canonicalID, Date().addingTimeInterval(TimeInterval(ttlSeconds)))
    }

    func removeAlias(url: String) async throws {
        aliases[url] = nil
    }

    func evict(id: String) async throws {
        actors[id] = nil
    }

    func isNegative(url: String, scope: NegativeScope) async throws -> Bool {
        guard let expiresAt = negatives[NegativeKey(url: url, scope: scope)] else { return false }
        return expiresAt > Date()
    }

    func markNegative(url: String, scope: NegativeScope, ttlSeconds: Int) async throws {
        let expiresAt = Date().addingTimeInterval(TimeInterval(ttlSeconds))
        negatives[NegativeKey(url: url, scope: scope)] = expiresAt
    }

    func acquireFetchLock(url: String, token: String, ttlSeconds: Int) async throws -> Bool {
        if let held = fetchLocks[url], held.expiresAt > Date() {
            return false
        }
        if let actor = storedOnNextLockAcquire {
            // Another replica finished with this URL between the caller's
            // cache lookup and its claim.
            storedOnNextLockAcquire = nil
            actors[actor.id] = (actor, Date().addingTimeInterval(3600))
        }
        if let negative = negativeOnNextLockAcquire {
            negativeOnNextLockAcquire = nil
            let key = NegativeKey(url: negative.url, scope: negative.scope)
            negatives[key] = Date().addingTimeInterval(300)
        }
        fetchLocks[url] = (token, Date().addingTimeInterval(TimeInterval(ttlSeconds)))
        return true
    }

    func releaseFetchLock(url: String, token: String) async throws {
        if fetchLocks[url]?.token == token {
            fetchLocks[url] = nil
            negativeAtRelease[url] = negatives.contains { $0.key.url == url && $0.value > Date() }
        }
    }

    func acquireRefetch(id: String, holdSeconds: Int) async throws -> Bool {
        if let held = refetchHolds[id], held.expiresAt > Date() {
            return false
        }
        refetchHolds[id] = (.refreshing, Date().addingTimeInterval(TimeInterval(holdSeconds)))
        return true
    }

    func settleRefetch(id: String, holdSeconds: Int) async throws {
        refetchHolds[id] = (.settled, Date().addingTimeInterval(TimeInterval(holdSeconds)))
    }

    func releaseRefetch(id: String) async throws {
        if refetchHolds[id]?.state == .refreshing {
            refetchHolds[id] = nil
        }
    }

    func refetchState(id: String) async throws -> RefetchState? {
        if let body = runOnNextRefetchState {
            runOnNextRefetchState = nil
            await body()
        }
        guard let held = refetchHolds[id], held.expiresAt > Date() else { return nil }
        return held.state
    }

    // MARK: - Test Helpers

    /// Stores `actor` at the moment the next fetch claim is granted, as if
    /// another replica populated the cache just before the claim.
    func storeOnNextLockAcquire(_ actor: VerifiedActor) {
        storedOnNextLockAcquire = actor
    }

    /// Marks `url` negative at the moment the next fetch claim is granted,
    /// as if another replica's failed fetch landed just before the claim.
    func markNegativeOnNextLockAcquire(url: String, scope: NegativeScope) {
        negativeOnNextLockAcquire = (url, scope)
    }

    /// Runs `body` while the next re-fetch state read is in flight, before
    /// the state is read.
    func runOnNextRefetchState(_ body: @escaping @Sendable () async -> Void) {
        runOnNextRefetchState = body
    }

    /// Whether a negative entry for `url` existed when its claim was last
    /// released, or `nil` if the claim was never released.
    func wasNegativeAtRelease(url: String) -> Bool? {
        negativeAtRelease[url]
    }

    /// Whether the re-fetch hold on `id` is currently armed.
    func isRefetchHeld(id: String) -> Bool {
        guard let held = refetchHolds[id] else { return false }
        return held.expiresAt > Date()
    }

    /// Whether an actor entry is stored under `id`.
    func hasActor(id: String) -> Bool {
        guard let entry = actors[id] else { return false }
        return entry.expiresAt > Date()
    }

    /// Holds the fetch claim on `url` under a foreign token for `duration`.
    func holdFetchLock(url: String, for duration: TimeInterval) {
        fetchLocks[url] = ("other-replica", Date().addingTimeInterval(duration))
    }

    /// Whether a fetch claim on `url` is currently held.
    func isFetchLocked(url: String) -> Bool {
        guard let held = fetchLocks[url] else { return false }
        return held.expiresAt > Date()
    }

    /// Lets every negative entry for `url` lapse as if its TTL had passed.
    func expireNegative(url: String) {
        negatives = negatives.filter { $0.key.url != url }
    }

    /// Lets the re-fetch hold on `id` lapse as if its TTL had passed.
    func expireRefetchHold(id: String) {
        refetchHolds[id] = nil
    }
}
