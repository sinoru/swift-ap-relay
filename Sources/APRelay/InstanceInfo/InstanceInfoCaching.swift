import Foundation

/// Protocol abstracting instance info cache operations and the coordination
/// keys the periodic check uses to run exactly once across replicas.
///
/// Implementations include ``RedisInstanceInfoCache`` for production
/// and a mock actor for tests.
protocol InstanceInfoCaching: Sendable {
    func getInstanceInfo(domain: String) async throws -> InstanceInfo?
    func setInstanceInfo(domain: String, info: InstanceInfo) async throws
    func getAllInstanceInfo(domains: [String]) async throws -> [String: InstanceInfo]

    /// Records a failed check for `domain` in a single atomic update: keeps
    /// the last-known metadata, marks the instance unreachable, increments
    /// `consecutiveFailures`, and sets `nextAttemptAt` from `nextAttemptAt`
    /// indexed by the new failure count (the last entry applies to every
    /// count beyond the array).
    ///
    /// - Returns: The stored entry after the update.
    func recordFailure(domain: String, at now: Date, nextAttemptAt: [Date]) async throws -> InstanceInfo

    // MARK: - Check Coordination

    /// Claims the current check tick for this replica.
    ///
    /// - Returns: `true` if no other replica holds the tick; the claim expires
    ///   after `ttlSeconds`.
    func acquireCheckTick(ttlSeconds: Int) async throws -> Bool

    /// Marks a fetch for `domain` as pending under `token`, the id of the job
    /// dispatched for it. The claim starts in the queued state.
    ///
    /// - Returns: `false` if a fetch for the domain is already pending; the
    ///   claim expires after `ttlSeconds` unless renewed.
    func claimFetch(domain: String, token: String, ttlSeconds: Int) async throws -> Bool

    /// The pending-fetch claim currently held for `domain`, if any.
    func pendingFetch(domain: String) async throws -> PendingFetch?

    /// Extends a *queued* claim for `domain` by `ttlSeconds` if it still
    /// belongs to `token`. A running claim is governed by its own lease and
    /// a claim taken by a later tick is left alone.
    func renewFetch(domain: String, token: String, ttlSeconds: Int) async throws

    /// Moves the claim for `domain` into the running state under `token`
    /// with a lease of `leaseSeconds`, atomically. Succeeds if the claim is
    /// still the queued claim for `token`, or if no claim is held (the queued
    /// claim lapsed with no replacement).
    ///
    /// - Returns: `false` if the domain is claimed by another token, in which
    ///   case the caller must not fetch.
    func startFetch(domain: String, token: String, leaseSeconds: Int) async throws -> Bool

    /// Releases the pending-fetch claim for `domain`, queued or running, if it
    /// still belongs to `token`; a claim taken by a later tick is left alone.
    func releaseFetch(domain: String, token: String) async throws
}

/// A pending-fetch claim: which job holds the domain and whether that job has
/// started fetching.
struct PendingFetch: Equatable, Sendable {
    let token: String
    let isRunning: Bool
}
