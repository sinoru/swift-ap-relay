/// Protocol abstracting all data access operations for the relay.
///
/// Implementations include ``RedisRelayRepository`` for production
/// and a mock actor for tests.
protocol RelayRepository: Sendable {
    // MARK: - Subscribers

    func getSubscriber(domain: String) async throws -> Subscriber?
    func getAllSubscribers(state: SubscriberState?) async throws -> [Subscriber]
    func getAcceptedInboxURLs() async throws -> [String]

    /// Creates or updates a subscriber record atomically.
    ///
    /// The write is refused as a whole when the subscriber's domain is
    /// blocked at the moment of the write, so a Follow racing an admin block
    /// cannot resurrect a subscriber the block just removed. It is also
    /// fenced against `lease`: once a later holder of the domain's subscriber
    /// lock has written, nothing is written under an earlier lease.
    ///
    /// The `outbox` entries are recorded in the same atomic step, and only
    /// when the record is written, reserved for the caller to deliver for
    /// `leaseSeconds` before ``claimOutboxEntries(limit:leaseSeconds:)`` may
    /// hand them out.
    ///
    /// - Returns: `true` if the record was written, `false` if the domain is
    ///   blocked and nothing was written.
    /// - Throws: ``SubscriberLockError/superseded(domain:)`` when a later
    ///   lock holder has already written.
    @discardableResult
    func saveSubscriber(
        _ subscriber: Subscriber,
        lease: SubscriberLease,
        outbox: [SubscriberOutboxEntry],
        leaseSeconds: Int
    ) async throws -> Bool

    /// Removes a subscriber record atomically, including every index entry.
    ///
    /// Fenced against `lease` and records `outbox` like
    /// ``saveSubscriber(_:lease:outbox:leaseSeconds:)``.
    ///
    /// - Throws: ``SubscriberLockError/superseded(domain:)`` when a later
    ///   lock holder has already written.
    func deleteSubscriber(
        domain: String,
        lease: SubscriberLease,
        outbox: [SubscriberOutboxEntry],
        leaseSeconds: Int
    ) async throws

    // MARK: - Subscriber Outbox

    /// Claims up to `limit` outbox entries whose reservation has lapsed,
    /// oldest first, reserving each for `leaseSeconds` so no other caller
    /// receives it meanwhile.
    func claimOutboxEntries(limit: Int, leaseSeconds: Int) async throws -> [SubscriberOutboxEntry]

    /// Removes a delivered outbox entry.
    func completeOutboxEntry(id: String) async throws

    // MARK: - Blocked Domains

    func isBlocked(domain: String) async throws -> Bool
    func getAllBlockedDomains() async throws -> [BlockedDomain]
    func blockDomain(_ domain: String, reason: String?) async throws -> Bool
    func unblockDomain(_ domain: String) async throws -> Bool

    // MARK: - Allowed Domains

    func isAllowed(domain: String) async throws -> Bool

    // MARK: - Settings

    func getSetting(key: String) async throws -> String?
    func setSetting(key: String, value: String) async throws

    /// Sets a setting only if it has no value yet.
    ///
    /// - Returns: `true` if the value was stored, `false` if the key already
    ///   had a value (which is left untouched).
    func setSettingIfAbsent(key: String, value: String) async throws -> Bool
}
