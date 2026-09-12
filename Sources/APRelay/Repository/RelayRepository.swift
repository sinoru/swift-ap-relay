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
    /// cannot resurrect a subscriber the block just removed.
    ///
    /// - Returns: `true` if the record was written, `false` if the domain is
    ///   blocked and nothing was written.
    @discardableResult
    func saveSubscriber(_ subscriber: Subscriber) async throws -> Bool

    /// Removes a subscriber record atomically, including every index entry.
    func deleteSubscriber(domain: String) async throws

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
