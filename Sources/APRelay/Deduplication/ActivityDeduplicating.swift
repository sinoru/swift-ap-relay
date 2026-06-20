/// Protocol abstracting activity deduplication.
///
/// Implementations include ``RedisActivityDeduplicator`` for production
/// and a mock actor for tests.
protocol ActivityDeduplicating: Sendable {
    /// Returns `true` if this activity ID was already seen recently.
    /// If not seen, records it atomically so subsequent calls return `true`.
    func isDuplicate(_ activityID: String) async throws -> Bool

    /// Releases a previously recorded activity ID so it no longer counts as
    /// seen. Used when an activity was recorded by ``isDuplicate(_:)`` but then
    /// rejected without being acted on, so a later legitimate activity that
    /// reuses the same ID is not mistaken for a duplicate.
    func forget(_ activityID: String) async throws
}
