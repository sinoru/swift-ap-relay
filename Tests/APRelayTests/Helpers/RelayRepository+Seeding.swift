@testable import APRelay

extension RelayRepository {
    /// Stores a subscriber as test setup, outside any handler and its lock.
    ///
    /// Uses a lease that was never issued (sequence 0), so it only works
    /// against the in-memory repositories, or a fenced one before any handler
    /// has written the domain.
    @discardableResult
    func seedSubscriber(_ subscriber: Subscriber) async throws -> Bool {
        try await saveSubscriber(
            subscriber,
            lease: SubscriberLease(domain: subscriber.domain, token: "test-seed", sequence: 0),
            outbox: [],
            leaseSeconds: 0
        )
    }
}
