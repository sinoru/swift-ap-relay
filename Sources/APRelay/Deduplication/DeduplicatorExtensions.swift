import Vapor

// MARK: - App Storage for ActivityDeduplicating

private struct DeduplicatorOverrideKey: StorageKey {
    typealias Value = any ActivityDeduplicating
}

extension Application {
    /// The activity deduplicator.
    ///
    /// In production this returns a ``RedisActivityDeduplicator`` backed by `app.redis`.
    /// In tests, set `deduplicatorOverride` to inject a mock.
    var activityDeduplicator: any ActivityDeduplicating {
        if let override = storage[DeduplicatorOverrideKey.self] {
            return override
        }
        return RedisActivityDeduplicator(redis: self.redis)
    }

    /// Override the deduplicator (used by tests to inject a mock).
    var deduplicatorOverride: (any ActivityDeduplicating)? {
        get { storage[DeduplicatorOverrideKey.self] }
        set { storage[DeduplicatorOverrideKey.self] = newValue }
    }
}

extension Request {
    /// The activity deduplicator for this request.
    var activityDeduplicator: any ActivityDeduplicating {
        if let override = application.storage[DeduplicatorOverrideKey.self] {
            return override
        }
        return RedisActivityDeduplicator(redis: self.application.redis)
    }

    /// Best-effort release of a deduplication slot for a rejected activity.
    ///
    /// Releasing the slot is a hardening optimization, not a correctness
    /// requirement: a failure only means the rejected id stays reserved until
    /// its TTL expires (the prior behavior), so it is logged rather than
    /// propagated and never turns an ignored activity into a 5xx response.
    func releaseDeduplicationSlot(_ activityID: String) async {
        do {
            try await activityDeduplicator.forget(activityID)
        } catch {
            logger.warning("Failed to release deduplication slot for \(activityID): \(error)")
        }
    }
}
