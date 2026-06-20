@testable import APRelay

/// In-memory implementation of ``ActivityDeduplicating`` for testing.
actor MockActivityDeduplicator: ActivityDeduplicating {
    private var seen: Set<String> = []

    func isDuplicate(_ activityID: String) async throws -> Bool {
        !seen.insert(activityID).inserted
    }

    func forget(_ activityID: String) async throws {
        seen.remove(activityID)
    }
}
