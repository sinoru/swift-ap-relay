import Queues
import Vapor

enum SubscriberState: String, CaseIterable, Codable, Sendable {
    case pending
    case accepted
    case rejected
}

struct Subscriber: Codable, Content, Sendable {
    let domain: String
    var inboxURL: String
    var actorID: String
    var state: SubscriberState
    var followActivityID: String
    var followObjectURI: String?
    var outboundFollowActivityID: String?
    var createdAt: Date?
    var updatedAt: Date?
}

extension Subscriber {
    /// Dispatches an UndoFollowJob if this subscriber has an outbound follow.
    func dispatchUndoFollowIfNeeded(on queue: Queue) async throws {
        guard let outboundFollowID = outboundFollowActivityID else { return }
        try await queue.dispatch(
            UndoFollowJob.self,
            UndoFollowPayload(
                inboxURL: inboxURL,
                targetActorID: actorID,
                followActivityID: outboundFollowID
            ),
            maxRetryCount: 5
        )
    }
}
