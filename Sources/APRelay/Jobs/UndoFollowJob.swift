import APRelayCore
import Foundation
import Queues
import Vapor

/// Payload for sending an Undo Follow activity from the relay to a remote instance.
struct UndoFollowPayload: Codable, Sendable {
    let inboxURL: String
    let targetActorID: String
    let followActivityID: String
    /// Id of the activity to send, chosen when the notification was recorded
    /// so that every retry and redelivery sends the same activity.
    ///
    /// `nil` only for jobs queued before ids were stored with them.
    /// TODO: Remove the optionality once those jobs have drained.
    let activityID: String?
}

/// Sends a signed Undo Follow activity to a remote inbox.
struct UndoFollowJob: AsyncJob {
    typealias Payload = UndoFollowPayload

    func dequeue(_ context: QueueContext, _ payload: UndoFollowPayload) async throws {
        let app = context.application
        guard try await payload.isCurrent(in: app.repository) else {
            context.logger.info("Skipping Undo Follow to \(payload.inboxURL): the subscriber has changed since it was recorded")
            return
        }
        let config = app.relayConfig
        let privateKey = app.signingKey

        let undo = APActivity(
            context: .default,
            id: payload.activityID ?? config.makeActivityID(),
            type: "Undo",
            actor: config.actorURL,
            object: .activity(APActivity(
                context: nil,
                id: payload.followActivityID,
                type: "Follow",
                actor: config.actorURL,
                object: .uri(payload.targetActorID),
                to: nil,
                cc: nil,
                published: nil
            )),
            to: .single(payload.targetActorID),
            cc: nil,
            published: Date.ISO8601FormatStyle.apRelay.format(Date())
        )

        let data = try JSONEncoder.apRelay.encode(undo)

        try await SignedDeliveryHelper.send(
            activity: data,
            to: payload.inboxURL,
            config: config,
            privateKey: privateKey,
            client: app.client,
            logger: context.logger
        )

        context.logger.notice("Sent Undo Follow to \(payload.inboxURL)")
    }

    func error(_ context: QueueContext, _ error: any Error, _ payload: UndoFollowPayload) async throws {
        context.logger.error("Failed to send Undo Follow to \(payload.inboxURL): \(error)")
    }
}
