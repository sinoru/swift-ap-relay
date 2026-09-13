import APRelayCore
import Foundation
import Queues
import Vapor

/// Payload for sending a Reject activity in response to a Follow.
struct RejectPayload: Codable, Sendable {
    let inboxURL: String
    let followActivityID: String
    let followerActorID: String
    let followObjectURI: String?
    /// Id of the activity to send, chosen when the notification was recorded
    /// so that every retry and redelivery sends the same activity.
    ///
    /// `nil` only for jobs queued before ids were stored with them.
    /// TODO: Remove the optionality once those jobs have drained.
    let activityID: String?
}

/// Sends a signed Reject activity to a remote inbox.
struct RejectJob: AsyncJob {
    typealias Payload = RejectPayload

    func dequeue(_ context: QueueContext, _ payload: RejectPayload) async throws {
        let app = context.application
        guard try await payload.isCurrent(in: app.repository) else {
            context.logger.info("Skipping Reject to \(payload.inboxURL): the subscriber has changed since it was recorded")
            return
        }
        let config = app.relayConfig
        let privateKey = app.signingKey

        let objectURI = payload.followObjectURI
            ?? "https://www.w3.org/ns/activitystreams#Public"

        let reject = APActivity(
            context: .default,
            id: payload.activityID ?? config.makeActivityID(),
            type: "Reject",
            actor: config.actorURL,
            object: .activity(APActivity(
                context: nil,
                id: payload.followActivityID,
                type: "Follow",
                actor: payload.followerActorID,
                object: .uri(objectURI),
                to: nil,
                cc: nil,
                published: nil
            )),
            to: .single(payload.followerActorID),
            cc: nil,
            published: Date.ISO8601FormatStyle.apRelay.format(Date())
        )

        let data = try JSONEncoder.apRelay.encode(reject)

        try await SignedDeliveryHelper.send(
            activity: data,
            to: payload.inboxURL,
            config: config,
            privateKey: privateKey,
            client: app.client,
            logger: context.logger
        )

        context.logger.notice("Sent Reject to \(payload.inboxURL)")
    }

    func error(_ context: QueueContext, _ error: any Error, _ payload: RejectPayload) async throws {
        context.logger.error("Failed to send Reject to \(payload.inboxURL): \(error)")
    }
}
