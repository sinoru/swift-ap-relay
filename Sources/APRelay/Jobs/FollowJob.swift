import APRelayCore
import Foundation
import Queues
import Vapor

/// Payload for sending a Follow activity from the relay to a remote instance (LitePub mutual follow).
struct FollowPayload: Codable, Sendable {
    let inboxURL: String
    let targetActorID: String
    let followActivityID: String
}

/// Sends a signed Follow activity to a remote inbox and records the outbound follow ID.
struct FollowJob: AsyncJob {
    typealias Payload = FollowPayload

    func dequeue(_ context: QueueContext, _ payload: FollowPayload) async throws {
        let app = context.application
        guard try await payload.isCurrent(in: app.repository) else {
            context.logger.info("Skipping Follow to \(payload.inboxURL): the subscriber has changed since it was recorded")
            return
        }
        let config = app.relayConfig
        let privateKey = app.signingKey

        let followID = payload.followActivityID

        let follow = APActivity(
            context: .default,
            id: followID,
            type: "Follow",
            actor: config.actorURL,
            object: .uri(payload.targetActorID),
            to: .single(payload.targetActorID),
            cc: nil,
            published: Date.ISO8601FormatStyle.apRelay.format(Date())
        )

        let data = try JSONEncoder.apRelay.encode(follow)

        try await SignedDeliveryHelper.send(
            activity: data,
            to: payload.inboxURL,
            config: config,
            privateKey: privateKey,
            client: app.client,
            logger: context.logger
        )

        context.logger.notice("Sent Follow to \(payload.inboxURL)")
    }

    func error(_ context: QueueContext, _ error: any Error, _ payload: FollowPayload) async throws {
        context.logger.error("Failed to send Follow to \(payload.inboxURL): \(error)")
    }
}
