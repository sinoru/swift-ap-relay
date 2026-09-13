import APRelayCore
import Foundation
import Queues
import Vapor

/// A notification to a remote instance that follows from a change to its
/// subscriber record.
///
/// Queuing the job and writing the record are separate Redis operations, so
/// doing them one after the other leaves a window in which only one of them
/// happens: a notification sent for a change that never committed, or a
/// committed change whose notification was never sent. Instead, the
/// notifications are recorded in the subscriber outbox by the same script
/// that writes the record, and delivered from there.
enum SubscriberNotification: Codable, Sendable {
    case accept(AcceptPayload)
    case follow(FollowPayload)
    case reject(RejectPayload)
    case undoFollow(UndoFollowPayload)

    /// Queues the job that sends this notification.
    func dispatch(on queue: Queue) async throws {
        switch self {
        case .accept(let payload):
            try await queue.dispatch(AcceptJob.self, payload, maxRetryCount: 5)
        case .follow(let payload):
            try await queue.dispatch(FollowJob.self, payload, maxRetryCount: 5)
        case .reject(let payload):
            try await queue.dispatch(RejectJob.self, payload, maxRetryCount: 5)
        case .undoFollow(let payload):
            try await queue.dispatch(UndoFollowJob.self, payload, maxRetryCount: 5)
        }
    }
}

/// A notification waiting in the subscriber outbox until its job is queued.
struct SubscriberOutboxEntry: Sendable {
    let id: String
    let notification: SubscriberNotification

    init(id: String = UUID().uuidString, notification: SubscriberNotification) {
        self.id = id
        self.notification = notification
    }
}

/// Timing for delivering the subscriber outbox.
struct SubscriberOutboxPolicy: Sendable {
    /// How long an entry is reserved for the request that wrote it, or for the
    /// replica that claimed it, before another delivery may take it. Queuing a
    /// job takes one Redis command, far below this.
    var leaseSeconds = 60

    /// How many entries one delivery run claims at a time.
    var batchSize = 50
}

extension Subscriber {
    /// The Undo Follow for this subscriber's outbound Follow, if it has one.
    func undoFollowNotification(config: RelayConfiguration) -> SubscriberNotification? {
        guard let outboundFollowID = outboundFollowActivityID else { return nil }
        return .undoFollow(
            UndoFollowPayload(
                inboxURL: inboxURL,
                targetActorID: actorID,
                followActivityID: outboundFollowID,
                activityID: config.makeActivityID()
            )
        )
    }
}

// MARK: - Relevance at Delivery

/// A notification job runs after the change that recorded it committed, but
/// possibly after later changes too: a notification left in the outbox is
/// delivered by recovery, and a queued job retries with backoff, so an older
/// notification can run after a newer one. Before sending, each job checks
/// that the stored subscriber still is what its notification announces, and
/// drops a notification a later change made obsolete.

extension RelayRepository {
    /// The stored subscriber for the domain of `actorID`.
    fileprivate func subscriber(forActor actorID: String) async throws -> Subscriber? {
        guard let domain = URL(string: actorID)?.host() else { return nil }
        return try await getSubscriber(domain: domain)
    }
}

extension AcceptPayload {
    /// Whether the subscriber is still accepted for this Follow.
    func isCurrent(in repository: any RelayRepository) async throws -> Bool {
        let subscriber = try await repository.subscriber(forActor: followerActorID)
        return subscriber?.state == .accepted && subscriber?.followActivityID == followActivityID
    }
}

extension RejectPayload {
    /// Whether the subscriber is still rejected for this Follow.
    func isCurrent(in repository: any RelayRepository) async throws -> Bool {
        let subscriber = try await repository.subscriber(forActor: followerActorID)
        return subscriber?.state == .rejected && subscriber?.followActivityID == followActivityID
    }
}

extension FollowPayload {
    /// Whether this is still the subscriber's outbound Follow.
    func isCurrent(in repository: any RelayRepository) async throws -> Bool {
        let subscriber = try await repository.subscriber(forActor: targetActorID)
        return subscriber?.outboundFollowActivityID == followActivityID
    }
}

extension UndoFollowPayload {
    /// Whether the outbound Follow this withdraws is no longer the
    /// subscriber's, so a re-established Follow with the same id is not undone.
    func isCurrent(in repository: any RelayRepository) async throws -> Bool {
        let subscriber = try await repository.subscriber(forActor: targetActorID)
        return subscriber?.outboundFollowActivityID != followActivityID
    }
}

enum SubscriberOutboxDelivery {
    /// Queues the jobs for `entries` and removes each entry once its job is
    /// queued.
    ///
    /// A failure leaves the entry in the outbox, where
    /// ``SubscriberOutboxJob`` delivers it once its lease lapses. Delivery is
    /// at least once: a crash between queuing a job and removing its entry
    /// queues it again, with the same activity id the notification recorded.
    static func deliver(
        _ entries: [SubscriberOutboxEntry],
        repository: any RelayRepository,
        queue: Queue,
        logger: Logger
    ) async {
        for entry in entries {
            do {
                try await entry.notification.dispatch(on: queue)
            } catch {
                logger.warning("Failed to queue subscriber notification \(entry.id); will retry: \(error)")
                continue
            }
            do {
                try await repository.completeOutboxEntry(id: entry.id)
            } catch {
                logger.warning("Failed to remove delivered subscriber notification \(entry.id): \(error)")
            }
        }
    }
}

extension Request {
    /// Writes `subscriber` under `lease` together with `notifications`, then
    /// delivers them if the write committed.
    ///
    /// - Returns: `false` if the domain is blocked and nothing was written.
    func saveSubscriber(
        _ subscriber: Subscriber,
        lease: SubscriberLease,
        notifying notifications: [SubscriberNotification]
    ) async throws -> Bool {
        let entries = notifications.map { SubscriberOutboxEntry(notification: $0) }
        guard try await repository.saveSubscriber(
            subscriber,
            lease: lease,
            outbox: entries,
            leaseSeconds: application.subscriberOutboxPolicy.leaseSeconds
        ) else {
            return false
        }
        await SubscriberOutboxDelivery.deliver(entries, repository: repository, queue: queue, logger: logger)
        return true
    }

    /// Removes the subscriber for `domain` under `lease` together with
    /// `notifications`, then delivers them.
    func deleteSubscriber(
        domain: String,
        lease: SubscriberLease,
        notifying notifications: [SubscriberNotification]
    ) async throws {
        let entries = notifications.map { SubscriberOutboxEntry(notification: $0) }
        try await repository.deleteSubscriber(
            domain: domain,
            lease: lease,
            outbox: entries,
            leaseSeconds: application.subscriberOutboxPolicy.leaseSeconds
        )
        await SubscriberOutboxDelivery.deliver(entries, repository: repository, queue: queue, logger: logger)
    }
}

// MARK: - App Storage

private struct SubscriberOutboxPolicyKey: StorageKey {
    typealias Value = SubscriberOutboxPolicy
}

extension Application {
    /// Timing used by the subscriber outbox; defaults unless a test shortens it.
    var subscriberOutboxPolicy: SubscriberOutboxPolicy {
        get { storage[SubscriberOutboxPolicyKey.self] ?? SubscriberOutboxPolicy() }
        set { storage[SubscriberOutboxPolicyKey.self] = newValue }
    }
}
