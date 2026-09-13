import Foundation
import Queues
import Testing
import Vapor
import VaporTesting
@testable import APRelay

/// Notifications that follow from a subscriber change are recorded with the
/// write and delivered from the subscriber outbox.
@Suite("Subscriber Outbox Tests", .serialized)
struct SubscriberOutboxTests {
    private static let domain = "outbox.example"

    private static let subscriber = Subscriber(
        domain: domain,
        inboxURL: "https://\(domain)/inbox",
        actorID: "https://\(domain)/actor",
        state: .rejected,
        followActivityID: "https://\(domain)/follow/1",
        createdAt: Date(),
        updatedAt: Date()
    )

    private static let lease = SubscriberLease(domain: domain, token: "test", sequence: 1)

    private static let reject = SubscriberNotification.reject(
        RejectPayload(
            inboxURL: subscriber.inboxURL,
            followActivityID: subscriber.followActivityID,
            followerActorID: subscriber.actorID,
            followObjectURI: nil,
            activityID: "https://relay.example/activities/reject-1"
        )
    )

    private static func makeContext(_ app: Application) -> QueueContext {
        QueueContext(
            queueName: .default,
            configuration: app.queues.configuration,
            application: app,
            logger: app.logger,
            on: app.eventLoopGroup.next()
        )
    }

    private static func mockRepository(_ app: Application) throws -> MockRelayRepository {
        try #require(app.repositoryOverride as? MockRelayRepository)
    }

    /// Writes the test subscriber with `notifications` without delivering
    /// them, as a request that stopped right after its write would.
    private static func writeUndelivered(
        _ repository: MockRelayRepository,
        leaseSeconds: Int
    ) async throws -> SubscriberOutboxEntry {
        let entry = SubscriberOutboxEntry(notification: reject)
        let saved = try await repository.saveSubscriber(
            subscriber, lease: lease, outbox: [entry], leaseSeconds: leaseSeconds
        )
        try #require(saved)
        return entry
    }

    @Test("A request delivers the notifications of its write and leaves nothing in the outbox")
    func requestDeliversImmediately() async throws {
        try await withApp(configure: testConfigure) { app in
            let activity = TestSigning.makeFollowActivity()
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) { res async in
                #expect(res.status == .accepted)
            }

            let accepts = app.queues.asyncTest.all(AcceptJob.self)
            #expect(accepts.count == 1)
            // The activity id is chosen when the notification is recorded, so
            // a retry or redelivery sends the same activity.
            #expect(accepts.first?.activityID != nil)
            #expect(try await Self.mockRepository(app).pendingOutboxCount() == 0)
        }
    }

    @Test("A notification that fails to queue stays in the outbox and is delivered by the recovery job")
    func failedQueuingIsRecovered() async throws {
        try await withApp(configure: testConfigure) { app in
            let repository = try Self.mockRepository(app)
            let entry = try await Self.writeUndelivered(repository, leaseSeconds: 0)

            await SubscriberOutboxDelivery.deliver(
                [entry],
                repository: repository,
                queue: FailingQueue(context: Self.makeContext(app)),
                logger: app.logger
            )
            #expect(await repository.pendingOutboxCount() == 1)
            #expect(app.queues.asyncTest.all(RejectJob.self).isEmpty)

            try await SubscriberOutboxJob().run(context: Self.makeContext(app))
            #expect(app.queues.asyncTest.all(RejectJob.self).count == 1)
            #expect(await repository.pendingOutboxCount() == 0)

            // A later run finds nothing to deliver again.
            try await SubscriberOutboxJob().run(context: Self.makeContext(app))
            #expect(app.queues.asyncTest.all(RejectJob.self).count == 1)
        }
    }

    @Test("The recovery job leaves a notification alone while its writer's reservation lasts")
    func recoveryWaitsForWriter() async throws {
        try await withApp(configure: testConfigure) { app in
            let repository = try Self.mockRepository(app)
            _ = try await Self.writeUndelivered(repository, leaseSeconds: 60)

            try await SubscriberOutboxJob().run(context: Self.makeContext(app))

            #expect(app.queues.asyncTest.all(RejectJob.self).isEmpty)
            #expect(await repository.pendingOutboxCount() == 1)
        }
    }

    // MARK: - Relevance at delivery

    private static let outboundFollowID = "https://relay.example/activities/outbound-1"

    private static func stored(state: SubscriberState, outbound: String? = outboundFollowID) -> Subscriber {
        var stored = subscriber
        stored.state = state
        stored.outboundFollowActivityID = outbound
        return stored
    }

    private static func accept(followID: String = subscriber.followActivityID) -> AcceptPayload {
        AcceptPayload(
            inboxURL: subscriber.inboxURL,
            followActivityID: followID,
            followerActorID: subscriber.actorID,
            followObjectURI: nil,
            activityID: nil
        )
    }

    private static func rejectPayload(followID: String = subscriber.followActivityID) -> RejectPayload {
        RejectPayload(
            inboxURL: subscriber.inboxURL,
            followActivityID: followID,
            followerActorID: subscriber.actorID,
            followObjectURI: nil,
            activityID: nil
        )
    }

    private static let follow = FollowPayload(
        inboxURL: subscriber.inboxURL,
        targetActorID: subscriber.actorID,
        followActivityID: outboundFollowID
    )

    private static let undoFollow = UndoFollowPayload(
        inboxURL: subscriber.inboxURL,
        targetActorID: subscriber.actorID,
        followActivityID: outboundFollowID,
        activityID: nil
    )

    @Test("Accept and Reject are current only while the subscriber is in that state for that Follow")
    func acceptAndRejectRelevance() async throws {
        let repository = MockRelayRepository()

        try await repository.seedSubscriber(Self.stored(state: .accepted))
        #expect(try await Self.accept().isCurrent(in: repository))
        #expect(try await !Self.accept(followID: "https://outbox.example/follow/old").isCurrent(in: repository))
        #expect(try await !Self.rejectPayload().isCurrent(in: repository))

        try await repository.seedSubscriber(Self.stored(state: .rejected))
        #expect(try await Self.rejectPayload().isCurrent(in: repository))
        #expect(try await !Self.accept().isCurrent(in: repository))

        try await repository.deleteSubscriber(
            domain: Self.domain, lease: Self.lease, outbox: [], leaseSeconds: 0
        )
        #expect(try await !Self.rejectPayload().isCurrent(in: repository))
    }

    @Test("Follow is current while it is the stored outbound Follow, and its Undo only once it is not")
    func followAndUndoRelevance() async throws {
        let repository = MockRelayRepository()

        try await repository.seedSubscriber(Self.stored(state: .accepted))
        #expect(try await Self.follow.isCurrent(in: repository))
        #expect(try await !Self.undoFollow.isCurrent(in: repository))

        // A remove commits: the pending Follow is obsolete, its Undo is not.
        try await repository.deleteSubscriber(
            domain: Self.domain, lease: Self.lease, outbox: [], leaseSeconds: 0
        )
        #expect(try await !Self.follow.isCurrent(in: repository))
        #expect(try await Self.undoFollow.isCurrent(in: repository))
    }

    @Test("A Follow job made obsolete by a later change finishes without sending")
    func obsoleteFollowJobIsSkipped() async throws {
        try await withApp(configure: testConfigure) { app in
            // No subscriber holds this outbound Follow any more, so the job
            // returns before it would sign and send anything.
            try await FollowJob().dequeue(Self.makeContext(app), Self.follow)
        }
    }

    @Test("A write refused as superseded or blocked records no notifications")
    func refusedWriteRecordsNothing() async throws {
        let repository = MockRelayRepository(fenced: true)
        let later = SubscriberLease(domain: Self.domain, token: "later", sequence: 2)
        let written = try await repository.saveSubscriber(
            Self.subscriber, lease: later, outbox: [], leaseSeconds: 0
        )
        try #require(written)

        await #expect(throws: SubscriberLockError.self) {
            try await repository.saveSubscriber(
                Self.subscriber,
                lease: Self.lease,
                outbox: [SubscriberOutboxEntry(notification: Self.reject)],
                leaseSeconds: 0
            )
        }

        _ = try await repository.blockDomain(Self.domain, reason: nil)
        let saved = try await repository.saveSubscriber(
            Self.subscriber,
            lease: SubscriberLease(domain: Self.domain, token: "latest", sequence: 3),
            outbox: [SubscriberOutboxEntry(notification: Self.reject)],
            leaseSeconds: 0
        )
        #expect(!saved)
        #expect(await repository.pendingOutboxCount() == 0)
    }
}
