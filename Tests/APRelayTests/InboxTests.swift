import APRelayCore
import Testing
import Vapor
import VaporTesting
@testable import APRelay

@Suite("Inbox Tests", .serialized)
struct InboxTests {
    // MARK: - Follow

    @Test("Follow with public collection object creates accepted subscriber")
    func followMastodonStyle() async throws {
        try await withApp(configure: testConfigure) { app in
            let activity = TestSigning.makeFollowActivity()
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            let subscribers = try await app.repository.getAllSubscribers(state: nil)
            #expect(subscribers.count == 1)
            #expect(subscribers.first?.domain == TestSigning.testActorDomain)
            #expect(subscribers.first?.state == .accepted)
            #expect(
                subscribers.first?.followObjectURI
                    == "https://www.w3.org/ns/activitystreams#Public"
            )
        }
    }

    @Test("Follow with relay actor URL as object creates subscriber (Pleroma style)")
    func followPleromaStyle() async throws {
        try await withApp(configure: testConfigure) { app in
            let config = app.relayConfig
            let activity = TestSigning.makeFollowActivity(objectURI: config.actorURL)
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            let subscribers = try await app.repository.getAllSubscribers(state: nil)
            #expect(subscribers.count == 1)
            #expect(subscribers.first?.state == .accepted)
            #expect(subscribers.first?.followObjectURI == config.actorURL)
        }
    }

    @Test("Follow with unrecognized object is ignored")
    func followUnrecognizedObject() async throws {
        try await withApp(configure: testConfigure) { app in
            let activity = TestSigning.makeFollowActivity(
                objectURI: "https://unknown.example/something"
            )
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            let subscribers = try await app.repository.getAllSubscribers(state: nil)
            #expect(subscribers.count == 0)
        }
    }

    @Test("Follow with manual accept creates pending subscriber")
    func followManualAccept() async throws {
        try await withApp(configure: testConfigureManualAccept) { app in
            let activity = TestSigning.makeFollowActivity()
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            let subscriber = try await app.repository.getSubscriber(
                domain: TestSigning.testActorDomain
            )
            #expect(subscriber?.state == .pending)
        }
    }

    @Test("Follow from rejected subscriber keeps rejected state (auto-accept mode)")
    func followRejectedSubscriberStaysRejected() async throws {
        try await withApp(configure: testConfigure) { app in
            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .rejected,
                followActivityID: "https://remote.example/activities/follow-previous",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            let activity = TestSigning.makeFollowActivity()
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            let subscriber = try await app.repository.getSubscriber(
                domain: TestSigning.testActorDomain
            )
            #expect(subscriber?.state == .rejected)
            // followActivityID may be updated for audit, but state must stick.
        }
    }

    @Test("Follow from rejected subscriber keeps rejected state (manual accept mode)")
    func followRejectedSubscriberStaysRejectedManualAccept() async throws {
        try await withApp(configure: testConfigureManualAccept) { app in
            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .rejected,
                followActivityID: "https://remote.example/activities/follow-previous",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            let activity = TestSigning.makeFollowActivity()
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            let subscriber = try await app.repository.getSubscriber(
                domain: TestSigning.testActorDomain
            )
            #expect(subscriber?.state == .rejected)
        }
    }

    // MARK: - Undo

    @Test("Undo with nested Follow deletes subscriber")
    func undoNestedFollow() async throws {
        try await withApp(configure: testConfigure) { app in
            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .accepted,
                followActivityID: "https://remote.example/activities/follow-1",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            let activity = TestSigning.makeUndoActivity()
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            let subscribers = try await app.repository.getAllSubscribers(state: nil)
            #expect(subscribers.count == 0)
        }
    }

    @Test("Undo with URI-only object deletes subscriber")
    func undoURIObject() async throws {
        try await withApp(configure: testConfigure) { app in
            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .accepted,
                followActivityID: "https://remote.example/activities/follow-1",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            let activity = TestSigning.makeUndoActivity(objectAsURI: true)
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            let subscribers = try await app.repository.getAllSubscribers(state: nil)
            #expect(subscribers.count == 0)
        }
    }

    @Test("Undo matching current Follow deletes rejected subscriber")
    func undoMatchingFollowDeletesRejectedSubscriber() async throws {
        try await withApp(configure: testConfigure) { app in
            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .rejected,
                followActivityID: "https://remote.example/activities/follow-1",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            let activity = TestSigning.makeUndoActivity()
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            // The rejected state is not preserved across a matching Undo;
            // the remote's withdrawal removes the record (see handleUndo).
            let subscribers = try await app.repository.getAllSubscribers(state: nil)
            #expect(subscribers.count == 0)
        }
    }

    @Test("Undo signed by a different actor keeps subscriber")
    func undoSignedByDifferentActorKeepsSubscriber() async throws {
        try await withApp(configure: { app in
            try await testConfigure(app)
            // The signature verifies, but it resolves to a different actor
            // on the subscriber's domain.
            app.actorFetcher = MockActorFetcher(id: "https://remote.example/other-actor")
        }) { app in
            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .accepted,
                followActivityID: "https://remote.example/activities/follow-1",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            // The body claims the stored subscriber actor and references
            // the current Follow, but the request is signed by other-actor.
            let activity = TestSigning.makeUndoActivity()
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            let subscribers = try await app.repository.getAllSubscribers(state: nil)
            #expect(subscribers.count == 1)
        }
    }

    @Test("Undo with stale nested Follow keeps subscriber")
    func undoStaleNestedFollowKeepsSubscriber() async throws {
        try await withApp(configure: testConfigure) { app in
            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .accepted,
                followActivityID: "https://remote.example/activities/follow-2",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            let activity = TestSigning.makeUndoActivity(
                followID: "https://remote.example/activities/follow-1"
            )
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            let subscribers = try await app.repository.getAllSubscribers(state: nil)
            #expect(subscribers.count == 1)
        }
    }

    @Test("Undo with stale URI-only object keeps subscriber")
    func undoStaleURIObjectKeepsSubscriber() async throws {
        try await withApp(configure: testConfigure) { app in
            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .accepted,
                followActivityID: "https://remote.example/activities/follow-2",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            let activity = TestSigning.makeUndoActivity(
                followID: "https://remote.example/activities/follow-1",
                objectAsURI: true
            )
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            let subscribers = try await app.repository.getAllSubscribers(state: nil)
            #expect(subscribers.count == 1)
        }
    }

    @Test("Undo with nested Follow from different actor keeps subscriber")
    func undoNestedFollowDifferentActorKeepsSubscriber() async throws {
        try await withApp(configure: testConfigure) { app in
            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .accepted,
                followActivityID: "https://remote.example/activities/follow-1",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            let activity = TestSigning.makeUndoActivity(
                innerActor: "https://remote.example/other-actor"
            )
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            let subscribers = try await app.repository.getAllSubscribers(state: nil)
            #expect(subscribers.count == 1)
        }
    }

    @Test("Undo whose top-level actor differs from subscriber actor keeps subscriber")
    func undoMismatchedOuterActorKeepsSubscriber() async throws {
        try await withApp(configure: testConfigure) { app in
            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .accepted,
                followActivityID: "https://remote.example/activities/follow-1",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            // Same domain, different actor; the nested Follow spoofs the
            // subscriber actor and references the current Follow id.
            let activity = TestSigning.makeUndoActivity(
                actor: "https://remote.example/other-actor",
                innerActor: TestSigning.testActorID
            )
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            let subscribers = try await app.repository.getAllSubscribers(state: nil)
            #expect(subscribers.count == 1)
        }
    }

    @Test("Undo with URI-only object from different actor keeps subscriber")
    func undoURIObjectMismatchedActorKeepsSubscriber() async throws {
        try await withApp(configure: testConfigure) { app in
            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .accepted,
                followActivityID: "https://remote.example/activities/follow-1",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            let activity = TestSigning.makeUndoActivity(
                actor: "https://remote.example/other-actor",
                objectAsURI: true
            )
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            let subscribers = try await app.repository.getAllSubscribers(state: nil)
            #expect(subscribers.count == 1)
        }
    }

    @Test("Undo with actor-less Follow object matching current Follow deletes subscriber")
    func undoGenericFollowObjectDeletesSubscriber() async throws {
        try await withApp(configure: testConfigure) { app in
            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .accepted,
                followActivityID: "https://remote.example/activities/follow-1",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            let json = TestSigning.makeUndoJSONWithBareFollowObject()
            let (headers, body) = try TestSigning.signedRequest(json: json)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            let subscribers = try await app.repository.getAllSubscribers(state: nil)
            #expect(subscribers.count == 0)
        }
    }

    @Test("Undo with stale actor-less Follow object keeps subscriber")
    func undoStaleGenericFollowObjectKeepsSubscriber() async throws {
        try await withApp(configure: testConfigure) { app in
            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .accepted,
                followActivityID: "https://remote.example/activities/follow-2",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            let json = TestSigning.makeUndoJSONWithBareFollowObject(
                followID: "https://remote.example/activities/follow-1"
            )
            let (headers, body) = try TestSigning.signedRequest(json: json)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            let subscribers = try await app.repository.getAllSubscribers(state: nil)
            #expect(subscribers.count == 1)
        }
    }

    // MARK: - Create / Relay

    @Test("Create from accepted subscriber returns 202")
    func createFromSubscriber() async throws {
        try await withApp(configure: testConfigure) { app in
            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .accepted,
                followActivityID: "https://remote.example/activities/follow-1",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            let activity = TestSigning.makeCreateActivity()
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }
        }
    }

    @Test("Create from non-subscriber is ignored")
    func createFromNonSubscriber() async throws {
        try await withApp(configure: testConfigure) { app in
            let activity = TestSigning.makeCreateActivity()
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }
        }
    }

    // MARK: - Delete / Forward

    @Test("Delete from subscriber returns 202")
    func deleteFromSubscriber() async throws {
        try await withApp(configure: testConfigure) { app in
            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .accepted,
                followActivityID: "https://remote.example/activities/follow-1",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            let activity = TestSigning.makeDeleteActivity()
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }
        }
    }

    // MARK: - Move

    @Test("Move from subscriber returns 202")
    func moveFromSubscriber() async throws {
        try await withApp(configure: testConfigure) { app in
            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .accepted,
                followActivityID: "https://remote.example/activities/follow-1",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            let activity = TestSigning.makeMoveActivity()
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }
        }
    }

    @Test("Move from non-subscriber is ignored")
    func moveFromNonSubscriber() async throws {
        try await withApp(configure: testConfigure) { app in
            let activity = TestSigning.makeMoveActivity()
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }
        }
    }

    // MARK: - Add / Remove

    @Test("Add from subscriber returns 202")
    func addFromSubscriber() async throws {
        try await withApp(configure: testConfigure) { app in
            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .accepted,
                followActivityID: "https://remote.example/activities/follow-1",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            let activity = TestSigning.makeAddActivity()
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }
        }
    }

    @Test("Remove from subscriber returns 202")
    func removeFromSubscriber() async throws {
        try await withApp(configure: testConfigure) { app in
            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .accepted,
                followActivityID: "https://remote.example/activities/follow-1",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            let activity = TestSigning.makeRemoveActivity()
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }
        }
    }

    // MARK: - Undo (non-Follow)

    @Test("Undo Announce from subscriber returns 202")
    func undoAnnounceFromSubscriber() async throws {
        try await withApp(configure: testConfigure) { app in
            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .accepted,
                followActivityID: "https://remote.example/activities/follow-1",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            let activity = TestSigning.makeUndoAnnounceActivity()
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            // Undo of non-Follow should NOT remove the subscriber
            let subscribers = try await app.repository.getAllSubscribers(state: nil)
            #expect(subscribers.count == 1)
        }
    }

    // MARK: - LitePub Mutual Follow

    @Test("Follow with relay actor URL dispatches follow back (LitePub)")
    func followPleromaStyleSendsFollowBack() async throws {
        try await withApp(configure: testConfigure) { app in
            let config = app.relayConfig
            let activity = TestSigning.makeFollowActivity(objectURI: config.actorURL)
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            let subscribers = try await app.repository.getAllSubscribers(state: nil)
            #expect(subscribers.count == 1)
            #expect(subscribers.first?.state == .accepted)
            #expect(subscribers.first?.followObjectURI == config.actorURL)
            #expect(subscribers.first?.outboundFollowActivityID != nil)
        }
    }

    @Test("Accept from LitePub instance is acknowledged")
    func acceptFromLitePubInstance() async throws {
        try await withApp(configure: testConfigure) { app in
            let config = app.relayConfig
            let outboundFollowID = "http://localhost/activities/outbound-follow-1"

            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .accepted,
                followActivityID: "https://remote.example/activities/follow-1",
                followObjectURI: config.actorURL,
                outboundFollowActivityID: outboundFollowID,
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            let activity = TestSigning.makeAcceptActivity(
                followActivityID: outboundFollowID,
                relayActorURL: config.actorURL
            )
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            // Subscriber should still exist and be accepted.
            let subscriber = try await app.repository.getSubscriber(
                domain: TestSigning.testActorDomain
            )
            #expect(subscriber != nil)
            #expect(subscriber?.state == .accepted)
        }
    }

    @Test("Accept without outbound Follow is ignored")
    func acceptWithoutOutboundFollowIgnored() async throws {
        try await withApp(configure: testConfigure) { app in
            let config = app.relayConfig

            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .accepted,
                followActivityID: "https://remote.example/activities/follow-1",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            let activity = TestSigning.makeAcceptActivity(
                followActivityID: "http://localhost/activities/unknown",
                relayActorURL: config.actorURL
            )
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            // Subscriber should still exist unchanged.
            let subscriber = try await app.repository.getSubscriber(
                domain: TestSigning.testActorDomain
            )
            #expect(subscriber != nil)
        }
    }

    @Test("Reject from LitePub instance removes subscriber")
    func rejectFromLitePubInstanceRemovesSubscriber() async throws {
        try await withApp(configure: testConfigure) { app in
            let config = app.relayConfig
            let outboundFollowID = "http://localhost/activities/outbound-follow-1"

            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .accepted,
                followActivityID: "https://remote.example/activities/follow-1",
                followObjectURI: config.actorURL,
                outboundFollowActivityID: outboundFollowID,
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            let activity = TestSigning.makeRejectActivity(
                followActivityID: outboundFollowID,
                relayActorURL: config.actorURL
            )
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            // Subscriber should be removed.
            let subscribers = try await app.repository.getAllSubscribers(state: nil)
            #expect(subscribers.count == 0)
        }
    }

    @Test("Reject without outbound Follow is ignored")
    func rejectWithoutOutboundFollowIgnored() async throws {
        try await withApp(configure: testConfigure) { app in
            let config = app.relayConfig

            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .accepted,
                followActivityID: "https://remote.example/activities/follow-1",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            let activity = TestSigning.makeRejectActivity(
                followActivityID: "http://localhost/activities/unknown",
                relayActorURL: config.actorURL
            )
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            // Subscriber should still exist.
            let subscriber = try await app.repository.getSubscriber(
                domain: TestSigning.testActorDomain
            )
            #expect(subscriber != nil)
        }
    }

    @Test("Reject signed by a different actor keeps subscriber")
    func rejectSignedByDifferentActorKeepsSubscriber() async throws {
        try await withApp(configure: { app in
            try await testConfigure(app)
            // The signature verifies, but it resolves to a different actor
            // on the subscriber's domain.
            app.actorFetcher = MockActorFetcher(id: "https://remote.example/other-actor")
        }) { app in
            let config = app.relayConfig
            let outboundFollowID = "http://localhost/activities/outbound-follow-1"

            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .accepted,
                followActivityID: "https://remote.example/activities/follow-1",
                followObjectURI: config.actorURL,
                outboundFollowActivityID: outboundFollowID,
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            let activity = TestSigning.makeRejectActivity(
                followActivityID: outboundFollowID,
                relayActorURL: config.actorURL
            )
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            // Subscriber should still exist.
            let subscriber = try await app.repository.getSubscriber(
                domain: TestSigning.testActorDomain
            )
            #expect(subscriber != nil)
        }
    }

    @Test("Undo Follow from LitePub subscriber removes subscriber")
    func undoFollowFromLitePubRemovesSubscriber() async throws {
        try await withApp(configure: testConfigure) { app in
            let sub = Subscriber(
                domain: TestSigning.testActorDomain,
                inboxURL: TestSigning.testInboxURL,
                actorID: TestSigning.testActorID,
                state: .accepted,
                followActivityID: "https://remote.example/activities/follow-1",
                outboundFollowActivityID: "http://localhost/activities/outbound-follow-1",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.saveSubscriber(sub)

            let activity = TestSigning.makeUndoActivity()
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            let subscribers = try await app.repository.getAllSubscribers(state: nil)
            #expect(subscribers.count == 0)
        }
    }

    // MARK: - Duplicate Detection

    @Test("Duplicate activity ID returns 202")
    func duplicateActivity() async throws {
        try await withApp(configure: testConfigure) { app in
            let activityID = "https://remote.example/activities/\(UUID().uuidString)"
            let activity1 = TestSigning.makeFollowActivity(id: activityID)
            let (headers1, body1) = try TestSigning.signedRequest(activity: activity1)

            try await app.testing().test(.POST, "inbox", headers: headers1, body: body1) {
                res async in
                #expect(res.status == .accepted)
            }

            let activity2 = TestSigning.makeFollowActivity(id: activityID)
            let (headers2, body2) = try TestSigning.signedRequest(activity: activity2)

            try await app.testing().test(.POST, "inbox", headers: headers2, body: body2) {
                res async in
                #expect(res.status == .accepted)
            }

            let subscribers = try await app.repository.getAllSubscribers(state: nil)
            #expect(subscribers.count == 1)
        }
    }

    // MARK: - Domain Blocking

    @Test("Activity from blocked domain returns 403")
    func blockedDomain() async throws {
        try await withApp(configure: testConfigure) { app in
            _ = try await app.repository.blockDomain(
                TestSigning.testActorDomain, reason: "test"
            )

            let activity = TestSigning.makeFollowActivity()
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .forbidden)
            }
        }
    }

    // MARK: - Restricted Mode

    @Test("Activity from non-allowed domain in restricted mode returns 403")
    func restrictedModeBlocked() async throws {
        try await withApp(configure: testConfigureRestricted) { app in
            let activity = TestSigning.makeFollowActivity()
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .forbidden)
            }
        }
    }

    // MARK: - Invalid Input

    @Test("Invalid JSON body returns 400")
    func invalidJSON() async throws {
        try await withApp(configure: testConfigure) { app in
            let body = Data("not json".utf8)
            let sigHeaders = try TestSigning.signedHeaders(body: body)
            var headers = HTTPHeaders()
            for (name, value) in sigHeaders {
                headers.add(name: name, value: value)
            }

            try await app.testing().test(
                .POST,
                "inbox",
                headers: headers,
                body: ByteBuffer(data: body)
            ) { res async in
                #expect(res.status == .badRequest)
            }
        }
    }

    @Test("Unknown activity type returns 202")
    func unknownActivityType() async throws {
        try await withApp(configure: testConfigure) { app in
            let activity = APActivity(
                context: .default,
                id: "https://remote.example/activities/\(UUID().uuidString)",
                type: "Question",
                actor: TestSigning.testActorID,
                object: .uri("https://remote.example/notes/1"),
                to: nil,
                cc: nil,
                published: nil
            )
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }
        }
    }

    // MARK: - Actor-Signer Validation

    @Test("Activity with mismatched actor domain returns 403")
    func actorSignerMismatch() async throws {
        try await withApp(configure: testConfigure) { app in
            let activity = TestSigning.makeFollowActivity(
                actor: "https://evil.example/actor"
            )
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .forbidden)
            }
        }
    }
}
