import APRelayCore
import Testing
import Vapor
import VaporTesting
@testable import APRelay

/// Changes to one subscriber from the inbox and the admin API run under the
/// subscriber lock, so they cannot interleave between a read and a write.
@Suite("Subscriber Lock Tests", .serialized)
struct SubscriberLockTests {
    private static let domain = TestSigning.testActorDomain
    private static let firstFollowID = "https://remote.example/activities/follow-1"
    private static let secondFollowID = "https://remote.example/activities/follow-2"

    private let authHeaders: HTTPHeaders = {
        var headers = HTTPHeaders()
        headers.bearerAuthorization = .init(token: "test-token")
        return headers
    }()

    private static func subscriber(state: SubscriberState, followID: String = firstFollowID) -> Subscriber {
        Subscriber(
            domain: domain,
            inboxURL: TestSigning.testInboxURL,
            actorID: TestSigning.testActorID,
            state: state,
            followActivityID: followID,
            createdAt: Date(),
            updatedAt: Date()
        )
    }

    private func configure(
        _ base: @escaping @Sendable (Application) async throws -> Void = testConfigure,
        repository: (any RelayRepository)? = nil,
        waitTimeout: Duration = .seconds(5)
    ) -> @Sendable (Application) async throws -> Void {
        { app in
            try await base(app)
            if let repository {
                app.repositoryOverride = repository
            }
            app.subscriberLockPolicy.pollInterval = .milliseconds(10)
            app.subscriberLockPolicy.waitTimeout = waitTimeout
        }
    }

    private func mockLock(_ app: Application) throws -> MockSubscriberLock {
        try #require(app.subscriberLockOverride as? MockSubscriberLock)
    }

    private func inboxStatus(
        _ app: Application,
        _ activity: APActivity
    ) async throws -> HTTPStatus {
        let request = try TestSigning.signedRequest(activity: activity)
        var status = HTTPStatus.internalServerError
        try await app.testing().test(.POST, "inbox", headers: request.headers, body: request.body) {
            res async in
            status = res.status
        }
        return status
    }

    private func adminStatus(_ app: Application, _ action: String) async throws -> HTTPStatus {
        var status = HTTPStatus.internalServerError
        try await app.testing().test(
            .POST,
            "api/admin/subscribers/\(Self.domain)/\(action)",
            headers: authHeaders
        ) { res async in
            status = res.status
        }
        return status
    }

    /// Polls until `condition` holds, failing the test after a few seconds.
    private func waitUntil(
        _ description: String,
        _ condition: () async -> Bool
    ) async throws {
        let deadline = ContinuousClock.now + .seconds(5)
        while await !condition() {
            guard ContinuousClock.now < deadline else {
                Issue.record("Timed out waiting until \(description)")
                throw CancellationError()
            }
            try await Task.sleep(for: .milliseconds(5))
        }
    }

    // MARK: - Interleaving

    @Test("An admin accept arriving during a re-Follow is applied after it, not overwritten")
    func followAndAdminAcceptDoNotInterleave() async throws {
        let base = MockRelayRepository()
        _ = try await base.seedSubscriber(Self.subscriber(state: .pending))
        let repository = GatedRelayRepository(base: base, gating: .saveSubscriber)

        try await withApp(
            configure: configure(testConfigureManualAccept, repository: repository)
        ) { app in
            let lock = try mockLock(app)
            let follow = TestSigning.makeFollowActivity(id: Self.secondFollowID)

            // The re-Follow has read the pending record and is about to write it.
            async let followStatus = inboxStatus(app, follow)
            try await waitUntil("the Follow is between its read and write") {
                await repository.isHeldAtGate
            }

            async let acceptStatus = adminStatus(app, "accept")
            try await waitUntil("the accept waits for the lock") {
                await lock.refusalCount(domain: Self.domain) > 0
            }
            await repository.gate.open()

            let statuses = try await [followStatus, acceptStatus]
            #expect(statuses == [.accepted, .ok])

            let stored = try await base.getSubscriber(domain: Self.domain)
            #expect(stored?.state == .accepted)
            #expect(stored?.followActivityID == Self.secondFollowID)
            #expect(app.queues.asyncTest.all(AcceptJob.self).count == 1)
            #expect(await !lock.isHeld(domain: Self.domain))
        }
    }

    @Test("Concurrent LitePub Follows from one domain announce a single outbound Follow")
    func concurrentFollowsShareOutboundFollow() async throws {
        let repository = GatedRelayRepository(base: MockRelayRepository(), gating: .saveSubscriber)

        try await withApp(configure: configure(repository: repository)) { app in
            let lock = try mockLock(app)
            let actorURL = app.relayConfig.actorURL
            let first = TestSigning.makeFollowActivity(id: Self.firstFollowID, objectURI: actorURL)
            let second = TestSigning.makeFollowActivity(id: Self.secondFollowID, objectURI: actorURL)

            async let firstStatus = inboxStatus(app, first)
            try await waitUntil("the first Follow is between its read and write") {
                await repository.isHeldAtGate
            }

            async let secondStatus = inboxStatus(app, second)
            try await waitUntil("the second Follow waits for the lock") {
                await lock.refusalCount(domain: Self.domain) > 0
            }
            await repository.gate.open()

            let statuses = try await [firstStatus, secondStatus]
            #expect(statuses == [.accepted, .accepted])

            let stored = try await repository.getSubscriber(domain: Self.domain)
            let outboundFollowID = try #require(stored?.outboundFollowActivityID)
            let followJobs = app.queues.asyncTest.all(FollowJob.self)
            #expect(!followJobs.isEmpty)
            #expect(followJobs.allSatisfy { $0.followActivityID == outboundFollowID })
        }
    }

    @Test("An Undo whose removal is pending does not remove the subscription a re-Follow creates")
    func undoDoesNotRemoveLaterFollow() async throws {
        let base = MockRelayRepository()
        _ = try await base.seedSubscriber(Self.subscriber(state: .accepted))
        let repository = GatedRelayRepository(base: base, gating: .deleteSubscriber)

        try await withApp(configure: configure(repository: repository)) { app in
            let lock = try mockLock(app)
            let undo = TestSigning.makeUndoActivity(followID: Self.firstFollowID)
            let follow = TestSigning.makeFollowActivity(id: Self.secondFollowID)

            // The Undo has matched the stored Follow and is about to remove it.
            async let undoStatus = inboxStatus(app, undo)
            try await waitUntil("the Undo is between its check and removal") {
                await repository.isHeldAtGate
            }

            async let followStatus = inboxStatus(app, follow)
            try await waitUntil("the re-Follow waits for the lock") {
                await lock.refusalCount(domain: Self.domain) > 0
            }
            await repository.gate.open()

            let statuses = try await [undoStatus, followStatus]
            #expect(statuses == [.accepted, .accepted])

            let stored = try await base.getSubscriber(domain: Self.domain)
            #expect(stored?.followActivityID == Self.secondFollowID)
        }
    }

    // MARK: - Waiting

    @Test("An inbox request that cannot get the lock answers 503 and releases its activity id")
    func inboxLockTimeout() async throws {
        try await withApp(configure: configure(waitTimeout: .milliseconds(100))) { app in
            let lock = try mockLock(app)
            await lock.holdExternally(domain: Self.domain)
            let activity = TestSigning.makeFollowActivity(id: Self.firstFollowID)
            let request = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: request.headers, body: request.body) {
                res async in
                #expect(res.status == .serviceUnavailable)
                #expect(res.headers.first(name: "Retry-After") != nil)
            }
            #expect(try await app.repository.getSubscriber(domain: Self.domain) == nil)
            #expect(app.queues.asyncTest.all(AcceptJob.self).isEmpty)

            // The sender's retry is processed rather than taken for a duplicate.
            await lock.releaseExternalHold(domain: Self.domain)
            try await app.testing().test(.POST, "inbox", headers: request.headers, body: request.body) {
                res async in
                #expect(res.status == .accepted)
            }
            #expect(try await app.repository.getSubscriber(domain: Self.domain) != nil)
        }
    }

    @Test("An admin change that cannot get the lock answers 409 and changes nothing")
    func adminLockTimeout() async throws {
        try await withApp(configure: configure(waitTimeout: .milliseconds(100))) { app in
            try await app.repository.seedSubscriber(Self.subscriber(state: .pending))
            let lock = try mockLock(app)
            await lock.holdExternally(domain: Self.domain)

            #expect(try await adminStatus(app, "accept") == .conflict)
            #expect(try await app.repository.getSubscriber(domain: Self.domain)?.state == .pending)
            #expect(app.queues.asyncTest.all(AcceptJob.self).isEmpty)
        }
    }

    @Test("A block that cannot get the lock stores nothing, and its retry completes")
    func blockLockTimeout() async throws {
        try await withApp(configure: configure(waitTimeout: .milliseconds(100))) { app in
            try await app.repository.seedSubscriber(Self.subscriber(state: .accepted))
            let lock = try mockLock(app)
            await lock.holdExternally(domain: Self.domain)

            var headers = authHeaders
            headers.contentType = .json
            let body = ByteBuffer(string: "{\"domain\":\"\(Self.domain)\"}")

            try await app.testing().test(.POST, "api/admin/blocked-domains", headers: headers, body: body) {
                res async in
                #expect(res.status == .conflict)
            }
            #expect(try await !app.repository.isBlocked(domain: Self.domain))
            #expect(try await app.repository.getSubscriber(domain: Self.domain) != nil)

            await lock.releaseExternalHold(domain: Self.domain)
            try await app.testing().test(.POST, "api/admin/blocked-domains", headers: headers, body: body) {
                res async in
                #expect(res.status == .ok)
            }
            #expect(try await app.repository.isBlocked(domain: Self.domain))
            #expect(try await app.repository.getSubscriber(domain: Self.domain) == nil)
        }
    }

    @Test("An inbox request whose lock acquisition fails answers 503 and releases its activity id")
    func inboxLockUnavailable() async throws {
        try await withApp(configure: configure()) { app in
            let lock = try mockLock(app)
            await lock.failNextAcquire(with: Abort(.internalServerError, reason: "connection lost"))
            let activity = TestSigning.makeFollowActivity(id: Self.firstFollowID)

            #expect(try await inboxStatus(app, activity) == .serviceUnavailable)
            #expect(try await app.repository.getSubscriber(domain: Self.domain) == nil)

            // The retry is processed rather than taken for a duplicate.
            #expect(try await inboxStatus(app, activity) == .accepted)
            #expect(try await app.repository.getSubscriber(domain: Self.domain) != nil)
        }
    }

    @Test("An admin change whose lock acquisition fails surfaces the failure and changes nothing")
    func adminLockUnavailable() async throws {
        try await withApp(configure: configure()) { app in
            try await app.repository.seedSubscriber(Self.subscriber(state: .pending))
            let lock = try mockLock(app)
            await lock.failNextAcquire(with: Abort(.internalServerError, reason: "connection lost"))

            #expect(try await adminStatus(app, "accept") == .internalServerError)
            #expect(try await app.repository.getSubscriber(domain: Self.domain)?.state == .pending)
            #expect(app.queues.asyncTest.all(AcceptJob.self).isEmpty)
        }
    }

    // MARK: - Fencing

    @Test("A Follow overtaken by a later lock holder's write is refused, answers 503, and releases its id")
    func followSupersededByLaterHolder() async throws {
        let base = MockRelayRepository(fenced: true)
        let repository = GatedRelayRepository(base: base, gating: .saveSubscriber)

        try await withApp(configure: configure(repository: repository)) { app in
            let lock = try mockLock(app)
            let first = TestSigning.makeFollowActivity(id: Self.firstFollowID)
            let second = TestSigning.makeFollowActivity(id: Self.secondFollowID)

            async let firstStatus = inboxStatus(app, first)
            try await waitUntil("the first Follow is between its read and write") {
                await repository.isHeldAtGate
            }
            // The first Follow stalls past its lock's lifetime; a second Follow
            // takes the lock and writes.
            await lock.expire(domain: Self.domain)
            #expect(try await inboxStatus(app, second) == .accepted)
            await repository.gate.open()

            #expect(try await firstStatus == .serviceUnavailable)
            let stored = try await base.getSubscriber(domain: Self.domain)
            #expect(stored?.followActivityID == Self.secondFollowID)
            #expect(app.queues.asyncTest.all(AcceptJob.self).count == 1)

            // The sender's retry is processed rather than taken for a duplicate.
            #expect(try await inboxStatus(app, first) == .accepted)
            let retried = try await base.getSubscriber(domain: Self.domain)
            #expect(retried?.followActivityID == Self.firstFollowID)
        }
    }

    @Test("An admin reject whose lock expires after dispatching, with no later write, still commits")
    func rejectOutlivingItsLockCommits() async throws {
        let base = MockRelayRepository(fenced: true)
        var pending = Self.subscriber(state: .pending)
        pending.outboundFollowActivityID = "http://localhost/activities/outbound-1"
        _ = try await base.seedSubscriber(pending)
        let repository = GatedRelayRepository(base: base, gating: .saveSubscriber)

        try await withApp(configure: configure(repository: repository)) { app in
            let lock = try mockLock(app)

            // Reject and Undo Follow are dispatched before the write, which
            // stalls past the lock's lifetime while nobody else takes it.
            async let rejectStatus = adminStatus(app, "reject")
            try await waitUntil("the reject is between its dispatch and write") {
                await repository.isHeldAtGate
            }
            await lock.expire(domain: Self.domain)
            await repository.gate.open()

            #expect(try await rejectStatus == .ok)
            let stored = try await base.getSubscriber(domain: Self.domain)
            #expect(stored?.state == .rejected)
            #expect(stored?.outboundFollowActivityID == nil)
            #expect(app.queues.asyncTest.all(RejectJob.self).count == 1)
            #expect(app.queues.asyncTest.all(UndoFollowJob.self).count == 1)
        }
    }

    // MARK: - Release

    @Test("The lock is released when the work under it fails")
    func lockReleasedOnFailure() async throws {
        // The block lands after the inbox check, so the Follow's write is
        // refused inside the lock.
        let base = MockRelayRepository()
        _ = try await base.blockDomain(Self.domain, reason: "test")
        let repository = StaleReadRelayRepository(base: base, staleUnblockedDomains: [Self.domain])

        try await withApp(configure: configure(repository: repository)) { app in
            let lock = try mockLock(app)

            let follow = TestSigning.makeFollowActivity(id: Self.firstFollowID)
            #expect(try await inboxStatus(app, follow) == .forbidden)
            #expect(await !lock.isHeld(domain: Self.domain))

            #expect(try await adminStatus(app, "accept") == .notFound)
            #expect(await !lock.isHeld(domain: Self.domain))
        }
    }
}
