import Foundation
import Queues
import Testing
import Vapor
import VaporTesting
@testable import APRelay

@Suite("Instance Info Jobs Tests", .serialized)
struct InstanceInfoJobsTests {
    private struct FetchFailed: Error {}

    private static func makeContext(_ app: Application) -> QueueContext {
        QueueContext(
            queueName: .instanceInfo,
            configuration: app.queues.configuration,
            application: app,
            logger: app.logger,
            on: app.eventLoopGroup.next()
        )
    }

    private static func makeSubscriber(_ domain: String, state: SubscriberState = .accepted) -> Subscriber {
        Subscriber(
            domain: domain,
            inboxURL: "https://\(domain)/inbox",
            actorID: "https://\(domain)/actor",
            state: state,
            followActivityID: "https://\(domain)/follow/1",
            createdAt: Date(),
            updatedAt: Date()
        )
    }

    private static func mockCache(_ app: Application) throws -> MockInstanceInfoCache {
        try #require(app.instanceInfoCache as? MockInstanceInfoCache)
    }

    // MARK: - Check Tick

    @Test("Tick dispatches one claimed fetch per accepted subscriber")
    func tickDispatchesPerAcceptedSubscriber() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.repository.saveSubscriber(Self.makeSubscriber("a.example"))
            try await app.repository.saveSubscriber(Self.makeSubscriber("b.example"))
            try await app.repository.saveSubscriber(Self.makeSubscriber("pending.example", state: .pending))
            let cache = try Self.mockCache(app)

            try await InstanceInfoCheckJob().run(context: Self.makeContext(app))

            let payloads = app.queues.asyncTest.all(InstanceInfoFetchJob.self)
            #expect(Set(payloads.map(\.domain)) == ["a.example", "b.example"])
            for payload in payloads {
                // The claim names the job so the next tick can see it is still queued.
                let token = try #require(payload.claimToken)
                let claim = try await cache.pendingFetch(domain: payload.domain)
                #expect(claim == PendingFetch(token: token, isRunning: false))
                #expect(app.queues.asyncTest.jobs[JobIdentifier(string: token)] != nil)
            }
        }
    }

    @Test("Tick is skipped while another replica holds the tick claim")
    func tickSkippedWhileHeld() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.repository.saveSubscriber(Self.makeSubscriber("a.example"))
            let cache = try Self.mockCache(app)
            // Another replica ran this tick first.
            #expect(try await cache.acquireCheckTick(ttlSeconds: 60))

            try await InstanceInfoCheckJob().run(context: Self.makeContext(app))

            #expect(app.queues.asyncTest.all(InstanceInfoFetchJob.self).isEmpty)
        }
    }

    @Test("Domain with a pending fetch is not dispatched again until the job releases it")
    func pendingFetchNotRedispatched() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.repository.saveSubscriber(Self.makeSubscriber("a.example"))
            try await app.repository.saveSubscriber(Self.makeSubscriber("b.example"))
            let cache = try Self.mockCache(app)
            let context = Self.makeContext(app)

            try await InstanceInfoCheckJob().run(context: context)
            let first = app.queues.asyncTest.all(InstanceInfoFetchJob.self)
            #expect(first.count == 2)

            // Next tick: both fetches are still pending, nothing new.
            await cache.expireCheckTick()
            try await InstanceInfoCheckJob().run(context: context)
            #expect(app.queues.asyncTest.all(InstanceInfoFetchJob.self).count == 2)

            // a.example's job finishes and releases its claim; only it is re-dispatched.
            let finished = try #require(first.first { $0.domain == "a.example" })
            try await cache.releaseFetch(domain: "a.example", token: try #require(finished.claimToken))
            await cache.expireCheckTick()
            try await InstanceInfoCheckJob().run(context: context)

            let third = app.queues.asyncTest.all(InstanceInfoFetchJob.self)
            #expect(third.count == 3)
            #expect(third.filter { $0.domain == "a.example" }.count == 2)
            #expect(third.filter { $0.domain == "b.example" }.count == 1)
        }
    }

    @Test("A queued claim whose job is still in the queue is renewed each tick")
    func queuedJobClaimIsRenewed() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.repository.saveSubscriber(Self.makeSubscriber("a.example"))
            let cache = try Self.mockCache(app)
            let context = Self.makeContext(app)

            try await InstanceInfoCheckJob().run(context: context)
            let firstExpiry = try #require(await cache.fetchClaimExpiresAt(domain: "a.example"))

            // The job is still queued at the next tick: same claim, later expiry, no new job.
            try await Task.sleep(for: .milliseconds(20))
            await cache.expireCheckTick()
            try await InstanceInfoCheckJob().run(context: context)

            let secondExpiry = try #require(await cache.fetchClaimExpiresAt(domain: "a.example"))
            #expect(secondExpiry > firstExpiry)
            #expect(app.queues.asyncTest.all(InstanceInfoFetchJob.self).count == 1)
        }
    }

    @Test("A claim whose job cannot be found is neither renewed nor replaced")
    func claimWithoutJobIsLeftToExpire() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.repository.saveSubscriber(Self.makeSubscriber("a.example"))
            let cache = try Self.mockCache(app)
            // A claim left behind by a job that finished without releasing it.
            #expect(try await cache.claimFetch(domain: "a.example", token: "ghost-job", ttlSeconds: 600))
            let expiry = try #require(await cache.fetchClaimExpiresAt(domain: "a.example"))

            try await InstanceInfoCheckJob().run(context: Self.makeContext(app))

            // Only the claim lapsing may let the domain be dispatched again.
            #expect(app.queues.asyncTest.all(InstanceInfoFetchJob.self).isEmpty)
            let claim = try await cache.pendingFetch(domain: "a.example")
            #expect(claim == PendingFetch(token: "ghost-job", isRunning: false))
            #expect(await cache.fetchClaimExpiresAt(domain: "a.example") == expiry)
        }
    }

    @Test("A queued claim whose job has waited longer than a day is no longer renewed")
    func orphanedQueuedClaimIsNotRenewed() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.repository.saveSubscriber(Self.makeSubscriber("a.example"))
            let cache = try Self.mockCache(app)
            // Job data left behind by a worker that popped the job and died
            // before taking its running lease, more than a day ago.
            let jobID = JobIdentifier()
            app.queues.asyncTest.jobs[jobID] = JobData(
                payload: try InstanceInfoFetchJob.serializePayload(
                    InstanceInfoFetchPayload(domain: "a.example", claimToken: jobID.string)
                ),
                maxRetryCount: 0,
                jobName: InstanceInfoFetchJob.name,
                delayUntil: nil,
                queuedAt: Date().addingTimeInterval(-25 * 3600)
            )
            #expect(try await cache.claimFetch(domain: "a.example", token: jobID.string, ttlSeconds: 600))
            let expiry = try #require(await cache.fetchClaimExpiresAt(domain: "a.example"))

            try await InstanceInfoCheckJob().run(context: Self.makeContext(app))

            // Not renewed, so the claim lapses on its own; nothing new while it stands.
            #expect(app.queues.asyncTest.all(InstanceInfoFetchJob.self).isEmpty)
            #expect(await cache.fetchClaimExpiresAt(domain: "a.example") == expiry)
        }
    }

    @Test("A running claim is left to its own lease and not renewed by the tick")
    func runningClaimIsNotRenewed() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.repository.saveSubscriber(Self.makeSubscriber("a.example"))
            let cache = try Self.mockCache(app)
            let context = Self.makeContext(app)

            // Tick dispatches; the job then starts and takes its running lease.
            try await InstanceInfoCheckJob().run(context: context)
            let payload = try #require(app.queues.asyncTest.all(InstanceInfoFetchJob.self).first)
            let token = try #require(payload.claimToken)
            #expect(try await cache.startFetch(domain: "a.example", token: token, leaseSeconds: 600))
            let lease = try #require(await cache.fetchClaimExpiresAt(domain: "a.example"))

            try await Task.sleep(for: .milliseconds(20))
            await cache.expireCheckTick()
            try await InstanceInfoCheckJob().run(context: context)

            #expect(app.queues.asyncTest.all(InstanceInfoFetchJob.self).count == 1)
            #expect(await cache.fetchClaimExpiresAt(domain: "a.example") == lease)

            // The worker died mid-fetch: once the lease lapses the domain is dispatched afresh.
            await cache.expireFetchClaim(domain: "a.example")
            await cache.expireCheckTick()
            try await InstanceInfoCheckJob().run(context: context)
            #expect(app.queues.asyncTest.all(InstanceInfoFetchJob.self).count == 2)
        }
    }

    @Test("Domain inside its backoff window is skipped without taking a claim")
    func backoffWindowSkipped() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.repository.saveSubscriber(Self.makeSubscriber("down.example"))
            let cache = try Self.mockCache(app)
            try await cache.setInstanceInfo(
                domain: "down.example",
                info: InstanceInfo(
                    isReachable: false,
                    lastCheckedAt: Date(),
                    consecutiveFailures: 2,
                    nextAttemptAt: Date().addingTimeInterval(600)
                )
            )

            try await InstanceInfoCheckJob().run(context: Self.makeContext(app))

            #expect(app.queues.asyncTest.all(InstanceInfoFetchJob.self).isEmpty)
            let claim = try await cache.pendingFetch(domain: "down.example")
            #expect(claim == nil)
        }
    }

    @Test("Tick claim lasts most of the interval but expires before the next tick")
    func tickLockTTL() {
        #expect(InstanceInfoCheckJob.tickLockTTLSeconds(interval: 60) == 54)
        #expect(InstanceInfoCheckJob.tickLockTTLSeconds(interval: 3600) == 3240)
        #expect(InstanceInfoCheckJob.tickLockTTLSeconds(interval: 1) == 1)
    }

    @Test("Queued claim outlives the interval between renewals")
    func queuedClaimTTL() {
        #expect(InstanceInfoCheckJob.queuedClaimTTLSeconds(interval: 60) == 3600)
        #expect(InstanceInfoCheckJob.queuedClaimTTLSeconds(interval: 3600) == 7200)
    }

    // MARK: - Fetch Lease

    @Test("A queued fetch superseded by a later tick's dispatch does not run")
    func supersededFetchIsFencedOut() async throws {
        try await withApp(configure: testConfigure) { app in
            let cache = try Self.mockCache(app)
            let stale = InstanceInfoFetchPayload(domain: "slow.example", claimToken: "old-token")

            // The claim lapsed while the job sat in the queue and a later tick
            // re-claimed the domain for a newer job: the old copy must neither
            // fetch nor disturb the newer claim.
            #expect(try await cache.claimFetch(domain: "slow.example", token: "new-token", ttlSeconds: 600))
            try await InstanceInfoFetchJob().dequeue(Self.makeContext(app), stale)

            let info = try await cache.getInstanceInfo(domain: "slow.example")
            #expect(info == nil)
            let claim = try await cache.pendingFetch(domain: "slow.example")
            #expect(claim == PendingFetch(token: "new-token", isRunning: false))
        }
    }

    @Test("Starting a fetch takes the running lease; a lapsed claim is re-taken by one copy")
    func beginFetchTakesLease() async throws {
        try await withApp(configure: testConfigure) { app in
            let cache = try Self.mockCache(app)
            let job = InstanceInfoFetchJob()

            // The normal path: the tick's queued claim becomes this job's running lease.
            #expect(try await cache.claimFetch(domain: "a.example", token: "job-1", ttlSeconds: 3600))
            let queued = InstanceInfoFetchPayload(domain: "a.example", claimToken: "job-1")
            let proceeds = await job.beginFetch(queued, cache: cache, logger: app.logger)
            #expect(proceeds)
            let running = try await cache.pendingFetch(domain: "a.example")
            #expect(running == PendingFetch(token: "job-1", isRunning: true))

            // No claim at all (it lapsed with no replacement): the job re-takes it.
            let stale = InstanceInfoFetchPayload(domain: "slow.example", claimToken: "old-token")
            let staleProceeds = await job.beginFetch(stale, cache: cache, logger: app.logger)
            #expect(staleProceeds)
            let retaken = try await cache.pendingFetch(domain: "slow.example")
            #expect(retaken == PendingFetch(token: "old-token", isRunning: true))

            // A second lapsed copy finds the first one running and is fenced out.
            let copy = InstanceInfoFetchPayload(domain: "slow.example", claimToken: "other-old-token")
            let copyProceeds = await job.beginFetch(copy, cache: cache, logger: app.logger)
            #expect(!copyProceeds)
            let afterCopy = try await cache.pendingFetch(domain: "slow.example")
            #expect(afterCopy == PendingFetch(token: "old-token", isRunning: true))

            // A directly triggered fetch holds no claim and always proceeds.
            let direct = InstanceInfoFetchPayload(domain: "slow.example")
            let directProceeds = await job.beginFetch(direct, cache: cache, logger: app.logger)
            #expect(directProceeds)
        }
    }

    // MARK: - Fetch Failure

    @Test("Failure keeps metadata, backs off, and releases the running lease")
    func failureRecordsBackoffAndReleasesClaim() async throws {
        try await withApp(configure: testConfigure) { app in
            let cache = try Self.mockCache(app)
            try await cache.setInstanceInfo(
                domain: "down.example",
                info: InstanceInfo(
                    softwareName: "mastodon",
                    softwareVersion: "4.3.0",
                    isReachable: true,
                    lastCheckedAt: Date(timeIntervalSinceNow: -60),
                    consecutiveFailures: 1
                )
            )
            #expect(try await cache.claimFetch(domain: "down.example", token: "tick-token", ttlSeconds: 600))
            #expect(try await cache.startFetch(domain: "down.example", token: "tick-token", leaseSeconds: 600))
            let payload = InstanceInfoFetchPayload(domain: "down.example", claimToken: "tick-token")

            let before = Date()
            try await InstanceInfoFetchJob().error(Self.makeContext(app), FetchFailed(), payload)

            let info = try #require(try await cache.getInstanceInfo(domain: "down.example"))
            #expect(info.softwareName == "mastodon")
            #expect(info.softwareVersion == "4.3.0")
            #expect(!info.isReachable)
            #expect(info.consecutiveFailures == 2)
            // Second failure: 120s raw delay, equal jitter → 60...120s from now.
            let next = try #require(info.nextAttemptAt)
            #expect(next >= before.addingTimeInterval(60))
            #expect(next <= Date().addingTimeInterval(120))
            let claim = try await cache.pendingFetch(domain: "down.example")
            #expect(claim == nil)
        }
    }

    @Test("A directly triggered fetch does not release a claim held by the tick")
    func directFetchLeavesTickClaim() async throws {
        try await withApp(configure: testConfigure) { app in
            let cache = try Self.mockCache(app)
            #expect(try await cache.claimFetch(domain: "down.example", token: "tick-token", ttlSeconds: 600))

            try await InstanceInfoFetchJob().error(
                Self.makeContext(app),
                FetchFailed(),
                InstanceInfoFetchPayload(domain: "down.example")
            )

            let claim = try await cache.pendingFetch(domain: "down.example")
            #expect(claim == PendingFetch(token: "tick-token", isRunning: false))
        }
    }

    @Test("Next-attempt schedule doubles from 60s and stops at the 30-minute cap")
    func nextAttemptSchedule() {
        let now = Date()
        let schedule = InstanceInfoFetchJob.nextAttemptSchedule(from: now)
        // 60, 120, 240, 480, 960, then capped at 1800.
        #expect(schedule.count == 6)
        let expectedRaw: [TimeInterval] = [60, 120, 240, 480, 960, 1800]
        for (date, raw) in zip(schedule, expectedRaw) {
            let delay = date.timeIntervalSince(now)
            #expect(delay >= raw / 2 && delay <= raw, "delay \(delay) outside [\(raw / 2), \(raw)]")
        }
    }

    @Test("Fetch payload without a claim token still decodes")
    func legacyPayloadDecodes() throws {
        let data = Data(#"{"domain":"a.example"}"#.utf8)
        let payload = try JSONDecoder().decode(InstanceInfoFetchPayload.self, from: data)
        #expect(payload.domain == "a.example")
        #expect(payload.claimToken == nil)
    }
}
