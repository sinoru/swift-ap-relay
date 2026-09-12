import Foundation
import Queues
import Vapor

/// Periodically dispatches ``InstanceInfoFetchJob`` for each accepted subscriber.
///
/// Doubles as a reachability heartbeat — every check updates `isReachable` /
/// `lastCheckedAt`. Subscribers currently in a backoff window (set by a prior
/// failure) are skipped until their `nextAttemptAt` elapses.
///
/// Scheduled jobs run on every replica, so each tick is first claimed in
/// Redis and only the replica that wins dispatches. Each dispatched domain
/// also holds a pending-fetch claim naming its job; while that job still
/// exists in the queue the domain is not dispatched again, so a slow fetch
/// or a backlog is worked through in order instead of being re-queued and
/// growing without bound.
struct InstanceInfoCheckJob: AsyncScheduledJob {
    /// Lifetime of a queued fetch claim between renewals. Each tick renews
    /// the claim of a job it can confirm is still waiting in the queue, so a
    /// backlog of any length is worked through in order without a claim
    /// expiring underneath a waiting job. Once the job starts it takes a
    /// short running lease instead (see ``InstanceInfoFetchJob``), which is
    /// what bounds recovery from a worker that crashed mid-fetch.
    static func queuedClaimTTLSeconds(interval: Int) -> Int {
        max(3600, 2 * interval)
    }

    /// Renewal of a queued claim stops once its job has been queued for this
    /// long. The queue driver keeps job data for a job a worker popped and
    /// then died on before it could take its running lease, so job data
    /// alone cannot prove a job is still waiting; this bound recovers such a
    /// domain within a day. A backlog older than this is far outside the
    /// cache TTL and not a supported operating range.
    static let maxQueuedJobAgeSeconds = 24 * 3600

    /// The tick claim must outlive the other replicas' ticks for this interval
    /// but expire before this replica's own next tick, which drifts later by
    /// the time the run takes. Holding it for most of the interval does both.
    static func tickLockTTLSeconds(interval: Int) -> Int {
        max(interval - max(interval / 10, 1), 1)
    }

    func run(context: QueueContext) async throws {
        let app = context.application
        let cache = app.instanceInfoCache
        let queue = app.queues.queue(.instanceInfo)

        let interval = app.relayConfig.instanceInfoCheckInterval
        let lockTTL = Self.tickLockTTLSeconds(interval: interval)
        let claimTTL = Self.queuedClaimTTLSeconds(interval: interval)
        guard try await cache.acquireCheckTick(ttlSeconds: lockTTL) else {
            context.logger.debug("Instance info check tick is held by another replica; skipping")
            return
        }

        let subscribers = try await app.repository.getAllSubscribers(state: .accepted)
        guard !subscribers.isEmpty else { return }

        let domains = subscribers.map(\.domain)
        let cached: [String: InstanceInfo]
        do {
            cached = try await cache.getAllInstanceInfo(domains: domains)
        } catch {
            // On cache failure, skip this tick rather than bypassing backoff and
            // flooding every subscriber. The next tick will retry.
            context.logger.warning("Failed to load cached instance info; skipping this check tick: \(error)")
            return
        }
        let now = Date()

        for subscriber in subscribers {
            if let entry = cached[subscriber.domain],
               let next = entry.nextAttemptAt,
               next > now
            {
                context.logger.trace("Skip instance info check for \(subscriber.domain): next attempt at \(next)")
                continue
            }

            // A held claim names the job it was taken for; leave the domain
            // to that job. A queued claim whose job is confirmed still in the
            // queue is kept alive (up to `maxQueuedJobAgeSeconds`); a running
            // claim is governed by the job's own lease. A claim whose job is
            // gone (finished without managing to release, or lost) or whose
            // lookup failed is not renewed, and never replaced outright: only
            // the claim lapsing lets the domain be dispatched again, so an
            // inconclusive lookup cannot put two fetches in flight.
            if let pending = try await cache.pendingFetch(domain: subscriber.domain) {
                if !pending.isRunning,
                   let queuedAt = await Self.queuedAt(
                       of: JobIdentifier(string: pending.token), on: queue, logger: context.logger
                   ),
                   now.timeIntervalSince(queuedAt) < TimeInterval(Self.maxQueuedJobAgeSeconds)
                {
                    try await cache.renewFetch(
                        domain: subscriber.domain,
                        token: pending.token,
                        ttlSeconds: claimTTL
                    )
                }
                context.logger.trace("Skip instance info check for \(subscriber.domain): a fetch is still pending")
                continue
            }

            let jobID = JobIdentifier()
            guard try await cache.claimFetch(
                domain: subscriber.domain,
                token: jobID.string,
                ttlSeconds: claimTTL
            ) else {
                context.logger.trace("Skip instance info check for \(subscriber.domain): a fetch is still pending")
                continue
            }

            do {
                try await queue.dispatch(
                    InstanceInfoFetchJob.self,
                    InstanceInfoFetchPayload(domain: subscriber.domain, claimToken: jobID.string),
                    maxRetryCount: 0,
                    id: jobID
                ).get()
            } catch {
                // Nothing will run to release the claim; give it back so the
                // next tick can retry the domain.
                try? await cache.releaseFetch(domain: subscriber.domain, token: jobID.string)
                throw error
            }
        }
    }

    /// When the job `id` was queued, if the queue still holds its data.
    /// Drivers report a missing job as an error, which is indistinguishable
    /// here from a failed lookup; both yield `nil` and the caller treats them
    /// alike.
    private static func queuedAt(of id: JobIdentifier, on queue: any Queue, logger: Logger) async -> Date? {
        do {
            return try await queue.get(id).get().queuedAt
        } catch {
            logger.debug("Pending instance info fetch job \(id.string) not found in the queue: \(error)")
            return nil
        }
    }
}
