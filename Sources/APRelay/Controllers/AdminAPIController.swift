import Queues
import Vapor

struct AdminAPIController: RouteCollection {
    func boot(routes: any RoutesBuilder) throws {
        let admin = routes.grouped("api", "admin").grouped(AdminAuthMiddleware())
        admin.get("subscribers", use: listSubscribers)
        admin.post("subscribers", ":domain", "accept", use: acceptSubscriber)
        admin.post("subscribers", ":domain", "reject", use: rejectSubscriber)
        admin.delete("subscribers", ":domain", use: removeSubscriber)
        admin.get("blocked-domains", use: listBlockedDomains)
        admin.post("blocked-domains", use: blockDomain)
        admin.delete("blocked-domains", ":domain", use: unblockDomain)
    }

    // MARK: - Subscribers

    @Sendable
    private func listSubscribers(req: Request) async throws -> [Subscriber] {
        let stateFilter = req.query[String.self, at: "state"]
        let state = stateFilter.flatMap { SubscriberState(rawValue: $0) }
        return try await req.repository.getAllSubscribers(state: state)
    }

    @Sendable
    private func acceptSubscriber(req: Request) async throws -> AdminResponse {
        let domain = req.parameters.get("domain")!

        return try await withSubscriberLock(domain: domain, req: req) { lease in
            guard var subscriber = try await req.repository.getSubscriber(domain: domain) else {
                throw Abort(.notFound, reason: "Subscriber not found")
            }

            guard subscriber.state != .accepted else {
                return AdminResponse(status: "accepted", domain: domain)
            }

            subscriber.state = .accepted

            // LitePub: if following relay actor directly, prepare outbound follow before saving.
            if subscriber.followObjectURI == req.relayConfig.actorURL {
                subscriber.outboundFollowActivityID = req.relayConfig.makeActivityID()
            }

            var notifications: [SubscriberNotification] = [
                .accept(
                    AcceptPayload(
                        inboxURL: subscriber.inboxURL,
                        followActivityID: subscriber.followActivityID,
                        followerActorID: subscriber.actorID,
                        followObjectURI: subscriber.followObjectURI,
                        activityID: req.relayConfig.makeActivityID()
                    )
                ),
            ]

            // LitePub: if instance followed the relay actor directly, follow back.
            if subscriber.followObjectURI == req.relayConfig.actorURL,
               let outboundFollowID = subscriber.outboundFollowActivityID
            {
                notifications.append(
                    .follow(
                        FollowPayload(
                            inboxURL: subscriber.inboxURL,
                            targetActorID: subscriber.actorID,
                            followActivityID: outboundFollowID
                        )
                    )
                )
            }

            guard try await req.saveSubscriber(subscriber, lease: lease, notifying: notifications) else {
                throw Abort(.conflict, reason: "Domain is blocked")
            }

            try await req.queues(.instanceInfo).dispatch(
                InstanceInfoFetchJob.self,
                InstanceInfoFetchPayload(domain: domain),
                maxRetryCount: 0
            )

            return AdminResponse(status: "accepted", domain: domain)
        }
    }

    @Sendable
    private func rejectSubscriber(req: Request) async throws -> AdminResponse {
        let domain = req.parameters.get("domain")!

        return try await withSubscriberLock(domain: domain, req: req) { lease in
            guard var subscriber = try await req.repository.getSubscriber(domain: domain) else {
                throw Abort(.notFound, reason: "Subscriber not found")
            }

            guard subscriber.state != .rejected else {
                return AdminResponse(status: "rejected", domain: domain)
            }

            subscriber.state = .rejected

            // The Reject, and the Undo Follow for our outbound Follow (LitePub),
            // are recorded with the write: sent only if it commits, and still
            // sent if queuing them fails after it did.
            let notifications: [SubscriberNotification] = [
                .reject(
                    RejectPayload(
                        inboxURL: subscriber.inboxURL,
                        followActivityID: subscriber.followActivityID,
                        followerActorID: subscriber.actorID,
                        followObjectURI: subscriber.followObjectURI,
                        activityID: req.relayConfig.makeActivityID()
                    )
                ),
            ] + [subscriber.undoFollowNotification(config: req.relayConfig)].compactMap { $0 }
            subscriber.outboundFollowActivityID = nil

            guard try await req.saveSubscriber(subscriber, lease: lease, notifying: notifications) else {
                throw Abort(.conflict, reason: "Domain is blocked")
            }

            return AdminResponse(status: "rejected", domain: domain)
        }
    }

    @Sendable
    private func removeSubscriber(req: Request) async throws -> AdminResponse {
        let domain = req.parameters.get("domain")!

        return try await withSubscriberLock(domain: domain, req: req) { lease in
            guard let subscriber = try await req.repository.getSubscriber(domain: domain) else {
                throw Abort(.notFound, reason: "Subscriber not found")
            }

            // LitePub: if we had an outbound Follow, send Undo Follow.
            try await req.deleteSubscriber(
                domain: domain,
                lease: lease,
                notifying: [subscriber.undoFollowNotification(config: req.relayConfig)].compactMap { $0 }
            )
            return AdminResponse(status: "removed", domain: domain)
        }
    }

    /// Runs a change to a subscriber under its lock, so it cannot interleave
    /// with an inbox Follow/Undo/Reject or another admin change for the same
    /// domain.
    ///
    /// A lock held by another change for the whole wait, or a change
    /// superseded by a later lock holder's write, answers 409 so the operator
    /// can retry. A failure to take the lock at all surfaces as the underlying
    /// error.
    private func withSubscriberLock<Result>(
        domain: String,
        req: Request,
        _ body: (SubscriberLease) async throws -> Result
    ) async throws -> Result {
        do {
            return try await req.withSubscriberLock(domain: domain, body)
        } catch SubscriberLockError.timedOut, SubscriberLockError.superseded {
            throw Abort(.conflict, reason: "Subscriber is being updated by another request")
        } catch SubscriberLockError.unavailable(_, let underlying) {
            throw underlying
        }
    }

    // MARK: - Blocked Domains

    @Sendable
    private func listBlockedDomains(req: Request) async throws -> [BlockedDomain] {
        try await req.repository.getAllBlockedDomains()
    }

    @Sendable
    private func blockDomain(req: Request) async throws -> AdminResponse {
        let body = try req.content.decode(BlockRequest.self)

        // Block and remove the subscriber under one lock, so a lock timeout
        // leaves nothing half done and the request can simply be retried.
        return try await withSubscriberLock(domain: body.domain, req: req) { lease in
            let added = try await req.repository.blockDomain(body.domain, reason: body.reason)

            // Remove the subscriber even when the domain was already blocked,
            // so a retry finishes the cleanup an interrupted earlier block left
            // behind; a blocked subscriber would otherwise keep receiving
            // broadcasts.
            if let subscriber = try await req.repository.getSubscriber(domain: body.domain) {
                // LitePub: if we had an outbound Follow, send Undo Follow.
                try await req.deleteSubscriber(
                    domain: body.domain,
                    lease: lease,
                    notifying: [subscriber.undoFollowNotification(config: req.relayConfig)].compactMap { $0 }
                )
            }

            guard added else {
                throw Abort(.conflict, reason: "Domain already blocked")
            }
            return AdminResponse(status: "blocked", domain: body.domain)
        }
    }

    @Sendable
    private func unblockDomain(req: Request) async throws -> AdminResponse {
        let domain = req.parameters.get("domain")!

        let removed = try await req.repository.unblockDomain(domain)
        if !removed {
            throw Abort(.notFound, reason: "Domain not blocked")
        }

        return AdminResponse(status: "unblocked", domain: domain)
    }
}

// MARK: - Request/Response DTOs

struct BlockRequest: Content {
    let domain: String
    let reason: String?
}

struct AdminResponse: Content {
    let status: String
    let domain: String
}
