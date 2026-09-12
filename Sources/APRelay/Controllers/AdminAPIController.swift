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

        guard var subscriber = try await req.repository.getSubscriber(domain: domain) else {
            throw Abort(.notFound, reason: "Subscriber not found")
        }

        guard subscriber.state != .accepted else {
            return AdminResponse(status: "accepted", domain: domain)
        }

        subscriber.state = .accepted

        // LitePub: if following relay actor directly, prepare outbound follow before saving.
        if subscriber.followObjectURI == req.relayConfig.actorURL {
            subscriber.outboundFollowActivityID = "\(req.relayConfig.baseURL)/activities/\(UUID().uuidString)"
        }

        guard try await req.repository.saveSubscriber(subscriber) else {
            throw Abort(.conflict, reason: "Domain is blocked")
        }

        try await req.queue.dispatch(
            AcceptJob.self,
            AcceptPayload(
                inboxURL: subscriber.inboxURL,
                followActivityID: subscriber.followActivityID,
                followerActorID: subscriber.actorID,
                followObjectURI: subscriber.followObjectURI
            ),
            maxRetryCount: 5
        )

        // LitePub: if instance followed the relay actor directly, follow back.
        if subscriber.followObjectURI == req.relayConfig.actorURL,
           let outboundFollowID = subscriber.outboundFollowActivityID
        {
            try await req.queue.dispatch(
                FollowJob.self,
                FollowPayload(
                    inboxURL: subscriber.inboxURL,
                    targetActorID: subscriber.actorID,
                    followActivityID: outboundFollowID
                ),
                maxRetryCount: 5
            )
        }

        try await req.queues(.instanceInfo).dispatch(
            InstanceInfoFetchJob.self,
            InstanceInfoFetchPayload(domain: domain),
            maxRetryCount: 0
        )

        return AdminResponse(status: "accepted", domain: domain)
    }

    @Sendable
    private func rejectSubscriber(req: Request) async throws -> AdminResponse {
        let domain = req.parameters.get("domain")!

        guard var subscriber = try await req.repository.getSubscriber(domain: domain) else {
            throw Abort(.notFound, reason: "Subscriber not found")
        }

        guard subscriber.state != .rejected else {
            return AdminResponse(status: "rejected", domain: domain)
        }

        subscriber.state = .rejected

        try await req.queue.dispatch(
            RejectJob.self,
            RejectPayload(
                inboxURL: subscriber.inboxURL,
                followActivityID: subscriber.followActivityID,
                followerActorID: subscriber.actorID,
                followObjectURI: subscriber.followObjectURI
            ),
            maxRetryCount: 5
        )

        // LitePub: if we had an outbound Follow, send Undo Follow.
        try await subscriber.dispatchUndoFollowIfNeeded(on: req.queue)
        subscriber.outboundFollowActivityID = nil

        guard try await req.repository.saveSubscriber(subscriber) else {
            throw Abort(.conflict, reason: "Domain is blocked")
        }

        return AdminResponse(status: "rejected", domain: domain)
    }

    @Sendable
    private func removeSubscriber(req: Request) async throws -> AdminResponse {
        let domain = req.parameters.get("domain")!

        guard let subscriber = try await req.repository.getSubscriber(domain: domain) else {
            throw Abort(.notFound, reason: "Subscriber not found")
        }

        // LitePub: if we had an outbound Follow, send Undo Follow.
        try await subscriber.dispatchUndoFollowIfNeeded(on: req.queue)

        try await req.repository.deleteSubscriber(domain: domain)
        return AdminResponse(status: "removed", domain: domain)
    }

    // MARK: - Blocked Domains

    @Sendable
    private func listBlockedDomains(req: Request) async throws -> [BlockedDomain] {
        try await req.repository.getAllBlockedDomains()
    }

    @Sendable
    private func blockDomain(req: Request) async throws -> AdminResponse {
        let body = try req.content.decode(BlockRequest.self)

        let added = try await req.repository.blockDomain(body.domain, reason: body.reason)
        if !added {
            throw Abort(.conflict, reason: "Domain already blocked")
        }

        // Also remove subscriber if exists.
        if let subscriber = try await req.repository.getSubscriber(domain: body.domain) {
            // LitePub: if we had an outbound Follow, send Undo Follow.
            try await subscriber.dispatchUndoFollowIfNeeded(on: req.queue)
            try await req.repository.deleteSubscriber(domain: body.domain)
        }

        return AdminResponse(status: "blocked", domain: body.domain)
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
