import APRelayCore
import Metrics
import Queues
import Tracing
import Vapor

struct InboxController: RouteCollection {
    func boot(routes: any RoutesBuilder) throws {
        let signed = routes.grouped(HTTPSignatureVerificationMiddleware())
        signed.post("inbox", use: inbox)
    }

    @Sendable
    private func inbox(req: Request) async throws -> HTTPStatus {
        guard let verifiedActor = req.verifiedActor else {
            throw Abort(.unauthorized, reason: "Signature verification required")
        }

        let body = req.body.data ?? ByteBuffer()
        let activity: APActivity
        do {
            activity = try JSONDecoder().decode(APActivity.self, from: body)
        } catch {
            req.logger.warning("Inbox rejected invalid activity JSON: \(error)")
            throw Abort(.badRequest, reason: "Invalid activity")
        }

        // Validate that the activity actor's domain matches the verified signer.
        let signerDomain = extractDomain(from: verifiedActor.id)
        let activityActorDomain = extractDomain(from: activity.actor)
        guard let signerDomain, let activityActorDomain,
            signerDomain == activityActorDomain
        else {
            throw Abort(
                .forbidden,
                reason: "Activity actor domain does not match signer domain"
            )
        }

        req.logger.info("Received \(activity.type) from \(activityActorDomain)")

        // Duplicate detection.
        if try await req.activityDeduplicator.isDuplicate(activity.id) {
            req.logger.info("Duplicate activity ignored: \(activity.id)")
            return .accepted
        }

        Counter(
            label: "relay_inbox_activities_total",
            dimensions: [("type", activity.type)]
        ).increment()

        try await withSpan("inbox.process") { span in
            span.attributes["activity.type"] = activity.type
            span.attributes["activity.actor"] = activity.actor
            span.attributes["activity.id"] = activity.id

            let config = req.relayConfig
            let repository = req.repository

            // Check if domain is blocked.
            let actorDomain = extractDomain(from: activity.actor)
            if let domain = actorDomain {
                if try await repository.isBlocked(domain: domain) {
                    throw Abort(.forbidden, reason: "Domain is blocked")
                }

                if config.restrictedMode {
                    if try await !repository.isAllowed(domain: domain) {
                        throw Abort(.forbidden, reason: "Domain is not in allowlist")
                    }
                }
            }

            switch activity.type {
            case "Follow":
                try await handleFollow(
                    activity: activity,
                    verifiedActor: verifiedActor,
                    req: req
                )
            case "Undo":
                try await handleUndo(
                    activity: activity,
                    verifiedActor: verifiedActor,
                    body: Data(buffer: body),
                    req: req
                )
            case "Accept":
                try await handleAccept(activity: activity, verifiedActor: verifiedActor, req: req)
            case "Reject":
                try await handleReject(activity: activity, verifiedActor: verifiedActor, req: req)
            case "Create", "Announce", "Delete", "Update", "Move", "Add", "Remove", "Like", "EmojiReact":
                try await handleActivity(activity: activity, body: Data(buffer: body), req: req)
            default:
                req.logger.info("Ignoring unsupported activity type: \(activity.type)")
            }
        }

        return .accepted
    }

    // MARK: - Follow

    private func handleFollow(
        activity: APActivity,
        verifiedActor: VerifiedActor,
        req: Request
    ) async throws {
        let config = req.relayConfig
        let repository = req.repository

        // The stored actorID is what handleUndo and
        // validateOutboundFollowResponse later compare the verified signer
        // against, so bind it to the signature here rather than trusting
        // the unauthenticated body field.
        guard activity.actor == verifiedActor.id else {
            req.logger.notice(
                "Follow actor \(activity.actor) does not match signer \(verifiedActor.id), ignoring"
            )
            return
        }

        guard let object = activity.object,
            case .uri(let objectURI) = object,
            isPublicURI(objectURI) || objectURI == config.actorURL
        else {
            req.logger.info("Follow target is not recognized, ignoring")
            return
        }

        let actorDomain = extractDomain(from: activity.actor) ?? activity.actor
        let inboxURL = verifiedActor.sharedInbox
            ?? verifiedActor.inbox
            ?? guessInboxURL(from: activity.actor)

        let initialState: SubscriberState = config.manualAccept ? .pending : .accepted
        var effectiveState = initialState
        var currentSubscriber: Subscriber?

        if var existing = try await repository.getSubscriber(domain: actorDomain) {
            // If switching from LitePub (relay actor) to Mastodon (public), clean up outbound follow.
            if existing.followObjectURI == config.actorURL && objectURI != config.actorURL {
                try await existing.dispatchUndoFollowIfNeeded(on: req.queue)
                existing.outboundFollowActivityID = nil
            }

            // Rejected state is sticky: once an admin rejects a subscriber,
            // a repeat Follow from the same domain must not silently
            // reinstate it. Reinstatement requires an explicit admin accept
            // via the admin API.
            if existing.state == .rejected {
                req.logger.notice("Ignoring Follow from rejected subscriber: \(actorDomain)")
            }
            existing.actorID = activity.actor
            existing.inboxURL = inboxURL
            existing.followActivityID = activity.id
            existing.followObjectURI = objectURI
            effectiveState = existing.state

            // LitePub: if following relay actor directly and accepted, prepare outbound follow.
            if objectURI == config.actorURL && effectiveState == .accepted
                && existing.outboundFollowActivityID == nil
            {
                existing.outboundFollowActivityID = "\(config.baseURL)/activities/\(UUID().uuidString)"
            }

            try await repository.saveSubscriber(existing)
            currentSubscriber = existing
        } else {
            var subscriber = Subscriber(
                domain: actorDomain,
                inboxURL: inboxURL,
                actorID: activity.actor,
                state: initialState,
                followActivityID: activity.id,
                followObjectURI: objectURI,
                createdAt: Date(),
                updatedAt: Date()
            )

            // LitePub: if following relay actor directly and accepted, prepare outbound follow.
            if objectURI == config.actorURL && initialState == .accepted {
                subscriber.outboundFollowActivityID = "\(config.baseURL)/activities/\(UUID().uuidString)"
            }

            try await repository.saveSubscriber(subscriber)
            currentSubscriber = subscriber
        }

        req.logger.notice("Follow from \(actorDomain), state: \(effectiveState.rawValue)")

        if effectiveState == .accepted {
            try await req.queue.dispatch(
                AcceptJob.self,
                AcceptPayload(
                    inboxURL: inboxURL,
                    followActivityID: activity.id,
                    followerActorID: activity.actor,
                    followObjectURI: objectURI
                ),
                maxRetryCount: 5
            )

            // LitePub: if instance followed the relay actor directly, follow back.
            if objectURI == config.actorURL,
               let outboundFollowID = currentSubscriber?.outboundFollowActivityID
            {
                try await req.queue.dispatch(
                    FollowJob.self,
                    FollowPayload(
                        inboxURL: inboxURL,
                        targetActorID: activity.actor,
                        followActivityID: outboundFollowID
                    ),
                    maxRetryCount: 5
                )
            }

            try await req.queues(.instanceInfo).dispatch(
                InstanceInfoFetchJob.self,
                InstanceInfoFetchPayload(domain: actorDomain),
                maxRetryCount: 0
            )
        }
    }

    // MARK: - Undo

    private func handleUndo(
        activity: APActivity,
        verifiedActor: VerifiedActor,
        body: Data,
        req: Request
    ) async throws {
        guard let object = activity.object else { return }

        let actorDomain = extractDomain(from: activity.actor) ?? activity.actor

        // Classify the Undo. An embedded object carries its own type, so a
        // non-Follow Undo (e.g. an un-boost) is recognized without a
        // repository read here. A URI-only object is opaque, so it is
        // treated as an Undo Follow only when it references the subscriber's
        // currently stored Follow — that case needs the lookup to classify.
        // Any other Undo — a non-Follow activity sent as a bare URI, or a
        // stale Undo Follow URI for a superseded Follow — falls through to
        // the broadcast path instead of being silently dropped.
        var subscriber: Subscriber?
        let isUndoFollow: Bool
        switch object {
        case .activity(let inner):
            isUndoFollow = inner.type == "Follow"
        case .object(let inner):
            isUndoFollow = inner.type == "Follow"
        case .uri(let uri):
            subscriber = try await req.repository.getSubscriber(domain: actorDomain)
            isUndoFollow = uri == subscriber?.followActivityID
        }

        guard isUndoFollow else {
            try await handleActivity(activity: activity, body: body, req: req)
            return
        }

        // An embedded Undo Follow deferred its lookup to here so non-Follow
        // undos never pay for it; the URI case already fetched above.
        if subscriber == nil {
            subscriber = try await req.repository.getSubscriber(domain: actorDomain)
        }
        guard let subscriber else { return }

        // Compare both the claimed actor and the actor that actually
        // signed the request: the signature middleware only binds the
        // signer to the activity actor's domain, so the body field
        // alone could be set to the stored actor by any same-domain
        // signer.
        guard subscriber.actorID == activity.actor,
            subscriber.actorID == verifiedActor.id
        else {
            req.logger.notice(
                "Undo actor \(activity.actor) signed by \(verifiedActor.id) does not match subscriber actor \(subscriber.actorID), ignoring"
            )
            return
        }

        // Only honor an Undo that references the currently stored Follow;
        // a stale Undo for a superseded Follow must not remove the
        // subscriber that re-followed since. (For a URI-only object this
        // already held when classifying above, but the embedded forms are
        // re-checked here against the id and actor.)
        let matchesCurrentFollow: Bool
        switch object {
        case .activity(let inner):
            matchesCurrentFollow =
                inner.id == subscriber.followActivityID
                && inner.actor == subscriber.actorID
        case .object(let inner):
            matchesCurrentFollow =
                inner.id == subscriber.followActivityID
                && (inner.actor == nil || inner.actor == subscriber.actorID)
        case .uri(let uri):
            matchesCurrentFollow = uri == subscriber.followActivityID
        }

        guard matchesCurrentFollow else {
            req.logger.notice(
                "Undo object from \(actorDomain) does not reference current Follow, ignoring"
            )
            return
        }

        // A matching Undo removes the record even for a rejected
        // subscriber: the remote's withdrawal is honored, and a domain
        // shedding the sticky rejected state (see handleFollow) via
        // Undo + re-Follow is an accepted trade-off — persistent
        // abusers belong on the block list.
        //
        // LitePub: if we had an outbound Follow, send Undo Follow back.
        try await subscriber.dispatchUndoFollowIfNeeded(on: req.queue)
        try await req.repository.deleteSubscriber(domain: actorDomain)
        req.logger.notice("Removed subscriber: \(actorDomain)")
    }

    // MARK: - Activity (Broadcast)

    private func handleActivity(
        activity: APActivity,
        body: Data,
        req: Request
    ) async throws {
        let actorDomain = extractDomain(from: activity.actor) ?? activity.actor
        let repository = req.repository

        guard
            let subscriber = try await repository.getSubscriber(domain: actorDomain),
            subscriber.state == .accepted
        else {
            req.logger.info("Activity from non-subscriber \(actorDomain), ignoring")
            return
        }

        // Create/Announce are wrapped in a relay-attributed Announce;
        // all other types are forwarded as-is.
        //
        // Why Announce wrapping instead of forwarding the original body:
        //   The relay signs HTTP requests with its own key, so the HTTP signature
        //   actor (relay) differs from the activity's actor (original author).
        //   - Mastodon/Misskey: accept the mismatch when an LD-Signature is present
        //     in the body, but not all origin servers attach one.
        //   - Akkoma: strictly rejects any HTTP-sig/actor mismatch (no LD-Sig
        //     fallback), returning 400 Bad Request.
        //   - Pleroma: falls back to fetching the object from the origin server
        //     for Create activities, but rejects other types.
        //   Wrapping in Announce keeps relay actor == HTTP-sig actor, which every
        //   implementation accepts. Receiving servers then fetch the original note
        //   via the Announce's object URI.
        //
        // Known limitation:
        //   Misskey displays relay Announces as "renotes" from the relay account
        //   because it lacks relay-specific Announce handling.
        //   See: https://github.com/misskey-dev/misskey/issues/11056
        let payload: Data
        switch activity.type {
        case "Create", "Announce":
            let config = req.relayConfig
            let objectURI = activity.object?.uriOrID ?? activity.id

            let announce = APActivity(
                context: .default,
                id: "\(config.baseURL)/activities/\(UUID().uuidString)",
                type: "Announce",
                actor: config.actorURL,
                object: .uri(objectURI),
                to: .single("https://www.w3.org/ns/activitystreams#Public"),
                cc: nil,
                published: Date.ISO8601FormatStyle.apRelay.format(Date())
            )
            payload = try JSONEncoder.apRelay.encode(announce)
        default:
            payload = body
        }

        let inboxURLs = try await repository.getAcceptedInboxURLs()

        let targetInboxes = inboxURLs.filter { $0 != subscriber.inboxURL }
        for inbox in targetInboxes {
            try await req.queues(.delivery).dispatch(
                DeliveryJob.self,
                DeliveryPayload(activity: payload, inboxURL: inbox),
                maxRetryCount: 5
            )
        }

        req.logger.info(
            "Broadcasting \(activity.type) from \(actorDomain) to \(targetInboxes.count) subscribers"
        )
    }

    // MARK: - Accept (LitePub mutual follow)

    private func handleAccept(
        activity: APActivity,
        verifiedActor: VerifiedActor,
        req: Request
    ) async throws {
        guard
            let subscriber = try await validateOutboundFollowResponse(
                activity: activity,
                verifiedActor: verifiedActor,
                req: req
            )
        else { return }

        let actorDomain = subscriber.domain
        req.logger.notice("Instance \(actorDomain) accepted our Follow (mutual follow established)")
    }

    // MARK: - Reject (LitePub mutual follow)

    private func handleReject(
        activity: APActivity,
        verifiedActor: VerifiedActor,
        req: Request
    ) async throws {
        guard
            let subscriber = try await validateOutboundFollowResponse(
                activity: activity,
                verifiedActor: verifiedActor,
                req: req
            )
        else { return }

        let actorDomain = subscriber.domain
        // Remote already rejected our Follow, so no need to send Undo back.
        try await req.repository.deleteSubscriber(domain: actorDomain)

        req.logger.notice(
            "Instance \(actorDomain) rejected our Follow; removed subscriber"
        )
    }

    /// Validates that an incoming Accept/Reject was signed by the stored
    /// subscriber actor and references our outbound Follow.
    /// Returns the matched subscriber, or nil if validation fails.
    private func validateOutboundFollowResponse(
        activity: APActivity,
        verifiedActor: VerifiedActor,
        req: Request
    ) async throws -> Subscriber? {
        let actorDomain = extractDomain(from: activity.actor) ?? activity.actor
        let config = req.relayConfig

        guard let subscriber = try await req.repository.getSubscriber(domain: actorDomain),
            let outboundFollowID = subscriber.outboundFollowActivityID
        else {
            req.logger.info(
                "Received \(activity.type) from \(actorDomain) but no outbound Follow tracked, ignoring"
            )
            return nil
        }

        guard subscriber.actorID == activity.actor,
            subscriber.actorID == verifiedActor.id
        else {
            req.logger.notice(
                "\(activity.type) actor \(activity.actor) signed by \(verifiedActor.id) does not match subscriber actor \(subscriber.actorID), ignoring"
            )
            return nil
        }

        guard let object = activity.object else {
            req.logger.info("\(activity.type) has no object, ignoring")
            return nil
        }

        switch object {
        case .activity(let inner):
            guard inner.type == "Follow", inner.actor == config.actorURL,
                inner.id == outboundFollowID
            else {
                req.logger.info("\(activity.type) inner activity is not our Follow, ignoring")
                return nil
            }
        case .uri(let uri):
            guard uri == outboundFollowID else {
                req.logger.info("\(activity.type) object URI does not match our Follow, ignoring")
                return nil
            }
        case .object(let obj):
            guard obj.id == outboundFollowID else {
                req.logger.info("\(activity.type) object ID does not match our Follow, ignoring")
                return nil
            }
        }

        return subscriber
    }

    // MARK: - Helpers

    private func extractDomain(from uri: String) -> String? {
        URL(string: uri)?.host()
    }

    private func guessInboxURL(from actorURI: String) -> String {
        guard let url = URL(string: actorURI) else { return actorURI }
        return "\(url.scheme ?? "https")://\(url.host() ?? "")/inbox"
    }

    private func isPublicURI(_ uri: String) -> Bool {
        uri == "https://www.w3.org/ns/activitystreams#Public"
            || uri == "as:Public"
            || uri == "Public"
    }
}
