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

        // Reject blocked or non-allowlisted domains before reserving a
        // deduplication slot, so that a domain which is later unblocked or
        // added to the allowlist can re-deliver the same activity id instead
        // of having it absorbed as a duplicate.
        if try await req.repository.isBlocked(domain: activityActorDomain) {
            throw Abort(.forbidden, reason: "Domain is blocked")
        }
        if req.relayConfig.restrictedMode,
            try await !req.repository.isAllowed(domain: activityActorDomain)
        {
            throw Abort(.forbidden, reason: "Domain is not in allowlist")
        }

        // Duplicate detection.
        if try await req.activityDeduplicator.isDuplicate(activity.id) {
            req.logger.info("Duplicate activity ignored: \(activity.id)")
            return .accepted
        }

        Counter(
            label: "relay_inbox_activities_total",
            dimensions: [("type", activity.type)]
        ).increment()

        do {
            try await process(activity: activity, body: body, verifiedActor: verifiedActor, req: req)
        } catch let error as SubscriberLockError {
            // The subscriber lock could not be taken, or a later holder wrote
            // before this change could. Release the dedup slot so the sender's retry
            // is processed rather than absorbed as a duplicate.
            await req.releaseDeduplicationSlot(activity.id)
            switch error {
            case .timedOut(let domain):
                req.logger.warning("Timed out waiting for the subscriber lock on \(domain)")
            case .unavailable(let domain, let underlying):
                req.logger.warning("Could not take the subscriber lock on \(domain): \(underlying)")
            case .superseded(let domain):
                req.logger.warning("Subscriber change on \(domain) was superseded by a later lock holder")
            }
            throw Abort(
                .serviceUnavailable,
                headers: ["Retry-After": "\(req.application.subscriberLockPolicy.ttlSeconds)"],
                reason: "Subscriber is being updated"
            )
        }

        return .accepted
    }

    private func process(
        activity: APActivity,
        body: ByteBuffer,
        verifiedActor: VerifiedActor,
        req: Request
    ) async throws {
        try await withSpan("inbox.process") { span in
            span.attributes["activity.type"] = activity.type
            span.attributes["activity.actor"] = activity.actor
            span.attributes["activity.id"] = activity.id

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
            case "Update", "Delete":
                await evictSignerIfSelfReferenced(
                    activity: activity, verifiedActor: verifiedActor, req: req
                )
                try await handleActivity(activity: activity, body: Data(buffer: body), req: req)
            case "Create", "Announce", "Move", "Add", "Remove", "Like", "EmojiReact":
                try await handleActivity(activity: activity, body: Data(buffer: body), req: req)
            default:
                req.logger.info("Ignoring unsupported activity type: \(activity.type)")
            }
        }
    }

    // MARK: - Follow

    private func handleFollow(
        activity: APActivity,
        verifiedActor: VerifiedActor,
        req: Request
    ) async throws {
        let config = req.relayConfig
        let repository = req.repository

        // This handler stores activity.actor as the subscriber's actorID, and
        // handleUndo / validateOutboundFollowResponse later require the
        // verified signer (verifiedActor.id) to equal that stored actorID. So
        // only store activity.actor once it is confirmed to be the signer
        // here, rather than trusting the unauthenticated body field. Release
        // the dedup slot the inbox reserved for this id so a spoofed Follow
        // cannot block the real actor's later, correctly signed Follow with
        // the same id.
        guard activity.actor == verifiedActor.id else {
            await req.releaseDeduplicationSlot(activity.id)
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

        // Read, decide, and write under the subscriber lock, so an admin
        // accept/reject or another Follow for this domain cannot land between
        // the read and the write and have its decision overwritten.
        try await req.withSubscriberLock(domain: actorDomain) { lease in
            let subscriber: Subscriber
            var notifications: [SubscriberNotification] = []

            if var existing = try await repository.getSubscriber(domain: actorDomain) {
                // If switching from LitePub (relay actor) to Mastodon (public), clean up outbound follow.
                if existing.followObjectURI == config.actorURL && objectURI != config.actorURL {
                    notifications += [existing.undoFollowNotification(config: config)].compactMap { $0 }
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

                // LitePub: if following relay actor directly and accepted, prepare outbound follow.
                if objectURI == config.actorURL && existing.state == .accepted
                    && existing.outboundFollowActivityID == nil
                {
                    existing.outboundFollowActivityID = config.makeActivityID()
                }
                subscriber = existing
            } else {
                let initialState: SubscriberState = config.manualAccept ? .pending : .accepted
                var created = Subscriber(
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
                    created.outboundFollowActivityID = config.makeActivityID()
                }
                subscriber = created
            }

            let isAccepted = subscriber.state == .accepted
            if isAccepted {
                notifications.append(
                    .accept(
                        AcceptPayload(
                            inboxURL: inboxURL,
                            followActivityID: activity.id,
                            followerActorID: activity.actor,
                            followObjectURI: objectURI,
                            activityID: config.makeActivityID()
                        )
                    )
                )

                // LitePub: if instance followed the relay actor directly, follow back.
                if objectURI == config.actorURL,
                   let outboundFollowID = subscriber.outboundFollowActivityID
                {
                    notifications.append(
                        .follow(
                            FollowPayload(
                                inboxURL: inboxURL,
                                targetActorID: activity.actor,
                                followActivityID: outboundFollowID
                            )
                        )
                    )
                }
            }

            try await save(subscriber, activityID: activity.id, lease: lease, notifying: notifications, req: req)
            req.logger.notice("Follow from \(actorDomain), state: \(subscriber.state.rawValue)")

            if isAccepted {
                try await req.queues(.instanceInfo).dispatch(
                    InstanceInfoFetchJob.self,
                    InstanceInfoFetchPayload(domain: actorDomain),
                    maxRetryCount: 0
                )
            }
        }
    }

    /// Persists a Follow's subscriber record with the notifications it
    /// entails, refusing it if the domain was blocked after the inbox check.
    ///
    /// The repository refuses the write atomically for a blocked domain. Like
    /// the inbox's own blocked-domain rejection, release the dedup slot so an
    /// unblock lets the instance re-deliver the same Follow id.
    private func save(
        _ subscriber: Subscriber,
        activityID: String,
        lease: SubscriberLease,
        notifying notifications: [SubscriberNotification],
        req: Request
    ) async throws {
        guard try await req.saveSubscriber(subscriber, lease: lease, notifying: notifications) else {
            await req.releaseDeduplicationSlot(activityID)
            throw Abort(.forbidden, reason: "Domain is blocked")
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

        // Identify the Follow this Undo references in a single pass. An
        // embedded object declares its own type and carries the referenced
        // Follow's id (and inner actor, for a full activity), so a non-Follow
        // Undo (e.g. an un-boost) is recognized and relayed without a
        // repository read. A bare URI is opaque: it is only an Undo Follow
        // when it references the subscriber's stored Follow, which is decided
        // against the lookup below.
        let referencedFollowID: String?
        let referencedInnerActor: String?
        let isBareURI: Bool
        switch object {
        case .activity(let inner):
            guard inner.type == "Follow" else {
                try await handleActivity(activity: activity, body: body, req: req)
                return
            }
            referencedFollowID = inner.id
            referencedInnerActor = inner.actor
            isBareURI = false
        case .object(let inner):
            guard inner.type == "Follow" else {
                try await handleActivity(activity: activity, body: body, req: req)
                return
            }
            referencedFollowID = inner.id
            referencedInnerActor = inner.actor
            isBareURI = false
        case .uri(let uri):
            referencedFollowID = uri
            referencedInnerActor = nil
            isBareURI = true
        }

        let outcome = try await req.withSubscriberLock(domain: actorDomain) { lease in
            try await undoFollow(
                referencedFollowID: referencedFollowID,
                referencedInnerActor: referencedInnerActor,
                isBareURI: isBareURI,
                activity: activity,
                verifiedActor: verifiedActor,
                lease: lease,
                req: req
            )
        }

        // Relay outside the lock: broadcasting dispatches a job per
        // subscriber and does not change this one.
        if outcome == .relay {
            try await handleActivity(activity: activity, body: body, req: req)
        }
    }

    private enum UndoOutcome {
        /// The Undo does not withdraw the stored Follow; relay it as an activity.
        case relay
        /// The Undo was handled here: the subscriber was removed or the Undo ignored.
        case handled
    }

    /// Checks an Undo against the stored Follow and removes the subscriber
    /// when it withdraws it. Runs under the subscriber lock, so a re-Follow
    /// cannot replace the stored Follow between the check and the removal.
    private func undoFollow(
        referencedFollowID: String?,
        referencedInnerActor: String?,
        isBareURI: Bool,
        activity: APActivity,
        verifiedActor: VerifiedActor,
        lease: SubscriberLease,
        req: Request
    ) async throws -> UndoOutcome {
        let actorDomain = extractDomain(from: activity.actor) ?? activity.actor

        guard let subscriber = try await req.repository.getSubscriber(domain: actorDomain) else {
            // A bare-URI Undo may still be a non-Follow activity to relay
            // (handleActivity ignores a non-subscriber); an embedded Undo
            // Follow with no subscriber has nothing to act on.
            return isBareURI ? .relay : .handled
        }

        // A bare URI that does not reference the stored Follow is a non-Follow
        // Undo (e.g. an un-boost); relay it rather than dropping it.
        if isBareURI, referencedFollowID != subscriber.followActivityID {
            return .relay
        }

        // Compare both the claimed actor and the actor that actually signed
        // the request: the signature middleware only binds the signer to the
        // activity actor's domain, so the body field alone could be set to
        // the stored actor by any same-domain signer. On a mismatch, release
        // the dedup slot so a spoofed Undo cannot block the real actor's
        // later, correctly signed activity with the same id.
        guard subscriber.actorID == activity.actor,
            subscriber.actorID == verifiedActor.id
        else {
            await req.releaseDeduplicationSlot(activity.id)
            req.logger.notice(
                "Undo actor \(activity.actor) signed by \(verifiedActor.id) does not match subscriber actor \(subscriber.actorID), ignoring"
            )
            return .handled
        }

        // Only honor an Undo that references the currently stored Follow; a
        // stale Undo for a superseded Follow must not remove the subscriber
        // that re-followed since. (A bare URI already matched above.)
        guard referencedFollowID == subscriber.followActivityID,
            referencedInnerActor == nil || referencedInnerActor == subscriber.actorID
        else {
            // The signer was already verified above; this is a verified
            // subscriber referencing a stale/non-current Follow, which is a
            // content mismatch (info) rather than a signer-identity anomaly
            // (notice).
            req.logger.info(
                "Undo object from \(actorDomain) does not reference current Follow, ignoring"
            )
            return .handled
        }

        // A matching Undo removes the record even for a rejected
        // subscriber: the remote's withdrawal is honored, and a domain
        // shedding the sticky rejected state (see handleFollow) via
        // Undo + re-Follow is an accepted trade-off — persistent
        // abusers belong on the block list.
        //
        // LitePub: if we had an outbound Follow, send Undo Follow back.
        try await req.deleteSubscriber(
            domain: actorDomain,
            lease: lease,
            notifying: [subscriber.undoFollowNotification(config: req.relayConfig)].compactMap { $0 }
        )
        req.logger.notice("Removed subscriber: \(actorDomain)")
        return .handled
    }

    // MARK: - Actor Cache

    /// Drops the signer's cached actor when it announces a change to itself:
    /// an Update whose object is the actor (new key, new inbox) or a Delete
    /// of the actor. Only the verified signer can evict its own entry.
    ///
    /// Best-effort: a stale entry is also refreshed by the re-fetch on
    /// signature failure and expires on its own, so a cache error is logged
    /// rather than turning a relayed activity into a 5xx.
    private func evictSignerIfSelfReferenced(
        activity: APActivity,
        verifiedActor: VerifiedActor,
        req: Request
    ) async {
        guard let objectID = activity.object?.uriOrID,
            ActorIdentity.matches(objectID, verifiedActor.id)
        else {
            return
        }
        do {
            try await req.application.actorCache.evict(id: verifiedActor.id)
            req.logger.info("Evicted cached actor \(verifiedActor.id) after \(activity.type)")
        } catch {
            req.logger.warning("Failed to evict cached actor \(verifiedActor.id): \(error)")
        }
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
                id: config.makeActivityID(),
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
        let actorDomain = extractDomain(from: activity.actor) ?? activity.actor

        // Validate and remove under the subscriber lock, so a re-Follow that
        // replaces the outbound Follow cannot land between the check and the
        // removal.
        try await req.withSubscriberLock(domain: actorDomain) { lease in
            guard
                try await validateOutboundFollowResponse(
                    activity: activity,
                    verifiedActor: verifiedActor,
                    req: req
                ) != nil
            else { return }

            // Remote already rejected our Follow, so no need to send Undo back.
            try await req.deleteSubscriber(domain: actorDomain, lease: lease, notifying: [])

            req.logger.notice(
                "Instance \(actorDomain) rejected our Follow; removed subscriber"
            )
        }
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
            // Release the dedup slot so a spoofed Accept/Reject cannot block
            // the real actor's later, correctly signed response with the
            // same id.
            await req.releaseDeduplicationSlot(activity.id)
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
