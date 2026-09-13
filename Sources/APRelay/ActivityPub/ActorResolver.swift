import APRelayCore
import Foundation
import Vapor

/// Why a key id URL could not be resolved to a verified actor.
///
/// Every case is masked to the uniform 401 by the middleware; the payload is
/// for server-side logs only, since it may echo an attacker-controlled URL.
enum ActorResolutionError: Error {
    /// The URL failed recently and is refused until the negative entry expires.
    case negativelyCached(String)
    /// Fetching the URL failed.
    case fetchFailed(String, any Error)
    /// The URL served a document that cannot vouch for a signing key in any role.
    case unusableDocument(String, reason: String)
    /// The URL was named as an actor's authority but does not identify
    /// itself as one; it may still be a valid key id URL.
    case notAuthoritative(String, reason: String)
    /// Another replica is still fetching the URL and did not finish in time.
    case waitTimedOut(String)

    /// The negative entry this failure warrants for `url`, if any. A failure
    /// that happened further down the chain makes `url` unusable outright;
    /// only `url`'s own refusal to act as an authority is scoped to that,
    /// and `url`'s own existing negative entry is left as it is.
    func negativeScope(for url: String) -> NegativeScope? {
        switch self {
        case .negativelyCached(let failedURL) where failedURL == url: nil
        case .notAuthoritative(let failedURL, _) where failedURL == url: .authority
        case .negativelyCached, .fetchFailed, .unusableDocument, .notAuthoritative: .document
        case .waitTimedOut: nil
        }
    }
}

/// A verified actor and whether it came from the cache or a fresh fetch.
struct ResolvedActor {
    let actor: VerifiedActor
    let fromCache: Bool
}

/// Resolves the URL a signature's key id points at to the actor that is
/// authoritative for the signing key, through the shared ``ActorCaching``.
///
/// The chain of documents mirrors Mastodon's `FetchRemoteKeyService`: a
/// document is trusted only when the resource its `id` names is the one
/// that was fetched, or when that id, fetched itself, identifies itself and
/// advertises a key. A standalone Key document is followed to its `owner`
/// under the same rule. The signing key handed back is always the one the
/// canonical document advertises, never the one a document at some other
/// URL claims for it. Which documents to trust does not depend on the
/// request, so the outcome is cached under the canonical id, with the key
/// id URL as an alias when it differs.
///
/// Around each fetch: a URL is fetched by one replica at a time (other
/// requests for it wait for the cache entry), a URL that failed is refused
/// for a while without another fetch, and a request that does not verify
/// against a cached key may trigger one re-fetch per actor per interval.
/// The canonical document goes through the same coordination under its own
/// URL, so aliases cannot multiply fetches of one origin. A request holds
/// one fetch claim at a time: the claim on a URL is released once its
/// document has been read and the URL it leads to recorded, before that
/// URL is confirmed under its own claim. Together these bound how many
/// outbound requests an unauthenticated request can cause, which is what
/// makes the cache a security measure rather than an optimization.
struct ActorResolver {
    let fetcher: any ActorFetcher
    let cache: any ActorCaching
    let policy: ActorCachePolicy
    let client: any Client
    let logger: Logger

    /// Resolves `fetchURL` to a verified actor.
    ///
    /// With `refreshing` set to the id of the actor whose cached key just
    /// failed to verify (and whose re-fetch hold this request claimed), the
    /// document at `fetchURL` is fetched again and that actor is fetched
    /// again wherever the chain reaches it; any other actor the chain
    /// reaches is fetched only if its own hold allows it.
    ///
    /// Throws ``ActorResolutionError`` for anything about the remote
    /// documents; errors from the cache itself propagate unchanged.
    func resolve(fetchURL: String, refreshing: String? = nil) async throws -> ResolvedActor {
        let url = try Self.normalized(fetchURL)
        let refreshing = refreshing.map(Self.holdID)
        if refreshing == nil, let target = try await cachedTarget(for: url) {
            do {
                return try await follow(target, from: url, refreshing: nil, mappingFromCache: true)
            } catch let error as ActorResolutionError where error.negativeScope(for: url) != nil {
                // The cached mapping no longer leads anywhere usable. That
                // says the mapping is stale, not that the URL is; read the
                // document again before judging the URL by what it says now.
            }
        }
        let target = try await withFetchClaim(
            url: url,
            scopes: [.document],
            recheck: { refreshing == nil ? try await cachedTarget(for: url) : nil }
        ) {
            try await fetchTarget(at: url)
        }
        return try await follow(target, from: url, refreshing: refreshing, mappingFromCache: false)
    }

    // MARK: - Re-fetch Hold

    /// Claims the single re-fetch of the actor `id` allowed per interval.
    func claimRefresh(of id: String) async throws -> Bool {
        try await cache.acquireRefetch(
            id: Self.holdID(id), holdSeconds: policy.refetchIntervalSeconds
        )
    }

    /// Waits for a re-fetch of the actor `id` that another request is
    /// running. Returns promptly when no refresh is in progress, and `false`
    /// when the refresh did not finish in time. What the refresh produced
    /// is then found by resolving the key id URL again through the cache.
    func awaitRefresh(id: String) async throws -> Bool {
        let deadline = ContinuousClock.now + policy.fetchWaitTimeout
        while try await cache.refetchState(id: Self.holdID(id)) == .refreshing {
            guard ContinuousClock.now < deadline else {
                return false
            }
            try await Task.sleep(for: policy.fetchPollInterval)
        }
        return true
    }

    /// Marks a claimed re-fetch of `id` as done, whether or not the actor
    /// could be fetched. Best-effort: a hold left in the refreshing state
    /// only makes requests wait out their timeout until it expires.
    func settleRefresh(of id: String) async {
        do {
            try await cache.settleRefetch(
                id: Self.holdID(id), holdSeconds: policy.refetchIntervalSeconds
            )
        } catch {
            logger.warning("Failed to settle the re-fetch hold for \(id): \(error)")
        }
    }

    /// Gives back a claimed re-fetch of `id` that never reached the actor.
    /// Best-effort, for the same reason as ``settleRefresh(of:)``.
    func releaseRefresh(of id: String) async {
        do {
            try await cache.releaseRefetch(id: Self.holdID(id))
        } catch {
            logger.warning("Failed to release the re-fetch hold for \(id): \(error)")
        }
    }

    // MARK: - Chain

    /// Where the document at a URL leads.
    private enum Target {
        /// The URL is the actor itself.
        case resolved(ResolvedActor)
        /// The URL names another id (a Key document's owner, a claimed id)
        /// that must confirm itself.
        case claims(String)
    }

    /// What the cache already knows about `url`: the actor stored under it,
    /// or the id its document was last seen to name.
    private func cachedTarget(for url: String) async throws -> Target? {
        if let actor = try await cache.actor(id: url) {
            return .resolved(ResolvedActor(actor: actor, fromCache: true))
        }
        if let id = try await cache.canonicalID(forURL: url) {
            return .claims(id)
        }
        return nil
    }

    /// Follows where the document at `url` leads. When the id it names
    /// cannot confirm itself, the alias is dropped; the URL itself is
    /// recorded as unusable only when the mapping was just read from its
    /// document, since a mapping from the cache may simply be out of date.
    private func follow(
        _ target: Target, from url: String, refreshing: String?, mappingFromCache: Bool
    ) async throws -> ResolvedActor {
        switch target {
        case .resolved(let resolved):
            return resolved
        case .claims(let id):
            do {
                return try await confirmCanonical(id: id, claimedAt: url, refreshing: refreshing)
            } catch let error as ActorResolutionError where error.negativeScope(for: url) != nil {
                try await cache.removeAlias(url: url)
                if !mappingFromCache {
                    try await cache.markNegative(
                        url: url, scope: .document, ttlSeconds: policy.negativeTTLSeconds
                    )
                }
                throw error
            }
        }
    }

    /// Reads the document at `url` under its claim and records where it
    /// leads, so requests waiting on the claim can follow it from the cache.
    private func fetchTarget(at url: String) async throws -> Target {
        let fetched = try await fetchDocument(at: url)
        if fetched.isKeyDocument, let owner = fetched.owner {
            guard !ActorIdentity.matches(owner, url) else {
                throw ActorResolutionError.unusableDocument(url, reason: "names itself as its owner")
            }
            return .claims(try await pointAlias(url, at: owner))
        }
        if ActorIdentity.matches(fetched.id, url) {
            return .resolved(try await storeTrusted(fetched, at: url))
        }
        // A redirect, a canonical URL that differs from the key id, or a
        // spoofing attempt: only the id's own origin can vouch for it.
        return .claims(try await pointAlias(url, at: fetched.id))
    }

    /// Confirms that `id`, named by the document at `url`, identifies itself
    /// and advertises a key, fetching it under its own claim unless the cache
    /// already has it.
    private func confirmCanonical(
        id: String, claimedAt url: String, refreshing: String?
    ) async throws -> ResolvedActor {
        let id = try Self.normalized(id)
        // The refresh permission belongs to one actor. Reaching a different
        // one, the chain may fetch it only under that actor's own hold.
        var claimedHold = false
        var force = false
        if let refreshing {
            if refreshing == id {
                force = true
            } else {
                force = try await claimRefresh(of: id)
                claimedHold = force
            }
        }
        if !force, let cached = try await confirmedActor(for: id) {
            return ResolvedActor(actor: cached, fromCache: true)
        }
        do {
            return try await withFetchClaim(
                url: id,
                scopes: [.document, .authority],
                recheck: {
                    guard !force, let cached = try await confirmedActor(for: id) else { return nil }
                    return ResolvedActor(actor: cached, fromCache: true)
                }
            ) {
                let fetched = try await fetchDocument(at: id)
                guard !fetched.isKeyDocument else {
                    throw ActorResolutionError.notAuthoritative(
                        id, reason: "is a Key document, not an actor"
                    )
                }
                guard ActorIdentity.matches(fetched.id, id) else {
                    throw ActorResolutionError.notAuthoritative(
                        id, reason: "claims id \(fetched.id) instead of identifying itself"
                    )
                }
                return try await storeTrusted(fetched, at: id)
            }
        } catch {
            if claimedHold {
                await settleRefresh(of: id)
            }
            throw error
        }
    }

    /// The actor already confirmed for `id`: stored under `id` itself, or
    /// under the spelling of the same resource its document advertised. An
    /// alias to a different resource means `id` did not identify itself.
    private func confirmedActor(for id: String) async throws -> VerifiedActor? {
        if let actor = try await cache.actor(id: id) {
            return actor
        }
        guard let spelled = try await cache.canonicalID(forURL: id),
            ActorIdentity.matches(spelled, id)
        else {
            return nil
        }
        return try await cache.actor(id: spelled)
    }

    /// Records that the document at `url` names `id`, and returns the
    /// spelling under which `id` is confirmed. An actor entry stored under
    /// `url` while it was canonical would shadow the alias, so it goes.
    private func pointAlias(_ url: String, at id: String) async throws -> String {
        guard let target = ActorIdentity.normalized(id) else {
            throw ActorResolutionError.unusableDocument(
                url, reason: "names \(id), not an http(s) URL"
            )
        }
        try await cache.evict(id: url)
        try await cache.recordAlias(url: url, canonicalID: target, ttlSeconds: policy.actorTTLSeconds)
        return target
    }

    /// Stores a self-identifying document as the actor at `url`.
    private func storeTrusted(_ document: RemoteActor, at url: String) async throws -> ResolvedActor {
        let actor = try trustedActor(from: document, at: url)
        try await cache.store(actor, ttlSeconds: policy.actorTTLSeconds)
        try await cache.settleRefetch(
            id: Self.holdID(actor.id), holdSeconds: policy.refetchIntervalSeconds
        )
        if url != actor.id {
            // Same resource, different spelling: reach the entry through an alias.
            try await cache.evict(id: url)
            try await cache.recordAlias(
                url: url, canonicalID: actor.id, ttlSeconds: policy.actorTTLSeconds
            )
        } else {
            try await cache.removeAlias(url: url)
        }
        return ResolvedActor(actor: actor, fromCache: false)
    }

    private func trustedActor(from document: RemoteActor, at url: String) throws -> VerifiedActor {
        guard let publicKeyPEM = document.publicKey?.publicKeyPem else {
            throw ActorResolutionError.unusableDocument(url, reason: "carries no public key")
        }
        if let owner = document.publicKey?.owner, !ActorIdentity.matches(owner, document.id) {
            throw ActorResolutionError.unusableDocument(
                url, reason: "publicKey.owner \(owner) does not match actor id \(document.id)"
            )
        }
        return VerifiedActor(
            id: document.id,
            inbox: document.inbox,
            sharedInbox: document.sharedInbox,
            publicKeyPEM: publicKeyPEM
        )
    }

    // MARK: - Fetching

    /// Runs `body` while holding the fetch claim on `url`, or returns what
    /// `recheck` finds in the cache once another holder is done. A failure
    /// of `body` about the document is recorded against `url` while the
    /// claim is still held, so the next holder finds the entry rather than
    /// fetching again.
    private func withFetchClaim<Value>(
        url: String,
        scopes: [NegativeScope],
        recheck: () async throws -> Value?,
        body: () async throws -> Value
    ) async throws -> Value {
        let token = UUID().uuidString
        let deadline = ContinuousClock.now + policy.fetchWaitTimeout
        while true {
            if try await isNegative(url, scopes: scopes) {
                throw ActorResolutionError.negativelyCached(url)
            }
            if try await cache.acquireFetchLock(
                url: url, token: token, ttlSeconds: policy.fetchLockTTLSeconds
            ) {
                var outcome: Result<Value, any Error>
                do {
                    // Another holder may have finished, either way, between
                    // this request's last look at the cache and its claim.
                    if try await isNegative(url, scopes: scopes) {
                        throw ActorResolutionError.negativelyCached(url)
                    }
                    if let found = try await recheck() {
                        outcome = .success(found)
                    } else {
                        outcome = .success(try await body())
                    }
                } catch {
                    outcome = .failure(error)
                }
                if case .failure(let error) = outcome,
                    let resolution = error as? ActorResolutionError,
                    let scope = resolution.negativeScope(for: url)
                {
                    do {
                        try await cache.markNegative(
                            url: url, scope: scope, ttlSeconds: policy.negativeTTLSeconds
                        )
                    } catch {
                        outcome = .failure(error)
                    }
                }
                await releaseFetchLock(url: url, token: token)
                return try outcome.get()
            }

            // Another holder is fetching this URL; its result lands in the cache.
            if let found = try await recheck() {
                return found
            }
            guard ContinuousClock.now < deadline else {
                throw ActorResolutionError.waitTimedOut(url)
            }
            try await Task.sleep(for: policy.fetchPollInterval)
        }
    }

    private func isNegative(_ url: String, scopes: [NegativeScope]) async throws -> Bool {
        for scope in scopes {
            if try await cache.isNegative(url: url, scope: scope) {
                return true
            }
        }
        return false
    }

    private func fetchDocument(at url: String) async throws -> RemoteActor {
        do {
            return try await fetcher.fetchActor(url: url, client: client)
        } catch is CancellationError {
            // The request was abandoned, which says nothing about the URL.
            throw CancellationError()
        } catch {
            throw ActorResolutionError.fetchFailed(url, error)
        }
    }

    /// Releasing the claim is best-effort: a claim left behind only delays
    /// the next fetch of this URL until it expires.
    private func releaseFetchLock(url: String, token: String) async {
        do {
            try await cache.releaseFetchLock(url: url, token: token)
        } catch {
            logger.warning("Failed to release actor fetch claim for \(url): \(error)")
        }
    }

    /// The one spelling of a URL that is fetched, claimed, and cached, so
    /// that spelling variants of one resource (host case, default port,
    /// fragment) cannot multiply fetches or slip past its negative entry.
    /// Anything that is not an http(s) URL with a host is refused before the
    /// cache is touched, so a key id cannot spell out another URL's key.
    private static func normalized(_ url: String) throws -> String {
        guard let normalized = ActorIdentity.normalized(url) else {
            throw ActorResolutionError.unusableDocument(url, reason: "is not an http(s) URL")
        }
        return normalized
    }

    /// Re-fetch holds are keyed by the normalized actor id, so every spelling
    /// of an actor shares one hold. An id that does not normalize (which no
    /// stored actor has) is used as it is.
    private static func holdID(_ id: String) -> String {
        ActorIdentity.normalized(id) ?? id
    }
}
