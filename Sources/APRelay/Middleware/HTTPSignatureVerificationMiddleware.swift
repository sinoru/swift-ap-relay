import APRelayCore
import Crypto
import _CryptoExtras
import Vapor

/// Middleware that verifies HTTP signatures on incoming requests to the inbox.
struct HTTPSignatureVerificationMiddleware: AsyncMiddleware {
    private let httpSignature = HTTPSignature()

    func respond(
        to request: Request,
        chainingTo next: any AsyncResponder
    ) async throws -> Response {
        // Error responses are deliberately uniform — a single `Signature verification failed`
        // with 401 for every failure mode, so error shape cannot be used as an oracle
        // to fingerprint which verification step a probe reached. Details are logged
        // server-side only.
        guard let signatureHeader = request.headers.first(name: "Signature") else {
            request.logger.warning("Signature verification failed: missing Signature header")
            throw Self.genericFailure
        }

        guard let components = httpSignature.parseSignatureHeader(signatureHeader) else {
            request.logger.warning("Signature verification failed: malformed Signature header")
            throw Self.genericFailure
        }

        // Enforce a minimum set of signed headers. draft-cavage-http-signatures
        // leaves this up to the verifier; we follow Mastodon's practice (see
        // app/lib/signed_request.rb in mastodon/mastodon): reject signatures
        // that omit `date` outright, and reject POSTs that omit `digest`.
        //
        // Without `date` in the signed headers list, the Date header value
        // is attacker-controlled and the 12-hour freshness window below
        // cannot be trusted. Without `digest` signed on a POST, body
        // integrity is not cryptographically bound to the signature even
        // though we still compare Digest to the received body. The Misskey,
        // Pleroma, and Akkoma senders all include both in practice, so
        // this does not affect real-world federation compatibility.
        let signedHeaderSet = Set(components.headers.map { $0.lowercased() })
        guard signedHeaderSet.contains("date") else {
            request.logger.warning("Signature verification failed: 'date' not in signed headers")
            throw Self.genericFailure
        }
        if request.method == .POST, !signedHeaderSet.contains("digest") {
            request.logger.warning("Signature verification failed: 'digest' not in signed headers on POST")
            throw Self.genericFailure
        }

        // Validate Date header freshness.
        guard let dateStr = request.headers.first(name: "Date") else {
            request.logger.warning("Signature verification failed: missing Date header")
            throw Self.genericFailure
        }
        guard let date = httpSignature.parseHTTPDate(dateStr) else {
            request.logger.warning("Signature verification failed: invalid Date header format")
            throw Self.genericFailure
        }
        let age = abs(Date().timeIntervalSince(date))
        if age > 43200 {
            request.logger.warning("Signature verification failed: Date header outside 12h window (age=\(Int(age))s)")
            throw Self.genericFailure
        }

        // Collect the body from the stream. Middleware runs before Vapor's
        // route-level body collection, so request.body.data is nil for
        // streamed requests.
        let bodyBuffer = try await request.body.collect(
            max: request.application.routes.defaultMaxBodySize.value
        ).get() ?? ByteBuffer()

        if request.method == .POST {
            guard let digest = request.headers.first(name: "Digest") else {
                request.logger.warning("Signature verification failed: missing Digest header on POST")
                throw Self.genericFailure
            }
            let expectedPrefix = "SHA-256="
            guard digest.hasPrefix(expectedPrefix) else {
                request.logger.warning("Signature verification failed: unsupported digest algorithm in Digest header")
                throw Self.genericFailure
            }
            let expectedHash = String(digest.dropFirst(expectedPrefix.count))
            let actualHash = Data(SHA256.hash(data: bodyBuffer.readableBytesView)).base64EncodedString()
            if expectedHash != actualHash {
                request.logger.warning("Signature verification failed: Digest mismatch")
                throw Self.genericFailure
            }
        }

        // Resolve the document the keyID points at to the actor that is
        // authoritative for the signing key. Normally the actor itself, but
        // it may be a standalone Key object or an old URL for the actor.
        let keyID = components.keyID
        let fetchURL = Self.resolveActorURL(from: keyID)
        let resolver = ActorResolver(
            fetcher: request.application.actorFetcher,
            cache: request.application.actorCache,
            policy: request.application.actorCachePolicy,
            client: request.client,
            logger: request.logger
        )
        var resolved = try await resolveActor(
            fetchURL, with: resolver, request: request, keyID: keyID
        )

        if !signatureVerifies(
            request: request, components: components,
            publicKeyPEM: resolved.actor.publicKeyPEM, keyID: keyID
        ) {
            // A cached key may have been rotated since it was stored. Fetch
            // the actor again, at most once per interval, so a stream of
            // invalid signatures cannot make the relay hammer its origin.
            // Requests that lose the claim to a refresh in progress wait
            // for it instead of being rejected against the key it replaces.
            guard resolved.fromCache else {
                request.logger.warning(
                    "Signature verification failed: signature invalid for keyID=\(keyID)"
                )
                throw Self.genericFailure
            }
            resolved = try await refreshedActor(
                replacing: resolved.actor,
                fetchURL: fetchURL,
                with: resolver,
                request: request,
                keyID: keyID
            )
            guard signatureVerifies(
                request: request, components: components,
                publicKeyPEM: resolved.actor.publicKeyPEM, keyID: keyID
            ) else {
                request.logger.warning(
                    "Signature verification failed: signature invalid for keyID=\(keyID) after refresh"
                )
                throw Self.genericFailure
            }
        }

        // Store verified actor info for downstream handlers.
        request.storage[VerifiedActorKey.self] = resolved.actor

        return try await next.respond(to: request)
    }

    private static let genericFailure = Abort(.unauthorized, reason: "Signature verification failed")

    /// Resolves the key id URL, masking the underlying error.
    ///
    /// The error may include the attacker-controlled URL, so it is logged
    /// server-side only; an unauthenticated caller must not be able to use
    /// error responses to confirm that the server reached a particular URL.
    /// Errors from the cache itself are not about the remote and propagate.
    private func resolveActor(
        _ fetchURL: String,
        with resolver: ActorResolver,
        request: Request,
        keyID: String,
        refreshing: String? = nil
    ) async throws -> ResolvedActor {
        do {
            return try await resolver.resolve(fetchURL: fetchURL, refreshing: refreshing)
        } catch let error as ActorResolutionError {
            request.logger.warning(
                "Signature verification failed: cannot resolve \(fetchURL) (keyID=\(keyID)): \(error)"
            )
            throw Self.genericFailure
        }
    }

    /// Re-fetches `stale` after a signature failed against its cached key,
    /// or waits for the request that is already doing so.
    ///
    /// The hold on the actor decides: unclaimed, this request claims it and
    /// fetches; claimed and still refreshing, this request waits for the
    /// refresh to finish and resolves the key id URL again from what it
    /// stored; settled, the actor was fetched moments ago and the key is
    /// simply wrong, so no request waits and no fetch is made.
    ///
    /// The hold is spent only by a fetch of the actor itself. When the key
    /// id URL fails before the chain reaches the actor, the actor is fetched
    /// directly, so a broken or hostile alias can neither block a rotated
    /// key from being picked up nor burn the actor's hold for nothing; when
    /// the chain ends at another actor, the hold is given back.
    private func refreshedActor(
        replacing stale: VerifiedActor,
        fetchURL: String,
        with resolver: ActorResolver,
        request: Request,
        keyID: String
    ) async throws -> ResolvedActor {
        if try await resolver.claimRefresh(of: stale.id) {
            request.logger.info(
                """
                Signature for keyID=\(keyID) does not verify against the cached key of \(stale.id); \
                re-fetching
                """
            )
            let refreshed: ResolvedActor
            do {
                refreshed = try await resolveActor(
                    fetchURL, with: resolver, request: request, keyID: keyID, refreshing: stale.id
                )
            } catch {
                guard !ActorIdentity.matches(fetchURL, stale.id) else {
                    await resolver.settleRefresh(of: stale.id)
                    throw error
                }
                do {
                    refreshed = try await resolveActor(
                        stale.id, with: resolver, request: request, keyID: keyID, refreshing: stale.id
                    )
                } catch {
                    await resolver.settleRefresh(of: stale.id)
                    throw error
                }
            }
            if !ActorIdentity.matches(refreshed.actor.id, stale.id) {
                await resolver.releaseRefresh(of: stale.id)
            }
            return refreshed
        }

        guard try await resolver.awaitRefresh(id: stale.id) else {
            request.logger.warning(
                """
                Signature verification failed: signature invalid for keyID=\(keyID) \
                and the refresh of \(stale.id) did not finish in time
                """
            )
            throw Self.genericFailure
        }
        // The refresh may have moved the key id URL to another actor; take
        // whatever it now maps to.
        return try await resolveActor(fetchURL, with: resolver, request: request, keyID: keyID)
    }

    /// Whether the request signature verifies against `publicKeyPEM`. A key
    /// that cannot be parsed counts as not verifying.
    private func signatureVerifies(
        request: Request,
        components: SignatureComponents,
        publicKeyPEM: String,
        keyID: String
    ) -> Bool {
        let method = request.method.rawValue.lowercased()
        let path =
            request.url.path
            + (request.url.query.map { "?\($0)" } ?? "")

        // Convert Vapor HTTPHeaders to [String: String] for Core.
        var headerMap: [String: String] = [:]
        for (name, value) in request.headers {
            headerMap[name] = value
        }

        do {
            return try httpSignature.verify(
                method: method,
                path: path,
                requestHeaders: headerMap,
                components: components,
                publicKeyPEM: publicKeyPEM
            )
        } catch {
            request.logger.warning("Signature verification failed: verify threw for keyID=\(keyID): \(error)")
            return false
        }
    }

    /// Resolves the actor URL from a key ID.
    ///
    /// Handles fragment-based (`actor#main-key`; Mastodon, Akkoma, Friendica)
    /// and path-based (`actor/publickey`; Misskey, `actor/main-key`;
    /// GoToSocial) key ID formats. A key ID with neither shape (for example
    /// Hubzilla, whose key id is the actor id itself) is returned unchanged.
    static func resolveActorURL(from keyID: String) -> String {
        // Fragment-based.
        if let hashIndex = keyID.firstIndex(of: "#") {
            return String(keyID.prefix(upTo: hashIndex))
        }

        // Path-based.
        let knownSuffixes = ["/publickey", "/main-key"]
        let lowered = keyID.lowercased()
        for suffix in knownSuffixes {
            if lowered.hasSuffix(suffix) {
                return String(keyID.dropLast(suffix.count))
            }
        }

        return keyID
    }
}

/// Verified actor information stored in request storage after signature
/// verification, and the shape cached by ``ActorCaching``.
struct VerifiedActor: Codable, Equatable, Sendable {
    let id: String
    let inbox: String?
    let sharedInbox: String?
    let publicKeyPEM: String
}

struct VerifiedActorKey: StorageKey {
    typealias Value = VerifiedActor
}

extension Request {
    var verifiedActor: VerifiedActor? {
        storage[VerifiedActorKey.self]
    }
}
