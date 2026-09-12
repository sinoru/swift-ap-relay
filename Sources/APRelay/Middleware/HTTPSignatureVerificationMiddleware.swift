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

        // Fetch the document the keyID points at. It is normally the actor
        // itself, but may be a standalone Key object (see RemoteActor).
        let keyID = components.keyID
        let actorURL = Self.resolveActorURL(from: keyID)
        let fetched = try await fetchDocument(at: actorURL, request: request, keyID: keyID)

        guard let signingPEM = fetched.publicKey?.publicKeyPem ?? fetched.publicKeyPem else {
            request.logger.warning("Signature verification failed: document at \(actorURL) carries no public key")
            throw Self.genericFailure
        }

        // Verify the signature first so that a request whose signature does
        // not even match the fetched key cannot trigger the second outbound
        // fetch that authority confirmation may need.
        try verifySignature(
            request: request,
            components: components,
            publicKeyPEM: signingPEM,
            keyID: keyID
        )

        // A valid signature only proves possession of the private key for
        // whatever document the requester-controlled keyID pointed at; the
        // document's self-declared `id` is not yet trustworthy. Confirm that
        // the origin of that id actually vouches for the signing key.
        let (trusted, trustedPEM) = try await confirmAuthority(
            of: fetched,
            fetchedFrom: actorURL,
            components: components,
            request: request
        )

        if let owner = trusted.publicKey?.owner, !ActorIdentity.matches(owner, trusted.id) {
            request.logger.warning(
                "Signature verification failed: publicKey.owner \(owner) does not match actor id \(trusted.id)"
            )
            throw Self.genericFailure
        }

        // Store verified actor info for downstream handlers.
        request.storage[VerifiedActorKey.self] = VerifiedActor(
            id: trusted.id,
            inbox: trusted.inbox,
            sharedInbox: trusted.sharedInbox,
            publicKeyPEM: trustedPEM
        )

        return try await next.respond(to: request)
    }

    private static let genericFailure = Abort(.unauthorized, reason: "Signature verification failed")

    /// Fetches a remote document, masking the underlying error.
    ///
    /// The error may include the attacker-controlled URL, so it is logged
    /// server-side only; an unauthenticated caller must not be able to use
    /// error responses to confirm that the server reached a particular URL.
    private func fetchDocument(at url: String, request: Request, keyID: String) async throws -> RemoteActor {
        do {
            return try await request.application.actorFetcher.fetchActor(url: url, client: request.client)
        } catch {
            request.logger.warning("Signature verification failed: fetch error for \(url) (keyID=\(keyID)): \(error)")
            throw Self.genericFailure
        }
    }

    /// Verifies the request signature against `publicKeyPEM`, throwing the
    /// uniform failure on any problem.
    private func verifySignature(
        request: Request,
        components: SignatureComponents,
        publicKeyPEM: String,
        keyID: String
    ) throws {
        let method = request.method.rawValue.lowercased()
        let path =
            request.url.path
            + (request.url.query.map { "?\($0)" } ?? "")

        // Convert Vapor HTTPHeaders to [String: String] for Core.
        var headerMap: [String: String] = [:]
        for (name, value) in request.headers {
            headerMap[name] = value
        }

        let isValid: Bool
        do {
            isValid = try httpSignature.verify(
                method: method,
                path: path,
                requestHeaders: headerMap,
                components: components,
                publicKeyPEM: publicKeyPEM
            )
        } catch {
            request.logger.warning("Signature verification failed: verify threw for keyID=\(keyID): \(error)")
            throw Self.genericFailure
        }

        guard isValid else {
            request.logger.warning("Signature verification failed: signature invalid for keyID=\(keyID)")
            throw Self.genericFailure
        }
    }

    /// Resolves the actor document that is authoritative for the signing key
    /// and returns it together with the key it advertises.
    ///
    /// Mirrors Mastodon's `FetchRemoteKeyService`: a document is trusted only
    /// when the resource its `id` names is the one that was fetched, or when a
    /// second fetch of that id yields a document whose own key verifies the
    /// request. The signature is re-verified against the authoritative key
    /// rather than comparing PEM strings, so two serializations of the same
    /// key (line endings, PKCS#1 versus SPKI) are not mistaken for different
    /// keys.
    ///
    /// - A standalone Key document is followed to its `owner`, which must
    ///   identify itself and advertise a key that verifies the request.
    /// - An actor document whose `id` matches the fetched URL is trusted as is.
    /// - An actor document claiming a different `id` (a redirect, a canonical
    ///   URL that differs from the keyID, or a spoofing attempt) is re-fetched
    ///   from that id. A spoofed id fails here because its real origin does
    ///   not serve the attacker's key.
    private func confirmAuthority(
        of fetched: RemoteActor,
        fetchedFrom url: String,
        components: SignatureComponents,
        request: Request
    ) async throws -> (actor: RemoteActor, publicKeyPEM: String) {
        if fetched.isKeyDocument, let owner = fetched.owner {
            let ownerDocument = try await fetchDocument(at: owner, request: request, keyID: fetched.id)
            guard ActorIdentity.matches(ownerDocument.id, owner),
                let ownerPEM = ownerDocument.publicKey?.publicKeyPem
            else {
                request.logger.warning(
                    "Signature verification failed: key document \(fetched.id) names owner \(owner) which does not identify itself or has no key"
                )
                throw Self.genericFailure
            }
            try verifySignature(request: request, components: components, publicKeyPEM: ownerPEM, keyID: owner)
            return (ownerDocument, ownerPEM)
        }

        if ActorIdentity.matches(fetched.id, url), let fetchedPEM = fetched.publicKey?.publicKeyPem {
            return (fetched, fetchedPEM)
        }

        let canonical = try await fetchDocument(at: fetched.id, request: request, keyID: fetched.id)
        guard ActorIdentity.matches(canonical.id, fetched.id),
            let canonicalPEM = canonical.publicKey?.publicKeyPem
        else {
            request.logger.warning(
                "Signature verification failed: document at \(url) claims id \(fetched.id) but that id does not identify itself or has no key"
            )
            throw Self.genericFailure
        }
        try verifySignature(request: request, components: components, publicKeyPEM: canonicalPEM, keyID: fetched.id)
        return (canonical, canonicalPEM)
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

/// Verified actor information stored in request storage after signature verification.
struct VerifiedActor: Sendable {
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
