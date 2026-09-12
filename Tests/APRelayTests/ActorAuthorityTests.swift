import APRelayCore
import Crypto
import _CryptoExtras
import Foundation
import Testing
import Vapor
import VaporTesting
@testable import APRelay

/// Covers the binding between a signature's keyID and the actor id the relay
/// ends up trusting (issue #10).
@Suite("Actor Authority Tests", .serialized)
struct ActorAuthorityTests {
    private static let attackerKey = try! _RSA.Signing.PrivateKey(keySize: .bits2048)
    private static let victimKey = try! _RSA.Signing.PrivateKey(keySize: .bits2048)

    private static let victimActorID = "https://victim.example/users/bob"
    private static let attackerActorURL = "https://evil.example/actor"

    /// Signs `activity` with `key` under `keyID` and returns request headers and body.
    private func signedRequest(
        activity: APActivity,
        key: _RSA.Signing.PrivateKey,
        keyID: String
    ) throws -> (headers: HTTPHeaders, body: ByteBuffer) {
        let data = try JSONEncoder().encode(activity)
        let sigHeaders = try HTTPSignature().sign(
            method: "post",
            path: "/inbox",
            host: "localhost",
            body: data,
            privateKey: key,
            keyID: keyID
        )
        var headers = HTTPHeaders()
        for (name, value) in sigHeaders {
            headers.add(name: name, value: value)
        }
        return (headers, ByteBuffer(data: data))
    }

    private func configure(fetcher: URLKeyedActorFetcher) -> @Sendable (Application) async throws -> Void {
        { app in
            try await testConfigure(app)
            app.actorFetcher = fetcher
        }
    }

    // MARK: - Spoofing

    @Test("Document at attacker keyID claiming a victim id is rejected when the victim serves a different key")
    func spoofedIDRejected() async throws {
        let fetcher = URLKeyedActorFetcher(documents: [
            Self.attackerActorURL: .actorDocument(
                id: Self.victimActorID,
                inbox: "https://evil.example/inbox",
                publicKeyPEM: Self.attackerKey.publicKey.pemRepresentation
            ),
            Self.victimActorID: .actorDocument(
                id: Self.victimActorID,
                inbox: "https://victim.example/inbox",
                publicKeyPEM: Self.victimKey.publicKey.pemRepresentation
            ),
        ])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let existing = Subscriber(
                domain: "victim.example",
                inboxURL: "https://victim.example/inbox",
                actorID: Self.victimActorID,
                state: .accepted,
                followActivityID: "https://victim.example/activities/follow-1"
            )
            try await app.repository.saveSubscriber(existing)

            let activity = TestSigning.makeFollowActivity(actor: Self.victimActorID)
            let (headers, body) = try signedRequest(
                activity: activity,
                key: Self.attackerKey,
                keyID: "\(Self.attackerActorURL)#main-key"
            )

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .unauthorized)
            }

            let subscriber = try await app.repository.getSubscriber(domain: "victim.example")
            #expect(subscriber?.inboxURL == "https://victim.example/inbox")
            #expect(subscriber?.followActivityID == "https://victim.example/activities/follow-1")
            #expect(await fetcher.log.urls == [Self.attackerActorURL, Self.victimActorID])
        }
    }

    @Test("Spoofed Undo cannot remove the victim's subscription")
    func spoofedUndoRejected() async throws {
        let fetcher = URLKeyedActorFetcher(documents: [
            Self.attackerActorURL: .actorDocument(
                id: Self.victimActorID,
                publicKeyPEM: Self.attackerKey.publicKey.pemRepresentation
            ),
            Self.victimActorID: .actorDocument(
                id: Self.victimActorID,
                publicKeyPEM: Self.victimKey.publicKey.pemRepresentation
            ),
        ])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let followID = "https://victim.example/activities/follow-1"
            try await app.repository.saveSubscriber(Subscriber(
                domain: "victim.example",
                inboxURL: "https://victim.example/inbox",
                actorID: Self.victimActorID,
                state: .accepted,
                followActivityID: followID
            ))

            let activity = TestSigning.makeUndoActivity(actor: Self.victimActorID, followID: followID)
            let (headers, body) = try signedRequest(
                activity: activity,
                key: Self.attackerKey,
                keyID: "\(Self.attackerActorURL)#main-key"
            )

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .unauthorized)
            }

            let subscriber = try await app.repository.getSubscriber(domain: "victim.example")
            #expect(subscriber != nil)
        }
    }

    @Test("Claimed id that cannot be fetched is rejected")
    func unreachableClaimedIDRejected() async throws {
        let fetcher = URLKeyedActorFetcher(documents: [
            Self.attackerActorURL: .actorDocument(
                id: Self.victimActorID,
                publicKeyPEM: Self.attackerKey.publicKey.pemRepresentation
            ),
        ])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let activity = TestSigning.makeFollowActivity(actor: Self.victimActorID)
            let (headers, body) = try signedRequest(
                activity: activity,
                key: Self.attackerKey,
                keyID: "\(Self.attackerActorURL)#main-key"
            )

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .unauthorized)
            }

            let subscribers = try await app.repository.getAllSubscribers(state: nil)
            #expect(subscribers.isEmpty)
        }
    }

    @Test("Invalid signature does not trigger the authority re-fetch")
    func invalidSignatureSkipsRefetch() async throws {
        let fetcher = URLKeyedActorFetcher(documents: [
            Self.attackerActorURL: .actorDocument(
                id: Self.victimActorID,
                publicKeyPEM: Self.attackerKey.publicKey.pemRepresentation
            ),
            Self.victimActorID: .actorDocument(
                id: Self.victimActorID,
                publicKeyPEM: Self.victimKey.publicKey.pemRepresentation
            ),
        ])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let activity = TestSigning.makeFollowActivity(actor: Self.victimActorID)
            // Signed with a key that matches neither document.
            let (headers, body) = try signedRequest(
                activity: activity,
                key: TestSigning.privateKey,
                keyID: "\(Self.attackerActorURL)#main-key"
            )

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .unauthorized)
            }

            #expect(await fetcher.log.urls == [Self.attackerActorURL])
        }
    }

    // MARK: - Legitimate id differences

    @Test("Document claiming a canonical id that vouches for the key is accepted under that id")
    func canonicalIDAccepted() async throws {
        let oldURL = "https://remote.example/actor-old"
        let canonical = TestSigning.testActorID
        let document = RemoteActor.actorDocument(
            id: canonical,
            inbox: TestSigning.testInboxURL,
            publicKeyPEM: TestSigning.publicKeyPEM
        )
        let fetcher = URLKeyedActorFetcher(documents: [oldURL: document, canonical: document])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let activity = TestSigning.makeFollowActivity(actor: canonical)
            let (headers, body) = try signedRequest(
                activity: activity,
                key: TestSigning.privateKey,
                keyID: "\(oldURL)#main-key"
            )

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            let subscriber = try await app.repository.getSubscriber(domain: TestSigning.testActorDomain)
            #expect(subscriber?.actorID == canonical)
            #expect(await fetcher.log.urls == [oldURL, canonical])
        }
    }

    @Test("Canonical document serializing the same key differently is still accepted")
    func canonicalIDWithDifferentPEMSerializationAccepted() async throws {
        let oldURL = "https://remote.example/actor-old"
        let canonical = TestSigning.testActorID
        // Same key, different serialization: CRLF line endings and no trailing newline.
        let reserialized = TestSigning.publicKeyPEM
            .trimmingCharacters(in: .newlines)
            .replacingOccurrences(of: "\n", with: "\r\n")
        #expect(reserialized != TestSigning.publicKeyPEM)

        let fetcher = URLKeyedActorFetcher(documents: [
            oldURL: .actorDocument(id: canonical, publicKeyPEM: TestSigning.publicKeyPEM),
            canonical: .actorDocument(id: canonical, inbox: TestSigning.testInboxURL, publicKeyPEM: reserialized),
        ])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let activity = TestSigning.makeFollowActivity(actor: canonical)
            let (headers, body) = try signedRequest(
                activity: activity,
                key: TestSigning.privateKey,
                keyID: "\(oldURL)#main-key"
            )

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            let subscriber = try await app.repository.getSubscriber(domain: TestSigning.testActorDomain)
            #expect(subscriber?.actorID == canonical)
        }
    }

    @Test("Host case and explicit default port differences do not require a re-fetch")
    func equivalentURLNotRefetched() async throws {
        let keyIDBase = "https://Remote.Example:443/actor"
        let document = RemoteActor.actorDocument(
            id: TestSigning.testActorID,
            inbox: TestSigning.testInboxURL,
            publicKeyPEM: TestSigning.publicKeyPEM
        )
        let fetcher = URLKeyedActorFetcher(documents: [keyIDBase: document])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let activity = TestSigning.makeFollowActivity()
            let (headers, body) = try signedRequest(
                activity: activity,
                key: TestSigning.privateKey,
                keyID: "\(keyIDBase)#main-key"
            )

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            #expect(await fetcher.log.urls == [keyIDBase])
        }
    }

    // MARK: - Standalone Key documents

    @Test("Standalone Key document is followed to an owner that advertises the same key")
    func keyDocumentFollowedToOwner() async throws {
        let keyURL = "https://remote.example/keys/1"
        let fetcher = URLKeyedActorFetcher(documents: [
            keyURL: .keyDocument(
                id: keyURL,
                owner: TestSigning.testActorID,
                publicKeyPEM: TestSigning.publicKeyPEM
            ),
            TestSigning.testActorID: .actorDocument(
                id: TestSigning.testActorID,
                inbox: TestSigning.testInboxURL,
                publicKeyPEM: TestSigning.publicKeyPEM
            ),
        ])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let activity = TestSigning.makeFollowActivity()
            let (headers, body) = try signedRequest(
                activity: activity,
                key: TestSigning.privateKey,
                keyID: keyURL
            )

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }

            let subscriber = try await app.repository.getSubscriber(domain: TestSigning.testActorDomain)
            #expect(subscriber?.actorID == TestSigning.testActorID)
            #expect(await fetcher.log.urls == [keyURL, TestSigning.testActorID])
        }
    }

    @Test("Standalone Key document whose owner advertises a different key is rejected")
    func keyDocumentWithDisownedKeyRejected() async throws {
        let keyURL = "https://evil.example/keys/1"
        let fetcher = URLKeyedActorFetcher(documents: [
            keyURL: .keyDocument(
                id: keyURL,
                owner: Self.victimActorID,
                publicKeyPEM: Self.attackerKey.publicKey.pemRepresentation
            ),
            Self.victimActorID: .actorDocument(
                id: Self.victimActorID,
                publicKeyPEM: Self.victimKey.publicKey.pemRepresentation
            ),
        ])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let activity = TestSigning.makeFollowActivity(actor: Self.victimActorID)
            let (headers, body) = try signedRequest(
                activity: activity,
                key: Self.attackerKey,
                keyID: keyURL
            )

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .unauthorized)
            }
        }
    }

    // MARK: - publicKey.owner

    @Test("publicKey.owner that differs from the actor id is rejected")
    func ownerMismatchRejected() async throws {
        let fetcher = URLKeyedActorFetcher(documents: [
            TestSigning.testActorID: .actorDocument(
                id: TestSigning.testActorID,
                publicKeyPEM: TestSigning.publicKeyPEM,
                owner: "https://remote.example/someone-else"
            ),
        ])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let activity = TestSigning.makeFollowActivity()
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .unauthorized)
            }
        }
    }

    @Test("Missing publicKey.owner is tolerated")
    func missingOwnerAccepted() async throws {
        let fetcher = URLKeyedActorFetcher(documents: [
            TestSigning.testActorID: .actorDocument(
                id: TestSigning.testActorID,
                inbox: TestSigning.testInboxURL,
                publicKeyPEM: TestSigning.publicKeyPEM,
                owner: .some(nil)
            ),
        ])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let activity = TestSigning.makeFollowActivity()
            let (headers, body) = try TestSigning.signedRequest(activity: activity)

            try await app.testing().test(.POST, "inbox", headers: headers, body: body) {
                res async in
                #expect(res.status == .accepted)
            }
        }
    }
}

// MARK: - keyID resolution

@Suite("KeyID Resolution Tests")
struct KeyIDResolutionTests {
    @Test(
        "Resolves actor URL from every known keyID shape",
        arguments: [
            // Mastodon, Akkoma, Friendica: fragment
            ("https://mastodon.example/users/alice#main-key", "https://mastodon.example/users/alice"),
            ("https://mastodon.example/actor#main-key", "https://mastodon.example/actor"),
            // Misskey: path-based /publickey
            ("https://misskey.example/users/9abcdef/publickey", "https://misskey.example/users/9abcdef"),
            // GoToSocial: path-based /main-key
            ("https://gts.example/users/alice/main-key", "https://gts.example/users/alice"),
            // Hubzilla: key id is the actor id itself
            ("https://hub.example/channel/alice", "https://hub.example/channel/alice"),
            // Unknown shape: returned unchanged, resolved via the Key document path
            ("https://other.example/keys/1", "https://other.example/keys/1"),
        ]
    )
    func resolvesActorURL(keyID: String, expected: String) {
        #expect(HTTPSignatureVerificationMiddleware.resolveActorURL(from: keyID) == expected)
    }
}
