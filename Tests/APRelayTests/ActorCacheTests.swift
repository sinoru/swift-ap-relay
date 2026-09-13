import APRelayCore
import _CryptoExtras
import Foundation
import Testing
import Vapor
import VaporTesting
@testable import APRelay

/// Covers the shared actor cache in front of signature verification: hits,
/// aliases, negative entries, the re-fetch on key rotation, eviction on
/// self-Update/Delete, and the fetch claim that merges concurrent fetches.
@Suite("Actor Cache Tests", .serialized)
struct ActorCacheTests {
    private static let rotatedKey = try! _RSA.Signing.PrivateKey(keySize: .bits2048)
    private static let rotatedAgainKey = try! _RSA.Signing.PrivateKey(keySize: .bits2048)

    private static let actorID = TestSigning.testActorID
    private static let keyID = "\(TestSigning.testActorID)#main-key"
    private static let oldURL = "https://remote.example/actor-old"
    private static let victimActorID = "https://victim.example/users/bob"
    private static let attackerActorURL = "https://evil.example/actor"

    private static func document(publicKeyPEM: String = TestSigning.publicKeyPEM) -> RemoteActor {
        .actorDocument(id: actorID, inbox: TestSigning.testInboxURL, publicKeyPEM: publicKeyPEM)
    }

    private func signedRequest(
        activity: APActivity = TestSigning.makeFollowActivity(),
        key: _RSA.Signing.PrivateKey = TestSigning.privateKey,
        keyID: String = ActorCacheTests.keyID
    ) throws -> (headers: HTTPHeaders, body: ByteBuffer) {
        try signedRequest(json: try JSONEncoder().encode(activity), key: key, keyID: keyID)
    }

    private func signedRequest(
        json: Data,
        key: _RSA.Signing.PrivateKey = TestSigning.privateKey,
        keyID: String = ActorCacheTests.keyID
    ) throws -> (headers: HTTPHeaders, body: ByteBuffer) {
        let sigHeaders = try HTTPSignature().sign(
            method: "post",
            path: "/inbox",
            host: "localhost",
            body: json,
            privateKey: key,
            keyID: keyID
        )
        var headers = HTTPHeaders()
        for (name, value) in sigHeaders {
            headers.add(name: name, value: value)
        }
        return (headers, ByteBuffer(data: json))
    }

    private func configure(fetcher: any ActorFetcher) -> @Sendable (Application) async throws -> Void {
        { app in
            try await testConfigure(app)
            app.actorFetcher = fetcher
            app.actorCachePolicy.fetchPollInterval = .milliseconds(10)
        }
    }

    private func mockCache(_ app: Application) throws -> MockActorCache {
        try #require(app.actorCacheOverride as? MockActorCache)
    }

    private func post(
        _ app: Application,
        _ request: (headers: HTTPHeaders, body: ByteBuffer),
        expecting status: HTTPStatus
    ) async throws {
        try await app.testing().test(.POST, "inbox", headers: request.headers, body: request.body) {
            res async in
            #expect(res.status == status)
        }
    }

    // MARK: - Hits and aliases

    @Test("A second request with the same key id is served from the cache")
    func repeatRequestServedFromCache() async throws {
        let fetcher = URLKeyedActorFetcher(documents: [Self.actorID: Self.document()])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let cache = try mockCache(app)
            try await post(app, try signedRequest(), expecting: .accepted)
            try await post(app, try signedRequest(), expecting: .accepted)

            #expect(await fetcher.log.urls == [Self.actorID])
            #expect(await cache.hasActor(id: Self.actorID))
        }
    }

    @Test("An old key id URL is remembered as an alias of the canonical id")
    func aliasResolvesWithoutFetch() async throws {
        let fetcher = URLKeyedActorFetcher(documents: [
            Self.oldURL: Self.document(),
            Self.actorID: Self.document(),
        ])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            try await post(app, try signedRequest(keyID: "\(Self.oldURL)#main-key"), expecting: .accepted)
            #expect(await fetcher.log.urls == [Self.oldURL, Self.actorID])

            // The canonical id and the alias both hit the one entry.
            try await post(app, try signedRequest(), expecting: .accepted)
            try await post(app, try signedRequest(keyID: "\(Self.oldURL)#main-key"), expecting: .accepted)
            #expect(await fetcher.log.urls == [Self.oldURL, Self.actorID])
        }
    }

    @Test("A cached canonical actor is not fetched again for a new alias, nor renewed by it")
    func cachedCanonicalReusedForNewAlias() async throws {
        let fetcher = URLKeyedActorFetcher(documents: [
            Self.oldURL: Self.document(),
            Self.actorID: Self.document(),
        ])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let cache = try mockCache(app)
            try await post(app, try signedRequest(), expecting: .accepted)
            await cache.expireRefetchHold(id: Self.actorID)

            try await post(app, try signedRequest(keyID: "\(Self.oldURL)#main-key"), expecting: .accepted)

            // The alias document was fetched, the canonical one came from the
            // cache, and reusing it did not arm the re-fetch hold again.
            #expect(await fetcher.log.urls == [Self.actorID, Self.oldURL])
            #expect(await !cache.isRefetchHeld(id: Self.actorID))
        }
    }

    @Test("A rotated key arriving through a new alias is re-fetched")
    func rotatedKeyThroughNewAliasIsRefetched() async throws {
        let original = URLKeyedActorFetcher(documents: [Self.actorID: Self.document()])
        let rotatedPEM = Self.rotatedKey.publicKey.pemRepresentation
        let rotated = URLKeyedActorFetcher(documents: [
            Self.oldURL: Self.document(publicKeyPEM: rotatedPEM),
            Self.actorID: Self.document(publicKeyPEM: rotatedPEM),
        ])

        try await withApp(configure: configure(fetcher: original)) { app in
            let cache = try mockCache(app)
            try await post(app, try signedRequest(), expecting: .accepted)
            await cache.expireRefetchHold(id: Self.actorID)

            // The alias resolves to the cached (old) key, which fails, so the
            // actor is re-fetched through the alias and the new key verifies.
            app.actorFetcher = rotated
            let viaAlias = try signedRequest(key: Self.rotatedKey, keyID: "\(Self.oldURL)#main-key")
            try await post(app, viaAlias, expecting: .accepted)
            #expect(await rotated.log.urls == [Self.oldURL, Self.oldURL, Self.actorID])
        }
    }

    @Test("Concurrent requests through different aliases fetch the canonical actor once")
    func concurrentAliasesShareOneCanonicalFetch() async throws {
        let otherURL = "https://remote.example/actor-older"
        let fetcher = GatedActorFetcher(documents: [
            Self.oldURL: Self.document(),
            otherURL: Self.document(),
            Self.actorID: Self.document(),
        ])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let first = try signedRequest(keyID: "\(Self.oldURL)#main-key")
            let second = try signedRequest(keyID: "\(otherURL)#main-key")
            async let firstStatus = status(app, first)
            async let secondStatus = status(app, second)

            // Both alias documents are being fetched under their own claims.
            while await fetcher.log.urls.count < 2 {
                try await Task.sleep(for: .milliseconds(5))
            }
            await fetcher.gate.open()

            let statuses = try await [firstStatus, secondStatus]
            #expect(statuses == [.accepted, .accepted])
            let urls = await fetcher.log.urls
            #expect(urls.count == 3)
            #expect(urls.filter { $0 == Self.actorID }.count == 1)
        }
    }

    @Test("Spelling variants of a cached canonical id neither fetch it again nor get their own claim")
    func equivalentSpellingsOfCachedCanonicalDoNotRefetch() async throws {
        let aliases = [
            "https://remote.example/alias-1": "\(Self.actorID)#nonce-1",
            "https://remote.example/alias-2": "\(Self.actorID)#nonce-2",
            "https://remote.example/alias-3": "https://Remote.Example:443/actor",
        ]
        var documents = [Self.actorID: Self.document()]
        for (alias, claimed) in aliases {
            documents[alias] = .actorDocument(id: claimed, publicKeyPEM: TestSigning.publicKeyPEM)
        }
        let fetcher = URLKeyedActorFetcher(documents: documents)

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            try await post(app, try signedRequest(), expecting: .accepted)

            for alias in aliases.keys.sorted() {
                let wrongKey = try signedRequest(key: Self.rotatedKey, keyID: "\(alias)#main-key")
                try await post(app, wrongKey, expecting: .unauthorized)
                try await post(app, try signedRequest(keyID: "\(alias)#main-key"), expecting: .accepted)
            }
            let urls = await fetcher.log.urls
            #expect(urls.filter { $0 == Self.actorID }.count == 1)
            #expect(Set(urls) == Set(aliases.keys).union([Self.actorID]))
        }
    }

    // MARK: - Negative entries

    @Test("A failed fetch is recorded before its claim is released")
    func failedFetchPublishedBeforeRelease() async throws {
        let fetcher = URLKeyedActorFetcher(documents: [:])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let cache = try mockCache(app)
            try await post(app, try signedRequest(), expecting: .unauthorized)
            #expect(await cache.wasNegativeAtRelease(url: Self.actorID) == true)
        }
    }

    @Test("A negative entry that lands just before the claim is honored without a fetch")
    func negativeFoundAfterClaimIsHonored() async throws {
        let fetcher = URLKeyedActorFetcher(documents: [Self.actorID: Self.document()])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let cache = try mockCache(app)
            await cache.markNegativeOnNextLockAcquire(url: Self.actorID, scope: .document)
            try await post(app, try signedRequest(), expecting: .unauthorized)
            #expect(await fetcher.log.urls.isEmpty)
        }
    }

    @Test("An unusable canonical document is refused at its own URL for every alias")
    func unusableCanonicalIsNegativelyCachedAtItsURL() async throws {
        let otherURL = "https://remote.example/actor-older"
        let unusable = RemoteActor.actorDocument(
            id: Self.actorID,
            publicKeyPEM: TestSigning.publicKeyPEM,
            owner: "https://remote.example/someone-else"
        )
        let fetcher = URLKeyedActorFetcher(documents: [
            Self.oldURL: Self.document(),
            otherURL: Self.document(),
            Self.actorID: unusable,
        ])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            try await post(app, try signedRequest(keyID: "\(Self.oldURL)#main-key"), expecting: .unauthorized)
            #expect(await fetcher.log.urls == [Self.oldURL, Self.actorID])

            // A new alias fetches its own document but not the canonical one
            // again; the canonical id itself is refused without a fetch.
            try await post(app, try signedRequest(keyID: "\(otherURL)#main-key"), expecting: .unauthorized)
            try await post(app, try signedRequest(), expecting: .unauthorized)
            #expect(await fetcher.log.urls == [Self.oldURL, Self.actorID, otherURL])
        }
    }

    @Test("A key id URL that fails to fetch is refused without another fetch until the entry expires")
    func failedFetchIsNegativelyCached() async throws {
        let fetcher = URLKeyedActorFetcher(documents: [:])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let cache = try mockCache(app)
            try await post(app, try signedRequest(), expecting: .unauthorized)
            try await post(app, try signedRequest(), expecting: .unauthorized)
            #expect(await fetcher.log.urls == [Self.actorID])

            await cache.expireNegative(url: Self.actorID)
            try await post(app, try signedRequest(), expecting: .unauthorized)
            #expect(await fetcher.log.urls == [Self.actorID, Self.actorID])
        }
    }

    @Test("A claimed id that fails to fetch is refused for its own key id too")
    func unreachableCanonicalIsNegativelyCached() async throws {
        let attackerKey = try _RSA.Signing.PrivateKey(keySize: .bits2048)
        let fetcher = URLKeyedActorFetcher(documents: [
            Self.attackerActorURL: .actorDocument(
                id: Self.victimActorID,
                publicKeyPEM: attackerKey.publicKey.pemRepresentation
            ),
        ])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let activity = TestSigning.makeFollowActivity(actor: Self.victimActorID)
            let viaAttacker = try signedRequest(
                activity: activity, key: attackerKey, keyID: "\(Self.attackerActorURL)#main-key"
            )
            try await post(app, viaAttacker, expecting: .unauthorized)
            #expect(await fetcher.log.urls == [Self.attackerActorURL, Self.victimActorID])

            // Neither the attacker URL nor the victim id is fetched again.
            try await post(app, viaAttacker, expecting: .unauthorized)
            let viaVictim = try signedRequest(
                activity: activity, key: attackerKey, keyID: "\(Self.victimActorID)#main-key"
            )
            try await post(app, viaVictim, expecting: .unauthorized)
            #expect(await fetcher.log.urls == [Self.attackerActorURL, Self.victimActorID])
        }
    }

    @Test("A document claiming a legitimate Key URL as its id does not poison that URL")
    func attackerClaimingKeyURLDoesNotPoisonIt() async throws {
        let keyURL = "https://remote.example/keys/1"
        let attackerKey = try _RSA.Signing.PrivateKey(keySize: .bits2048)
        let secondAttackerURL = "https://evil.example/actor-2"
        let fetcher = URLKeyedActorFetcher(documents: [
            keyURL: .keyDocument(id: keyURL, owner: Self.actorID, publicKeyPEM: TestSigning.publicKeyPEM),
            Self.actorID: Self.document(),
            Self.attackerActorURL: .actorDocument(
                id: keyURL, publicKeyPEM: attackerKey.publicKey.pemRepresentation
            ),
            secondAttackerURL: .actorDocument(
                id: keyURL, publicKeyPEM: attackerKey.publicKey.pemRepresentation
            ),
        ])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let activity = TestSigning.makeFollowActivity(actor: Self.actorID)
            let attack = try signedRequest(
                activity: activity, key: attackerKey, keyID: "\(Self.attackerActorURL)#main-key"
            )
            try await post(app, attack, expecting: .unauthorized)
            #expect(await fetcher.log.urls == [Self.attackerActorURL, keyURL])

            // The Key URL still works as a key id for its real owner.
            try await post(app, try signedRequest(keyID: keyURL), expecting: .accepted)
            #expect(await fetcher.log.urls == [Self.attackerActorURL, keyURL, keyURL, Self.actorID])

            // Another attacker document naming the Key URL costs it no fetch.
            let attackAgain = try signedRequest(
                activity: activity, key: attackerKey, keyID: "\(secondAttackerURL)#main-key"
            )
            try await post(app, attackAgain, expecting: .unauthorized)
            #expect(
                await fetcher.log.urls
                    == [Self.attackerActorURL, keyURL, keyURL, Self.actorID, secondAttackerURL]
            )
        }
    }

    @Test("A key id that is not an http(s) URL is refused without touching the cache")
    func invalidKeyIDRejectedWithoutCacheAccess() async throws {
        let keyURL = "https://remote.example/keys/1"
        let fetcher = URLKeyedActorFetcher(documents: [
            keyURL: .keyDocument(id: keyURL, owner: Self.actorID, publicKeyPEM: TestSigning.publicKeyPEM),
            Self.actorID: Self.document(),
        ])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let cache = try mockCache(app)
            // Spelled to land on the actor's authority-negative key if scopes
            // shared a namespace.
            let poison = try signedRequest(keyID: "authority:\(Self.actorID)#main-key")
            try await post(app, poison, expecting: .unauthorized)
            #expect(await fetcher.log.urls.isEmpty)
            let document = try await cache.isNegative(url: Self.actorID, scope: .document)
            let authority = try await cache.isNegative(url: Self.actorID, scope: .authority)
            #expect(!document)
            #expect(!authority)

            // The actor still confirms itself as the owner of a Key document.
            try await post(app, try signedRequest(keyID: keyURL), expecting: .accepted)
            #expect(await fetcher.log.urls == [keyURL, Self.actorID])
        }
    }

    @Test("URLs carrying credentials are refused as key ids and as claimed ids")
    func credentialBearingURLsRefused() async throws {
        let withCredentials = "https://nonce@remote.example/actor"
        let fetcher = URLKeyedActorFetcher(documents: [
            Self.actorID: Self.document(),
            Self.oldURL: .actorDocument(id: withCredentials, publicKeyPEM: TestSigning.publicKeyPEM),
        ])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            try await post(app, try signedRequest(), expecting: .accepted)

            // As a key id: refused before any fetch or cache access.
            try await post(app, try signedRequest(keyID: "\(withCredentials)#main-key"), expecting: .unauthorized)
            #expect(await fetcher.log.urls == [Self.actorID])

            // As a claimed id: the alias document is read once, the cached
            // actor is not fetched again, and the alias is refused afterwards.
            let viaAlias = try signedRequest(keyID: "\(Self.oldURL)#main-key")
            try await post(app, viaAlias, expecting: .unauthorized)
            try await post(app, viaAlias, expecting: .unauthorized)
            #expect(await fetcher.log.urls == [Self.actorID, Self.oldURL])
        }
    }

    @Test("Documents naming each other as canonical are rejected without waiting on each other")
    func mutuallyClaimingDocumentsRejectedPromptly() async throws {
        let otherID = "https://remote.example/actor-b"
        let fetcher = URLKeyedActorFetcher(documents: [
            Self.actorID: .actorDocument(id: otherID, publicKeyPEM: TestSigning.publicKeyPEM),
            otherID: .actorDocument(id: Self.actorID, publicKeyPEM: TestSigning.publicKeyPEM),
        ])

        try await withApp(configure: { app in
            try await configure(fetcher: fetcher)(app)
            app.actorCachePolicy.fetchWaitTimeout = .seconds(10)
        }) { app in
            let first = try signedRequest()
            let second = try signedRequest(keyID: "\(otherID)#main-key")

            let clock = ContinuousClock()
            let elapsed = try await clock.measure {
                async let firstStatus = status(app, first)
                async let secondStatus = status(app, second)
                let statuses = try await [firstStatus, secondStatus]
                #expect(statuses == [.unauthorized, .unauthorized])
            }
            #expect(elapsed < .seconds(2))
        }
    }

    @Test("A Key document naming itself as owner is rejected without waiting on its own claim")
    func selfOwnedKeyDocumentRejectedImmediately() async throws {
        let keyURL = "https://remote.example/keys/self"
        let fetcher = URLKeyedActorFetcher(documents: [
            keyURL: .keyDocument(id: keyURL, owner: keyURL, publicKeyPEM: TestSigning.publicKeyPEM),
        ])

        try await withApp(configure: { app in
            try await configure(fetcher: fetcher)(app)
            app.actorCachePolicy.fetchWaitTimeout = .seconds(10)
        }) { app in
            let clock = ContinuousClock()
            let elapsed = try await clock.measure {
                try await post(app, try signedRequest(keyID: keyURL), expecting: .unauthorized)
            }
            #expect(elapsed < .seconds(2))
            #expect(await fetcher.log.urls == [keyURL])

            try await post(app, try signedRequest(keyID: keyURL), expecting: .unauthorized)
            #expect(await fetcher.log.urls == [keyURL])
        }
    }

    // MARK: - Key rotation

    @Test("A canonical URL that starts pointing at a new id is superseded by the alias")
    func changedCanonicalSupersedesDirectEntry() async throws {
        let newID = "https://remote.example/actor-new"
        let rotatedPEM = Self.rotatedKey.publicKey.pemRepresentation
        let original = URLKeyedActorFetcher(documents: [Self.actorID: Self.document()])
        let moved = URLKeyedActorFetcher(documents: [
            Self.actorID: .actorDocument(id: newID, publicKeyPEM: rotatedPEM),
            newID: .actorDocument(id: newID, inbox: TestSigning.testInboxURL, publicKeyPEM: rotatedPEM),
        ])

        try await withApp(configure: configure(fetcher: original)) { app in
            let cache = try mockCache(app)
            try await post(app, try signedRequest(), expecting: .accepted)
            await cache.expireRefetchHold(id: Self.actorID)
            app.actorFetcher = moved

            // The refresh follows the old URL to the new id; the entry stored
            // under the old URL no longer shadows the alias.
            try await post(app, try signedRequest(key: Self.rotatedKey), expecting: .accepted)
            #expect(await moved.log.urls == [Self.actorID, newID])
            #expect(await !cache.hasActor(id: Self.actorID))

            try await post(app, try signedRequest(key: Self.rotatedKey), expecting: .accepted)
            #expect(await moved.log.urls == [Self.actorID, newID])
        }
    }

    @Test("A signature that fails against a fresh entry does not re-fetch or wait")
    func invalidSignatureAgainstFreshEntryDoesNotRefetch() async throws {
        let fetcher = URLKeyedActorFetcher(documents: [Self.actorID: Self.document()])

        try await withApp(configure: { app in
            try await configure(fetcher: fetcher)(app)
            app.actorCachePolicy.fetchWaitTimeout = .seconds(10)
        }) { app in
            try await post(app, try signedRequest(), expecting: .accepted)

            let clock = ContinuousClock()
            let elapsed = try await clock.measure {
                try await post(app, try signedRequest(key: Self.rotatedKey), expecting: .unauthorized)
            }
            #expect(elapsed < .seconds(2))
            #expect(await fetcher.log.urls == [Self.actorID])
        }
    }

    @Test("Requests overlapping a key refresh wait for it instead of being rejected")
    func concurrentRequestsAwaitRefresh() async throws {
        let original = URLKeyedActorFetcher(documents: [Self.actorID: Self.document()])
        let rotated = GatedActorFetcher(documents: [
            Self.actorID: Self.document(publicKeyPEM: Self.rotatedKey.publicKey.pemRepresentation),
        ])

        try await withApp(configure: configure(fetcher: original)) { app in
            let cache = try mockCache(app)
            try await post(app, try signedRequest(), expecting: .accepted)
            await cache.expireRefetchHold(id: Self.actorID)
            app.actorFetcher = rotated

            let first = try signedRequest(key: Self.rotatedKey)
            let second = try signedRequest(key: Self.rotatedKey)
            async let firstStatus = status(app, first)
            async let secondStatus = status(app, second)

            // One request claims the refresh and is inside the fetch; give
            // the other time to lose the claim and start waiting.
            while await rotated.log.urls.isEmpty {
                try await Task.sleep(for: .milliseconds(5))
            }
            try await Task.sleep(for: .milliseconds(50))
            await rotated.gate.open()

            let statuses = try await [firstStatus, secondStatus]
            #expect(statuses == [.accepted, .accepted])
            #expect(await rotated.log.urls == [Self.actorID])
        }
    }

    @Test("A failed refresh settles the hold so waiting requests stop and later ones do not wait")
    func failedRefreshSettlesHold() async throws {
        let original = URLKeyedActorFetcher(documents: [Self.actorID: Self.document()])
        let unreachable = URLKeyedActorFetcher(documents: [:])

        try await withApp(configure: configure(fetcher: original)) { app in
            let cache = try mockCache(app)
            try await post(app, try signedRequest(), expecting: .accepted)
            await cache.expireRefetchHold(id: Self.actorID)
            app.actorFetcher = unreachable

            try await post(app, try signedRequest(key: Self.rotatedKey), expecting: .unauthorized)
            let state = try await cache.refetchState(id: Self.actorID)
            #expect(state == .settled)
            #expect(await unreachable.log.urls == [Self.actorID])

            // The old key still verifies from the cache; a wrong key neither
            // waits nor fetches.
            try await post(app, try signedRequest(), expecting: .accepted)
            try await post(app, try signedRequest(key: Self.rotatedKey), expecting: .unauthorized)
            #expect(await unreachable.log.urls == [Self.actorID])
        }
    }

    @Test("A rotated key is picked up by one re-fetch, then rate limited")
    func rotatedKeyTriggersOneRefetch() async throws {
        let original = URLKeyedActorFetcher(documents: [Self.actorID: Self.document()])
        let rotated = URLKeyedActorFetcher(documents: [
            Self.actorID: Self.document(publicKeyPEM: Self.rotatedKey.publicKey.pemRepresentation),
        ])
        let rotatedAgain = URLKeyedActorFetcher(documents: [
            Self.actorID: Self.document(publicKeyPEM: Self.rotatedAgainKey.publicKey.pemRepresentation),
        ])

        try await withApp(configure: configure(fetcher: original)) { app in
            let cache = try mockCache(app)
            try await post(app, try signedRequest(), expecting: .accepted)

            // The origin rotates its key after the hold from the first fetch lapsed.
            app.actorFetcher = rotated
            await cache.expireRefetchHold(id: Self.actorID)
            try await post(app, try signedRequest(key: Self.rotatedKey), expecting: .accepted)
            #expect(await rotated.log.urls == [Self.actorID])

            // The new key is cached; the old one no longer verifies and no
            // re-fetch is allowed within the interval.
            try await post(app, try signedRequest(key: Self.rotatedKey), expecting: .accepted)
            try await post(app, try signedRequest(), expecting: .unauthorized)
            #expect(await rotated.log.urls == [Self.actorID])

            app.actorFetcher = rotatedAgain
            try await post(app, try signedRequest(key: Self.rotatedAgainKey), expecting: .unauthorized)
            #expect(await rotatedAgain.log.urls.isEmpty)
        }
    }

    @Test("A refresh through a URL that now names another actor respects that actor's hold")
    func refreshDoesNotBypassTargetHold() async throws {
        let secondURL = "https://remote.example/actor-2"
        let targetID = "https://remote.example/actor-b"
        let targetPEM = Self.rotatedKey.publicKey.pemRepresentation
        let original = URLKeyedActorFetcher(documents: [
            Self.actorID: Self.document(),
            secondURL: .actorDocument(id: secondURL, publicKeyPEM: TestSigning.publicKeyPEM),
            targetID: .actorDocument(id: targetID, publicKeyPEM: targetPEM),
        ])
        let redirected = URLKeyedActorFetcher(documents: [
            Self.actorID: .actorDocument(id: targetID, publicKeyPEM: targetPEM),
            secondURL: .actorDocument(id: targetID, publicKeyPEM: targetPEM),
            targetID: .actorDocument(id: targetID, publicKeyPEM: targetPEM),
        ])

        try await withApp(configure: configure(fetcher: original)) { app in
            let cache = try mockCache(app)
            try await post(app, try signedRequest(), expecting: .accepted)
            try await post(app, try signedRequest(keyID: "\(secondURL)#main-key"), expecting: .accepted)
            try await post(
                app, try signedRequest(key: Self.rotatedKey, keyID: "\(targetID)#main-key"),
                expecting: .accepted
            )
            await cache.expireRefetchHold(id: Self.actorID)
            await cache.expireRefetchHold(id: secondURL)
            app.actorFetcher = redirected

            // Each URL's own hold lets its document be re-read, but the actor
            // it now names was fetched moments ago and is not fetched again.
            try await post(app, try signedRequest(key: Self.rotatedAgainKey), expecting: .unauthorized)
            try await post(
                app, try signedRequest(key: Self.rotatedAgainKey, keyID: "\(secondURL)#main-key"),
                expecting: .unauthorized
            )
            #expect(await redirected.log.urls == [Self.actorID, secondURL])

            // The redirect itself is recorded: the target's key now verifies
            // through the old URL without a fetch.
            try await post(app, try signedRequest(key: Self.rotatedKey), expecting: .accepted)
            #expect(await redirected.log.urls == [Self.actorID, secondURL])
        }
    }

    @Test("A failing alias of a cached actor refreshes the actor itself instead of spending its hold")
    func failingAliasRefreshesActorDirectly() async throws {
        let attackerKey = try _RSA.Signing.PrivateKey(keySize: .bits2048)
        let rotatedPEM = Self.rotatedKey.publicKey.pemRepresentation
        let original = URLKeyedActorFetcher(documents: [
            Self.actorID: Self.document(),
            Self.attackerActorURL: .actorDocument(
                id: Self.actorID, publicKeyPEM: attackerKey.publicKey.pemRepresentation
            ),
        ])
        let rotated = URLKeyedActorFetcher(documents: [
            Self.actorID: Self.document(publicKeyPEM: rotatedPEM),
        ])

        try await withApp(configure: configure(fetcher: original)) { app in
            let cache = try mockCache(app)
            try await post(app, try signedRequest(), expecting: .accepted)
            // Records the alias attacker → actor although the signature fails.
            let viaAttacker = try signedRequest(key: attackerKey, keyID: "\(Self.attackerActorURL)#main-key")
            try await post(app, viaAttacker, expecting: .unauthorized)
            #expect(await original.log.urls == [Self.actorID, Self.attackerActorURL])

            // The attacker URL now fails while the actor has rotated its key.
            await cache.expireRefetchHold(id: Self.actorID)
            app.actorFetcher = rotated
            try await post(app, viaAttacker, expecting: .unauthorized)
            #expect(await rotated.log.urls == [Self.attackerActorURL, Self.actorID])
            let state = try await cache.refetchState(id: Self.actorID)
            #expect(state == .settled)

            // The rotated key was picked up on that fetch.
            try await post(app, try signedRequest(key: Self.rotatedKey), expecting: .accepted)
            #expect(await rotated.log.urls == [Self.attackerActorURL, Self.actorID])
        }
    }

    @Test("A refresh whose chain ends at another actor gives the borrowed hold back")
    func refreshEndingElsewhereReleasesHold() async throws {
        let otherID = "https://evil.example/actor-c"
        let attackerKey = try _RSA.Signing.PrivateKey(keySize: .bits2048)
        let attackerPEM = attackerKey.publicKey.pemRepresentation
        let rotatedPEM = Self.rotatedKey.publicKey.pemRepresentation
        let original = URLKeyedActorFetcher(documents: [
            Self.actorID: Self.document(),
            Self.attackerActorURL: .actorDocument(id: Self.actorID, publicKeyPEM: attackerPEM),
        ])
        let redirected = URLKeyedActorFetcher(documents: [
            Self.actorID: Self.document(publicKeyPEM: rotatedPEM),
            Self.attackerActorURL: .actorDocument(id: otherID, publicKeyPEM: attackerPEM),
            otherID: .actorDocument(id: otherID, publicKeyPEM: attackerPEM),
        ])

        try await withApp(configure: configure(fetcher: original)) { app in
            let cache = try mockCache(app)
            try await post(app, try signedRequest(), expecting: .accepted)
            let viaAttacker = try signedRequest(key: Self.rotatedAgainKey, keyID: "\(Self.attackerActorURL)#main-key")
            try await post(app, viaAttacker, expecting: .unauthorized)

            // The attacker URL now names the attacker's own actor; the borrowed
            // hold on the victim is returned untouched.
            await cache.expireRefetchHold(id: Self.actorID)
            app.actorFetcher = redirected
            try await post(app, viaAttacker, expecting: .unauthorized)
            #expect(await redirected.log.urls == [Self.attackerActorURL, otherID])
            let state = try await cache.refetchState(id: Self.actorID)
            #expect(state == nil)

            // So the victim's rotated key is still refreshable right away.
            try await post(app, try signedRequest(key: Self.rotatedKey), expecting: .accepted)
            #expect(await redirected.log.urls == [Self.attackerActorURL, otherID, Self.actorID])
        }
    }

    @Test("A cached alias whose target is gone is re-read instead of being refused")
    func staleAliasIsReReadBeforeBeingRefused() async throws {
        let oldTarget = "https://remote.example/actor-b"
        let newTarget = "https://remote.example/actor-c"
        let oldPEM = Self.rotatedKey.publicKey.pemRepresentation
        let newPEM = Self.rotatedAgainKey.publicKey.pemRepresentation
        let before = URLKeyedActorFetcher(documents: [
            Self.oldURL: .actorDocument(id: oldTarget, publicKeyPEM: oldPEM),
            oldTarget: .actorDocument(id: oldTarget, publicKeyPEM: oldPEM),
        ])
        let after = URLKeyedActorFetcher(documents: [
            Self.oldURL: .actorDocument(id: newTarget, publicKeyPEM: newPEM),
            newTarget: .actorDocument(id: newTarget, publicKeyPEM: newPEM),
        ])

        try await withApp(configure: configure(fetcher: before)) { app in
            let cache = try mockCache(app)
            let viaOldKey = try signedRequest(key: Self.rotatedKey, keyID: "\(Self.oldURL)#main-key")
            try await post(app, viaOldKey, expecting: .accepted)

            // The old target is gone from the cache and unreachable; the
            // alias document now names a different actor.
            try await cache.evict(id: oldTarget)
            app.actorFetcher = after
            let viaNewKey = try signedRequest(key: Self.rotatedAgainKey, keyID: "\(Self.oldURL)#main-key")
            try await post(app, viaNewKey, expecting: .accepted)
            #expect(await after.log.urls == [oldTarget, Self.oldURL, newTarget])
            let refused = try await cache.isNegative(url: Self.oldURL, scope: .document)
            #expect(!refused)
        }
    }

    @Test("Requests waiting on a refresh that moved the key id URL follow the new mapping")
    func waitersFollowRefreshThatMovedTheActor() async throws {
        let newTarget = "https://remote.example/actor-c"
        let newPEM = Self.rotatedKey.publicKey.pemRepresentation
        let original = URLKeyedActorFetcher(documents: [Self.actorID: Self.document()])
        let moved = GatedActorFetcher(documents: [
            Self.actorID: .actorDocument(id: newTarget, publicKeyPEM: newPEM),
            newTarget: .actorDocument(id: newTarget, publicKeyPEM: newPEM),
        ])

        try await withApp(configure: configure(fetcher: original)) { app in
            let cache = try mockCache(app)
            try await post(app, try signedRequest(), expecting: .accepted)
            await cache.expireRefetchHold(id: Self.actorID)
            app.actorFetcher = moved

            let first = try signedRequest(key: Self.rotatedKey)
            let second = try signedRequest(key: Self.rotatedKey)
            async let firstStatus = status(app, first)
            async let secondStatus = status(app, second)

            while await moved.log.urls.isEmpty {
                try await Task.sleep(for: .milliseconds(5))
            }
            try await Task.sleep(for: .milliseconds(50))
            await moved.gate.open()

            let statuses = try await [firstStatus, secondStatus]
            #expect(statuses == [.accepted, .accepted])
            #expect(await moved.log.urls == [Self.actorID, newTarget])
        }
    }

    // MARK: - Eviction

    @Test("An Update of the signer's own actor evicts its cached entry")
    func selfUpdateEvictsCachedActor() async throws {
        let fetcher = URLKeyedActorFetcher(documents: [Self.actorID: Self.document()])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let cache = try mockCache(app)
            try await post(app, try signedRequest(), expecting: .accepted)

            let update = Data("""
                {
                    "@context": "https://www.w3.org/ns/activitystreams",
                    "id": "https://remote.example/activities/update-1",
                    "type": "Update",
                    "actor": "\(Self.actorID)",
                    "object": {
                        "id": "\(Self.actorID)",
                        "type": "Application",
                        "inbox": "\(TestSigning.testInboxURL)"
                    }
                }
                """.utf8)
            try await post(app, try signedRequest(json: update), expecting: .accepted)
            #expect(await !cache.hasActor(id: Self.actorID))

            try await post(app, try signedRequest(), expecting: .accepted)
            #expect(await fetcher.log.urls == [Self.actorID, Self.actorID])
        }
    }

    @Test("A Delete of the signer's own actor evicts its cached entry")
    func selfDeleteEvictsCachedActor() async throws {
        let fetcher = URLKeyedActorFetcher(documents: [Self.actorID: Self.document()])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let cache = try mockCache(app)
            try await post(app, try signedRequest(), expecting: .accepted)

            let delete = APActivity(
                context: .default,
                id: "https://remote.example/activities/delete-1",
                type: "Delete",
                actor: Self.actorID,
                object: .uri(Self.actorID),
                to: nil,
                cc: nil,
                published: nil
            )
            try await post(app, try signedRequest(activity: delete), expecting: .accepted)
            #expect(await !cache.hasActor(id: Self.actorID))
        }
    }

    @Test("An Update or Delete of another object keeps the cached entry")
    func unrelatedUpdateKeepsCachedActor() async throws {
        let fetcher = URLKeyedActorFetcher(documents: [Self.actorID: Self.document()])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let cache = try mockCache(app)
            try await post(app, try signedRequest(), expecting: .accepted)

            let update = APActivity(
                context: .default,
                id: "https://remote.example/activities/update-2",
                type: "Update",
                actor: Self.actorID,
                object: .uri("https://remote.example/notes/1"),
                to: nil,
                cc: nil,
                published: nil
            )
            try await post(app, try signedRequest(activity: update), expecting: .accepted)
            try await post(app, try signedRequest(activity: TestSigning.makeDeleteActivity()), expecting: .accepted)
            #expect(await cache.hasActor(id: Self.actorID))
            #expect(await fetcher.log.urls == [Self.actorID])
        }
    }

    // MARK: - Fetch claim

    @Test("Concurrent requests for one key id share a single fetch")
    func concurrentRequestsShareOneFetch() async throws {
        let fetcher = GatedActorFetcher(documents: [Self.actorID: Self.document()])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let cache = try mockCache(app)
            let first = try signedRequest()
            let second = try signedRequest()

            async let firstStatus = status(app, first)
            async let secondStatus = status(app, second)

            // Wait until one request is inside the fetch, then let it finish.
            while await fetcher.log.urls.isEmpty {
                try await Task.sleep(for: .milliseconds(5))
            }
            try await Task.sleep(for: .milliseconds(50))
            await fetcher.gate.open()

            let statuses = try await [firstStatus, secondStatus]
            #expect(statuses == [.accepted, .accepted])
            #expect(await fetcher.log.urls == [Self.actorID])
            #expect(await !cache.isFetchLocked(url: Self.actorID))
        }
    }

    @Test("A request that gets the claim after another holder finished uses that result")
    func claimAfterConcurrentFetchUsesCache() async throws {
        let fetcher = URLKeyedActorFetcher(documents: [Self.actorID: Self.document()])
        let stored = VerifiedActor(
            id: Self.actorID,
            inbox: TestSigning.testInboxURL,
            sharedInbox: nil,
            publicKeyPEM: TestSigning.publicKeyPEM
        )

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let cache = try mockCache(app)
            await cache.storeOnNextLockAcquire(stored)
            try await post(app, try signedRequest(), expecting: .accepted)
            #expect(await fetcher.log.urls.isEmpty)
        }
    }

    @Test("A request takes over a fetch claim whose holder died")
    func waiterTakesOverExpiredClaim() async throws {
        let fetcher = URLKeyedActorFetcher(documents: [Self.actorID: Self.document()])

        try await withApp(configure: configure(fetcher: fetcher)) { app in
            let cache = try mockCache(app)
            await cache.holdFetchLock(url: Self.actorID, for: 0.2)
            try await post(app, try signedRequest(), expecting: .accepted)
            #expect(await fetcher.log.urls == [Self.actorID])
        }
    }

    @Test("A request gives up when another holder's fetch does not finish in time")
    func waiterGivesUpAfterTimeout() async throws {
        let fetcher = URLKeyedActorFetcher(documents: [Self.actorID: Self.document()])

        try await withApp(configure: { app in
            try await configure(fetcher: fetcher)(app)
            app.actorCachePolicy.fetchWaitTimeout = .milliseconds(100)
        }) { app in
            let cache = try mockCache(app)
            await cache.holdFetchLock(url: Self.actorID, for: 60)
            try await post(app, try signedRequest(), expecting: .unauthorized)
            #expect(await fetcher.log.urls.isEmpty)
        }
    }

    private func status(_ app: Application, _ request: (headers: HTTPHeaders, body: ByteBuffer)) async throws -> HTTPStatus {
        var status = HTTPStatus.internalServerError
        try await app.testing().test(.POST, "inbox", headers: request.headers, body: request.body) {
            res async in
            status = res.status
        }
        return status
    }
}
