import APRelayCore
import _CryptoExtras
import Foundation
import Testing
import Vapor
import VaporTesting
@testable import APRelay

/// The per-process actor cache in front of the shared one.
@Suite("Local Actor Cache Tests", .serialized)
struct LocalActorCacheTests {
    private static let actorID = TestSigning.testActorID
    private static let aliasURL = "https://remote.example/actor-old"
    private static let rotatedKey = try! _RSA.Signing.PrivateKey(keySize: .bits2048)

    private static func actor(id: String = actorID, publicKeyPEM: String = TestSigning.publicKeyPEM) -> VerifiedActor {
        VerifiedActor(id: id, inbox: TestSigning.testInboxURL, sharedInbox: nil, publicKeyPEM: publicKeyPEM)
    }

    private static func layered(ttlSeconds: Int = 60, capacity: Int = 100) -> (LayeredActorCache, MockActorCache) {
        let backing = MockActorCache()
        let cache = LayeredActorCache(
            local: LocalActorCache(ttlSeconds: ttlSeconds, capacity: capacity),
            backing: backing
        )
        return (cache, backing)
    }

    // MARK: - Lookups

    @Test("A cached actor and alias are answered locally, even after the shared entries change")
    func answersLocally() async throws {
        let (cache, backing) = Self.layered()
        try await cache.store(Self.actor(), ttlSeconds: 3600)
        try await cache.recordAlias(url: Self.aliasURL, canonicalID: Self.actorID, ttlSeconds: 3600)

        // Another replica evicts the shared entries.
        try await backing.evict(id: Self.actorID)
        try await backing.removeAlias(url: Self.aliasURL)

        let actor = try await cache.actor(id: Self.actorID)
        let alias = try await cache.canonicalID(forURL: Self.aliasURL)
        #expect(actor?.id == Self.actorID)
        #expect(alias == Self.actorID)
    }

    @Test("A miss reads the shared cache and keeps the answer")
    func missFillsFromShared() async throws {
        let (cache, backing) = Self.layered()
        try await backing.store(Self.actor(), ttlSeconds: 3600)

        let first = try await cache.actor(id: Self.actorID)
        try await backing.evict(id: Self.actorID)
        let second = try await cache.actor(id: Self.actorID)

        #expect(first?.id == Self.actorID)
        #expect(second?.id == Self.actorID)
    }

    @Test("Local entries lapse after their lifetime")
    func entriesExpire() async throws {
        let (cache, backing) = Self.layered(ttlSeconds: 0)
        try await cache.store(Self.actor(), ttlSeconds: 3600)
        try await backing.evict(id: Self.actorID)

        let actor = try await cache.actor(id: Self.actorID)
        #expect(actor == nil)
    }

    @Test("Eviction and alias removal on this replica take effect locally at once")
    func localWritesApply() async throws {
        let (cache, _) = Self.layered()
        try await cache.store(Self.actor(), ttlSeconds: 3600)
        try await cache.recordAlias(url: Self.aliasURL, canonicalID: Self.actorID, ttlSeconds: 3600)

        try await cache.evict(id: Self.actorID)
        try await cache.removeAlias(url: Self.aliasURL)

        let actor = try await cache.actor(id: Self.actorID)
        let alias = try await cache.canonicalID(forURL: Self.aliasURL)
        #expect(actor == nil)
        #expect(alias == nil)
    }

    // MARK: - Invalidation

    @Test("Asking about a re-fetch hold drops the actor's local entries, however they are spelled")
    func refetchSignalsInvalidate() async throws {
        let (cache, backing) = Self.layered()
        let spelledID = "https://REMOTE.example:443/actor"
        try await cache.store(Self.actor(id: spelledID), ttlSeconds: 3600)
        try await cache.recordAlias(url: Self.aliasURL, canonicalID: spelledID, ttlSeconds: 3600)
        try await backing.evict(id: spelledID)
        try await backing.removeAlias(url: Self.aliasURL)

        _ = try await cache.refetchState(id: Self.actorID)

        let actor = try await cache.actor(id: spelledID)
        let alias = try await cache.canonicalID(forURL: Self.aliasURL)
        #expect(actor == nil)
        #expect(alias == nil)
    }

    @Test("Claiming a re-fetch drops the actor's local entry")
    func acquireRefetchInvalidates() async throws {
        let (cache, backing) = Self.layered()
        try await cache.store(Self.actor(), ttlSeconds: 3600)
        try await backing.evict(id: Self.actorID)

        _ = try await cache.acquireRefetch(id: Self.actorID, holdSeconds: 60)

        let actor = try await cache.actor(id: Self.actorID)
        #expect(actor == nil)
    }

    @Test("Invalidating an actor leaves other actors' entries in place")
    func invalidationIsScopedToTheActor() {
        let local = LocalActorCache(ttlSeconds: 60, capacity: 100)
        let otherID = "https://other.example/actor"
        local.store(Self.actor(), since: local.mark())
        local.store(Self.actor(id: otherID), since: local.mark())
        local.recordAlias(url: "https://other.example/actor-old", canonicalID: otherID, since: local.mark())

        local.invalidate(actor: Self.actorID)

        #expect(Self.cachedActor(local, id: Self.actorID) == nil)
        #expect(Self.cachedActor(local, id: otherID)?.id == otherID)
        #expect(Self.cachedAlias(local, url: "https://other.example/actor-old") == otherID)
    }

    @Test("The cache stays within its capacity, dropping the entries closest to expiry")
    func capacityIsBounded() async throws {
        let local = LocalActorCache(ttlSeconds: 60, capacity: 10)
        for index in 0...10 {
            local.store(Self.actor(id: "https://remote.example/users/\(index)"), since: local.mark())
        }

        #expect(Self.cachedActor(local, id: "https://remote.example/users/0") == nil)
        #expect(Self.cachedActor(local, id: "https://remote.example/users/10") != nil)
    }

    // MARK: - Fills

    @Test("A shared-cache read that raced an eviction does not put the evicted actor back")
    func fillAfterEvictionIsDropped() throws {
        let local = LocalActorCache(ttlSeconds: 60, capacity: 100)
        let mark = try #require(Self.fillMark(local.actor(id: Self.actorID)))

        local.evict(id: Self.actorID)
        local.fill(Self.actor(), since: mark)

        #expect(Self.cachedActor(local, id: Self.actorID) == nil)
    }

    @Test("A shared-cache read that raced a store does not overwrite the stored actor")
    func fillAfterStoreIsDropped() throws {
        let local = LocalActorCache(ttlSeconds: 60, capacity: 100)
        let rotatedPEM = Self.rotatedKey.publicKey.pemRepresentation
        let mark = try #require(Self.fillMark(local.actor(id: Self.actorID)))

        local.store(Self.actor(publicKeyPEM: rotatedPEM), since: local.mark())
        local.fill(Self.actor(), since: mark)

        #expect(Self.cachedActor(local, id: Self.actorID)?.publicKeyPEM == rotatedPEM)
    }

    @Test("A store mirrored after an eviction that completed during its shared write does not restore the actor")
    func storeAfterEvictionIsDropped() {
        let local = LocalActorCache(ttlSeconds: 60, capacity: 100)
        let mark = local.mark()

        local.evict(id: Self.actorID)
        local.store(Self.actor(), since: mark)

        #expect(Self.cachedActor(local, id: Self.actorID) == nil)
    }

    @Test("A store mirrored after a newer store does not overwrite it, and still drops older reads")
    func lateStoreKeepsNewerStore() throws {
        let local = LocalActorCache(ttlSeconds: 60, capacity: 100)
        let rotatedPEM = Self.rotatedKey.publicKey.pemRepresentation
        let staleMark = local.mark()
        let readMark = try #require(Self.fillMark(local.actor(id: Self.actorID)))

        local.store(Self.actor(publicKeyPEM: rotatedPEM), since: local.mark())
        local.store(Self.actor(), since: staleMark)
        #expect(Self.cachedActor(local, id: Self.actorID)?.publicKeyPEM == rotatedPEM)

        local.evict(id: Self.actorID)
        local.fill(Self.actor(), since: readMark)
        #expect(Self.cachedActor(local, id: Self.actorID) == nil)
    }

    @Test("An alias mirrored after its removal completed during the shared write is not restored")
    func aliasAfterRemovalIsDropped() {
        let local = LocalActorCache(ttlSeconds: 60, capacity: 100)
        let mark = local.mark()

        local.removeAlias(url: Self.aliasURL)
        local.recordAlias(url: Self.aliasURL, canonicalID: Self.actorID, since: mark)

        #expect(Self.cachedAlias(local, url: Self.aliasURL) == nil)
    }

    @Test("An actor filled while the re-fetch state is being read does not survive the read")
    func fillDuringRefetchStateIsDropped() async throws {
        let (cache, backing) = Self.layered()
        try await backing.store(Self.actor(), ttlSeconds: 3600)
        await backing.runOnNextRefetchState {
            _ = try? await cache.actor(id: Self.actorID)
        }

        _ = try await cache.refetchState(id: Self.actorID)
        // The refresh stored the replacement once the state was read.
        let rotatedPEM = Self.rotatedKey.publicKey.pemRepresentation
        try await backing.store(Self.actor(publicKeyPEM: rotatedPEM), ttlSeconds: 3600)

        let actor = try await cache.actor(id: Self.actorID)
        #expect(actor?.publicKeyPEM == rotatedPEM)
    }

    @Test("A shared-cache read that raced an invalidation of its actor is dropped")
    func fillAfterInvalidationIsDropped() throws {
        let local = LocalActorCache(ttlSeconds: 60, capacity: 100)
        let mark = try #require(Self.fillMark(local.canonicalID(forURL: Self.aliasURL)))

        local.invalidate(actor: "https://REMOTE.example/actor")
        local.fillAlias(url: Self.aliasURL, canonicalID: Self.actorID, since: mark)

        #expect(Self.cachedAlias(local, url: Self.aliasURL) == nil)
    }

    @Test("A read that began before changes the cache no longer records is dropped")
    func fillOlderThanRecordedChangesIsDropped() throws {
        let local = LocalActorCache(ttlSeconds: 60, capacity: 4)
        let mark = try #require(Self.fillMark(local.actor(id: Self.actorID)))

        for index in 0...4 {
            local.evict(id: "https://remote.example/users/\(index)")
        }
        local.fill(Self.actor(), since: mark)

        #expect(Self.cachedActor(local, id: Self.actorID) == nil)
    }

    @Test("A URL known locally as an alias is not looked up as an actor in the shared cache")
    func aliasSkipsSharedActorLookup() async throws {
        let (cache, backing) = Self.layered()
        try await cache.recordAlias(url: Self.aliasURL, canonicalID: Self.actorID, ttlSeconds: 3600)
        // Only the shared cache has an actor under the alias URL.
        try await backing.store(Self.actor(id: Self.aliasURL), ttlSeconds: 3600)

        let actor = try await cache.actor(id: Self.aliasURL)
        #expect(actor == nil)
    }

    private static func cachedActor(_ local: LocalActorCache, id: String) -> VerifiedActor? {
        guard case .found(let actor) = local.actor(id: id) else { return nil }
        return actor
    }

    private static func cachedAlias(_ local: LocalActorCache, url: String) -> String? {
        guard case .found(let id) = local.canonicalID(forURL: url) else { return nil }
        return id
    }

    private static func fillMark<Value>(_ lookup: LocalActorCache.Lookup<Value>) -> LocalActorCache.Mark? {
        guard case .unknown(let mark) = lookup else { return nil }
        return mark
    }

    // MARK: - Signature verification

    @Test("A key rotated and re-fetched by another replica verifies here despite a stale local entry")
    func staleLocalEntryDoesNotRejectRotatedKey() async throws {
        let original = URLKeyedActorFetcher(documents: [
            Self.actorID: .actorDocument(id: Self.actorID, inbox: TestSigning.testInboxURL, publicKeyPEM: TestSigning.publicKeyPEM),
        ])
        let (cache, backing) = Self.layered()

        try await withApp(configure: { app in
            try await testConfigure(app)
            app.actorCacheOverride = cache
            app.actorFetcher = original
            app.actorCachePolicy.fetchPollInterval = .milliseconds(10)
        }) { app in
            try await Self.post(app, key: TestSigning.privateKey, expecting: .accepted)
            #expect(await original.log.urls == [Self.actorID])

            // Another replica re-fetched the actor after its key rotated.
            let rotatedPEM = Self.rotatedKey.publicKey.pemRepresentation
            try await backing.store(Self.actor(publicKeyPEM: rotatedPEM), ttlSeconds: 3600)
            try await backing.settleRefetch(id: Self.actorID, holdSeconds: 300)

            try await Self.post(app, key: Self.rotatedKey, expecting: .accepted)
            #expect(await original.log.urls == [Self.actorID])
        }
    }

    private static func post(
        _ app: Application,
        key: _RSA.Signing.PrivateKey,
        expecting status: HTTPStatus
    ) async throws {
        let json = try JSONEncoder().encode(TestSigning.makeFollowActivity())
        let sigHeaders = try HTTPSignature().sign(
            method: "post",
            path: "/inbox",
            host: "localhost",
            body: json,
            privateKey: key,
            keyID: "\(actorID)#main-key"
        )
        var headers = HTTPHeaders()
        for (name, value) in sigHeaders {
            headers.add(name: name, value: value)
        }
        try await app.testing().test(.POST, "inbox", headers: headers, body: ByteBuffer(data: json)) { res async in
            #expect(res.status == status)
        }
    }
}
