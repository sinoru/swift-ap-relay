import Vapor

/// Shared cache of verified remote actors and the coordination keys around
/// fetching them.
///
/// Implementations include ``RedisActorCache`` for production and a mock
/// actor for tests. Entries are keyed by the actor's canonical id; a key id
/// URL that resolves to a different id (an old URL, a standalone Key
/// document) is recorded as an alias so the next request skips the chain of
/// fetches that resolved it.
protocol ActorCaching: Sendable {
    /// The verified actor stored under `id`, if any.
    func actor(id: String) async throws -> VerifiedActor?

    /// The canonical actor id an alias URL resolved to, if recorded.
    func canonicalID(forURL url: String) async throws -> String?

    /// Stores a freshly fetched actor under its id.
    func store(_ actor: VerifiedActor, ttlSeconds: Int) async throws

    /// Records that the document at `url` names the actor `canonicalID`.
    /// Does not touch the actor entry itself, so an alias never extends its
    /// life.
    func recordAlias(url: String, canonicalID: String, ttlSeconds: Int) async throws

    /// Forgets what `url` was last seen to name.
    func removeAlias(url: String) async throws

    /// Drops the actor stored under `id`. Aliases pointing at it stay and
    /// resolve again through a fetch.
    func evict(id: String) async throws

    /// Whether `url` recently failed in the given way.
    func isNegative(url: String, scope: NegativeScope) async throws -> Bool

    /// Remembers that `url` failed in the given way.
    func markNegative(url: String, scope: NegativeScope, ttlSeconds: Int) async throws

    /// Claims the right to fetch `url` on behalf of every replica. Returns
    /// `false` while another holder's claim is live.
    func acquireFetchLock(url: String, token: String, ttlSeconds: Int) async throws -> Bool

    /// Releases the fetch claim on `url` if it is still held under `token`.
    func releaseFetchLock(url: String, token: String) async throws

    /// Claims the single re-fetch allowed for `id` per `holdSeconds`, leaving
    /// the hold in the `refreshing` state. Returns `false` when one was
    /// claimed, or the actor fetched, more recently.
    func acquireRefetch(id: String, holdSeconds: Int) async throws -> Bool

    /// Records that a fetch of the actor `id` just finished, successfully or
    /// not: the hold stays for `holdSeconds` in the `settled` state, so a
    /// signature failure within it does not trigger another fetch and
    /// requests waiting on a refresh stop waiting.
    func settleRefetch(id: String, holdSeconds: Int) async throws

    /// Gives back a claimed re-fetch of `id` that did not reach the actor,
    /// so the next request may claim it. A settled hold is left alone.
    func releaseRefetch(id: String) async throws

    /// The state of the re-fetch hold on `id`, or `nil` when none is held.
    func refetchState(id: String) async throws -> RefetchState?
}

/// What a negative entry says about a URL.
///
/// A key id URL may legitimately lead somewhere else (a Key document to its
/// owner, an old URL to the canonical id), but the same URL must not be
/// accepted when another document names it as its authority. The two
/// verdicts are cached apart, so a document that claims a legitimate Key or
/// alias URL as its id cannot make that URL's own signatures fail.
enum NegativeScope: Sendable {
    /// The URL yields no usable document in any role: the fetch failed, or
    /// the document identifies itself but carries no valid key.
    case document
    /// The URL cannot vouch for itself as a canonical actor: it serves a Key
    /// document or claims another id. It may still resolve as a key id URL.
    case authority
}

/// What the re-fetch hold on an actor currently means.
enum RefetchState: String, Sendable {
    /// A request claimed the re-fetch and is fetching the actor.
    case refreshing
    /// The actor was fetched recently; no further fetch until the hold lapses.
    case settled
}

/// Lifetimes for the actor cache and the fetch path in front of it.
struct ActorCachePolicy: Sendable {
    /// How long a verified actor stays cached. Key rotation is handled by the
    /// re-fetch on signature failure and self-Update/Delete evict explicitly,
    /// so the lifetime only bounds how long a changed inbox goes unnoticed.
    var actorTTLSeconds = 6 * 3600

    /// How long a URL that failed to yield a usable document is refused
    /// without another fetch. Mastodon cools off failed key fetches for five
    /// minutes (`SignatureVerification::STOPLIGHT_COOL_OFF_TIME`).
    var negativeTTLSeconds = 5 * 60

    /// Minimum spacing between fetches of one actor triggered by a signature
    /// that does not verify against its cached key. Bounds how often a stream
    /// of invalid signatures can make the relay hit an actor's origin.
    var refetchIntervalSeconds = 5 * 60

    /// Lifetime of the fetch claim that merges concurrent fetches of one URL.
    /// Resolving a URL makes at most two requests, so a live holder finishes
    /// within two request timeouts; a holder that died frees the URL by then.
    var fetchLockTTLSeconds = 2 * Int(HTTPActorFetcher.requestTimeoutSeconds) + 10

    /// How long a request waits for another holder's fetch before giving up.
    var fetchWaitTimeout: Duration = .seconds(30)

    /// How often a waiting request re-checks the cache.
    var fetchPollInterval: Duration = .milliseconds(100)
}

// MARK: - App Storage

private struct ActorCacheOverrideKey: StorageKey {
    typealias Value = any ActorCaching
}

private struct ActorCachePolicyKey: StorageKey {
    typealias Value = ActorCachePolicy
}

extension Application {
    /// The actor cache.
    ///
    /// In production this returns a ``RedisActorCache`` backed by `app.redis`.
    /// In tests, set `actorCacheOverride` to inject a mock.
    var actorCache: any ActorCaching {
        if let override = storage[ActorCacheOverrideKey.self] {
            return override
        }
        return RedisActorCache(redis: self.redis)
    }

    /// Override the actor cache (used by tests to inject a mock).
    var actorCacheOverride: (any ActorCaching)? {
        get { storage[ActorCacheOverrideKey.self] }
        set { storage[ActorCacheOverrideKey.self] = newValue }
    }

    /// Lifetimes used by the actor cache; defaults unless a test shortens them.
    var actorCachePolicy: ActorCachePolicy {
        get { storage[ActorCachePolicyKey.self] ?? ActorCachePolicy() }
        set { storage[ActorCachePolicyKey.self] = newValue }
    }
}
