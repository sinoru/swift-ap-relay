import Foundation

/// An ``ActorCaching`` that answers actor and alias lookups from a
/// ``LocalActorCache`` before the shared cache behind it.
///
/// A URL known locally as an alias has no actor stored under it, so that
/// lookup is answered without the shared cache too.
///
/// Writes go to the shared cache first and are mirrored locally. The
/// coordination keys (fetch claims, negative entries, re-fetch holds) always
/// go to the shared cache, since they are only meaningful across replicas.
///
/// A local entry can be stale by up to its lifetime when another replica
/// changed the actor. Resolution asks about an actor's re-fetch hold when a
/// signature failed against its cached key, and before it fetches the actor,
/// so those calls drop the actor's local entries first: the failing request,
/// every request waiting on the refresh, and the fetch all read the shared
/// cache from then on.
struct LayeredActorCache: ActorCaching {
    let local: LocalActorCache
    let backing: any ActorCaching

    func actor(id: String) async throws -> VerifiedActor? {
        switch local.actor(id: id) {
        case .found(let actor):
            return actor
        case .absent:
            return nil
        case .unknown(let mark):
            guard let actor = try await backing.actor(id: id) else { return nil }
            local.fill(actor, since: mark)
            return actor
        }
    }

    func canonicalID(forURL url: String) async throws -> String? {
        switch local.canonicalID(forURL: url) {
        case .found(let id):
            return id
        case .absent:
            return nil
        case .unknown(let mark):
            guard let id = try await backing.canonicalID(forURL: url) else { return nil }
            local.fillAlias(url: url, canonicalID: id, since: mark)
            return id
        }
    }

    // Writes are mirrored locally only if nothing changed the entry here
    // while the shared write was in flight, so a store that resumes late
    // cannot undo an eviction or a newer store that already completed.

    func store(_ actor: VerifiedActor, ttlSeconds: Int) async throws {
        let mark = local.mark()
        try await backing.store(actor, ttlSeconds: ttlSeconds)
        local.store(actor, ttlSeconds: ttlSeconds, since: mark)
    }

    func recordAlias(url: String, canonicalID: String, ttlSeconds: Int) async throws {
        let mark = local.mark()
        try await backing.recordAlias(url: url, canonicalID: canonicalID, ttlSeconds: ttlSeconds)
        local.recordAlias(url: url, canonicalID: canonicalID, ttlSeconds: ttlSeconds, since: mark)
    }

    // Removals apply locally before the shared write, so lookups stop
    // answering from the old entry, and again after it, so a shared-cache
    // read that began in between cannot put the old entry back.

    func removeAlias(url: String) async throws {
        local.removeAlias(url: url)
        defer { local.removeAlias(url: url) }
        try await backing.removeAlias(url: url)
    }

    func evict(id: String) async throws {
        local.evict(id: id)
        defer { local.evict(id: id) }
        try await backing.evict(id: id)
    }

    func isNegative(url: String, scope: NegativeScope) async throws -> Bool {
        try await backing.isNegative(url: url, scope: scope)
    }

    func markNegative(url: String, scope: NegativeScope, ttlSeconds: Int) async throws {
        try await backing.markNegative(url: url, scope: scope, ttlSeconds: ttlSeconds)
    }

    func acquireFetchLock(url: String, token: String, ttlSeconds: Int) async throws -> Bool {
        try await backing.acquireFetchLock(url: url, token: token, ttlSeconds: ttlSeconds)
    }

    func releaseFetchLock(url: String, token: String) async throws {
        try await backing.releaseFetchLock(url: url, token: token)
    }

    func acquireRefetch(id: String, holdSeconds: Int) async throws -> Bool {
        local.invalidate(actor: id)
        return try await backing.acquireRefetch(id: id, holdSeconds: holdSeconds)
    }

    func settleRefetch(id: String, holdSeconds: Int) async throws {
        try await backing.settleRefetch(id: id, holdSeconds: holdSeconds)
    }

    func releaseRefetch(id: String) async throws {
        try await backing.releaseRefetch(id: id)
    }

    func refetchState(id: String) async throws -> RefetchState? {
        local.invalidate(actor: id)
        // Again once the state is read: a lookup that read the old actor
        // before the refresh stored its replacement must not survive the
        // check that lets waiting requests resolve again.
        defer { local.invalidate(actor: id) }
        return try await backing.refetchState(id: id)
    }
}
