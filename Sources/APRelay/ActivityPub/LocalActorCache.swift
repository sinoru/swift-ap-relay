import APRelayCore
import Foundation
import SynchronizationKit

/// A short-lived, per-process copy of recently used actor cache entries.
///
/// Redis stays the source of truth: this only answers repeated lookups
/// without a round trip. Entries live for a short time, so a change made by
/// another replica is seen here within that time, and every entry for an
/// actor is dropped as soon as a signature fails against it (see
/// ``LayeredActorCache``).
///
/// What a shared-cache read found, or a shared-cache write made, is kept
/// only when the entry did not change on this process after the read or
/// write began, so one that raced a write or an invalidation cannot put the
/// old entry back.
final class LocalActorCache: Sendable {
    /// What this process knows about a key.
    enum Lookup<Value: Sendable>: Sendable {
        /// The entry is cached here.
        case found(Value)
        /// This process knows there is no entry, without asking.
        case absent
        /// Not known here; a read of the shared cache may fill it from `mark`.
        case unknown(Mark)
    }

    /// A point in this cache's history where a shared-cache read or write
    /// began.
    struct Mark: Sendable {
        fileprivate let generation: UInt64
    }

    private enum Key: Hashable, Sendable {
        case actor(String)
        case alias(String)
    }

    /// Something that makes an in-flight shared-cache read stale.
    private enum Change: Hashable, Sendable {
        /// A write to one entry.
        case key(Key)
        /// An invalidation of every entry for a normalized actor id.
        case actor(String)
    }

    private struct Entry<Value: Sendable>: Sendable {
        let value: Value
        let expiresAt: ContinuousClock.Instant
        /// The normalized actor ids the entry is dropped with.
        let actorIDs: [String]
    }

    private struct State: Sendable {
        var actors: [String: Entry<VerifiedActor>] = [:]
        var aliases: [String: Entry<String>] = [:]
        /// The keys of every entry for each normalized actor id.
        var members: [String: Set<Key>] = [:]

        var generation: UInt64 = 0
        /// The generation of the latest change to each key or actor.
        var changes: [Change: UInt64] = [:]
        /// Changes at or below this generation are no longer recorded.
        var forgottenGeneration: UInt64 = 0

        var count: Int { actors.count + aliases.count }

        mutating func record(_ change: Change, capacity: Int) {
            generation += 1
            changes[change] = generation
            guard changes.count > capacity else { return }
            // Forget the older half; reads that began before it are refused.
            let generations = changes.values.sorted()
            let cutoff = generations[generations.count / 2]
            changes = changes.filter { $0.value > cutoff }
            forgottenGeneration = max(forgottenGeneration, cutoff)
        }

        func hasChanged(_ key: Key, actorIDs: [String], since mark: Mark) -> Bool {
            guard mark.generation >= forgottenGeneration else { return true }
            let candidates = [Change.key(key)] + actorIDs.map(Change.actor)
            return candidates.contains { (changes[$0] ?? 0) > mark.generation }
        }

        mutating func insertActor(_ entry: Entry<VerifiedActor>) {
            let key = Key.actor(entry.value.id)
            remove(key)
            actors[entry.value.id] = entry
            addMember(key, to: entry.actorIDs)
        }

        mutating func insertAlias(url: String, _ entry: Entry<String>) {
            let key = Key.alias(url)
            remove(key)
            aliases[url] = entry
            addMember(key, to: entry.actorIDs)
        }

        mutating func remove(_ key: Key) {
            let actorIDs: [String]
            switch key {
            case .actor(let id):
                guard let entry = actors.removeValue(forKey: id) else { return }
                actorIDs = entry.actorIDs
            case .alias(let url):
                guard let entry = aliases.removeValue(forKey: url) else { return }
                actorIDs = entry.actorIDs
            }
            for actorID in actorIDs {
                members[actorID]?.remove(key)
                if members[actorID]?.isEmpty == true {
                    members[actorID] = nil
                }
            }
        }

        private mutating func addMember(_ key: Key, to actorIDs: [String]) {
            for actorID in actorIDs {
                members[actorID, default: []].insert(key)
            }
        }

        /// Keeps the cache within its capacity: expired entries go first,
        /// then the tenth of the entries closest to expiry, so a full cache
        /// is not rescanned on every insert.
        mutating func trim(capacity: Int, now: ContinuousClock.Instant) {
            guard count > capacity else { return }
            var expiries = actors.map { (key: Key.actor($0.key), expiresAt: $0.value.expiresAt) }
                + aliases.map { (key: Key.alias($0.key), expiresAt: $0.value.expiresAt) }
            for expired in expiries where expired.expiresAt <= now {
                remove(expired.key)
            }

            let excess = count - capacity
            guard excess > 0 else { return }
            expiries = expiries.filter { $0.expiresAt > now }.sorted { $0.expiresAt < $1.expiresAt }
            for dropped in expiries.prefix(max(excess, capacity / 10)) {
                remove(dropped.key)
            }
        }
    }

    private let ttl: Duration
    private let capacity: Int
    private let state = RWLock(State())

    init(ttlSeconds: Int, capacity: Int) {
        self.ttl = .seconds(ttlSeconds)
        self.capacity = capacity
    }

    /// The actor stored under `id`. An alias recorded for `id` means no
    /// actor is stored under it, since resolution drops the one before
    /// recording the other.
    func actor(id: String) -> Lookup<VerifiedActor> {
        let now = ContinuousClock.now
        return state.withReadLock { state in
            if let entry = state.actors[id], entry.expiresAt > now {
                return .found(entry.value)
            }
            if let entry = state.aliases[id], entry.expiresAt > now {
                return .absent
            }
            return .unknown(Mark(generation: state.generation))
        }
    }

    /// The canonical actor id recorded for the alias `url`.
    func canonicalID(forURL url: String) -> Lookup<String> {
        let now = ContinuousClock.now
        return state.withReadLock { state in
            if let entry = state.aliases[url], entry.expiresAt > now {
                return .found(entry.value)
            }
            return .unknown(Mark(generation: state.generation))
        }
    }

    /// The current point in this cache's history, taken before a write to
    /// the shared cache that is mirrored here afterwards.
    func mark() -> Mark {
        state.withReadLock { Mark(generation: $0.generation) }
    }

    /// Mirrors `actor`, written to the shared cache after `mark`, for the
    /// local lifetime or for `ttlSeconds` if that is shorter. The write
    /// always counts as a change, so reads that began before it are
    /// dropped, but the entry is kept only if nothing changed it here since
    /// `mark`: a later eviction or store must not be undone.
    func store(_ actor: VerifiedActor, ttlSeconds: Int? = nil, since mark: Mark) {
        let entry = Entry(value: actor, expiresAt: expiry(ttlSeconds: ttlSeconds), actorIDs: [Self.actorID(actor.id)])
        state.withWriteLock { state in
            let key = Key.actor(actor.id)
            let isCurrent = !state.hasChanged(key, actorIDs: entry.actorIDs, since: mark)
            state.record(.key(key), capacity: capacity)
            guard isCurrent else { return }
            state.insertActor(entry)
            state.trim(capacity: capacity, now: .now)
        }
    }

    /// Keeps `actor`, read from the shared cache, unless its entry changed
    /// here since `mark`.
    func fill(_ actor: VerifiedActor, since mark: Mark) {
        let entry = Entry(value: actor, expiresAt: expiry(ttlSeconds: nil), actorIDs: [Self.actorID(actor.id)])
        state.withWriteLock { state in
            guard !state.hasChanged(.actor(actor.id), actorIDs: entry.actorIDs, since: mark) else { return }
            state.insertActor(entry)
            state.trim(capacity: capacity, now: .now)
        }
    }

    /// Mirrors an alias written to the shared cache after `mark`, under the
    /// same rule as ``store(_:ttlSeconds:since:)``.
    func recordAlias(url: String, canonicalID: String, ttlSeconds: Int? = nil, since mark: Mark) {
        let entry = aliasEntry(url: url, canonicalID: canonicalID, ttlSeconds: ttlSeconds)
        state.withWriteLock { state in
            let key = Key.alias(url)
            let isCurrent = !state.hasChanged(key, actorIDs: entry.actorIDs, since: mark)
            state.record(.key(key), capacity: capacity)
            guard isCurrent else { return }
            state.insertAlias(url: url, entry)
            state.trim(capacity: capacity, now: .now)
        }
    }

    /// Keeps an alias read from the shared cache, unless its entry changed
    /// here since `mark`.
    func fillAlias(url: String, canonicalID: String, since mark: Mark) {
        let entry = aliasEntry(url: url, canonicalID: canonicalID, ttlSeconds: nil)
        state.withWriteLock { state in
            guard !state.hasChanged(.alias(url), actorIDs: entry.actorIDs, since: mark) else { return }
            state.insertAlias(url: url, entry)
            state.trim(capacity: capacity, now: .now)
        }
    }

    func removeAlias(url: String) {
        state.withWriteLock { state in
            state.record(.key(.alias(url)), capacity: capacity)
            state.remove(.alias(url))
        }
    }

    func evict(id: String) {
        state.withWriteLock { state in
            state.record(.key(.actor(id)), capacity: capacity)
            state.remove(.actor(id))
        }
    }

    /// Drops every entry for the actor `id` names, however it is spelled:
    /// the actor itself and each alias that points at it or is spelled as it.
    func invalidate(actor id: String) {
        let actorID = Self.actorID(id)
        state.withWriteLock { state in
            state.record(.actor(actorID), capacity: capacity)
            for key in state.members[actorID] ?? [] {
                state.remove(key)
            }
        }
    }

    private func aliasEntry(url: String, canonicalID: String, ttlSeconds: Int?) -> Entry<String> {
        let actorIDs = [Self.actorID(url), Self.actorID(canonicalID)]
        return Entry(
            value: canonicalID,
            expiresAt: expiry(ttlSeconds: ttlSeconds),
            actorIDs: actorIDs[0] == actorIDs[1] ? [actorIDs[0]] : actorIDs
        )
    }

    private func expiry(ttlSeconds: Int?) -> ContinuousClock.Instant {
        ContinuousClock.now + min(ttl, ttlSeconds.map { .seconds($0) } ?? ttl)
    }

    /// The spelling every entry for one actor is grouped under.
    private static func actorID(_ id: String) -> String {
        ActorIdentity.normalized(id) ?? id
    }
}
