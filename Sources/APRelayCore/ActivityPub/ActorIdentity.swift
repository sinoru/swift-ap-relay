import Foundation

/// Compares ActivityPub identifiers as resources rather than as strings.
package enum ActorIdentity {
    /// Returns whether two identifiers denote the same resource.
    ///
    /// Scheme and host are compared case-insensitively and an explicit default
    /// port equals an omitted one, so `https://Example.org:443/actor` matches
    /// `https://example.org/actor`. Path and query must match exactly, and the
    /// fragment is ignored. Identifiers that do not parse as URLs with a host
    /// fall back to plain string equality.
    package static func matches(_ lhs: String, _ rhs: String) -> Bool {
        guard
            let a = URL(string: lhs), let b = URL(string: rhs),
            let aHost = a.host(), let bHost = b.host()
        else {
            return lhs == rhs
        }
        return a.scheme?.lowercased() == b.scheme?.lowercased()
            && aHost.lowercased() == bHost.lowercased()
            && effectivePort(of: a) == effectivePort(of: b)
            && a.path(percentEncoded: true) == b.path(percentEncoded: true)
            && a.query == b.query
    }

    /// The single spelling under which a resource is fetched and cached:
    /// lowercase scheme and host, no explicit default port, no fragment.
    /// Path and query are kept as they are. Returns `nil` for anything that
    /// is not an http(s) URL with a host, or that carries credentials (no
    /// ActivityPub identifier does, and ``matches(_:_:)`` ignores them, so
    /// they would only serve as a free spelling variation), so it doubles
    /// as validation.
    package static func normalized(_ identifier: String) -> String? {
        guard var components = URLComponents(string: identifier),
            let scheme = components.scheme?.lowercased(), scheme == "http" || scheme == "https",
            let host = components.host, !host.isEmpty,
            components.user == nil, components.password == nil
        else {
            return nil
        }
        components.scheme = scheme
        components.host = host.lowercased()
        if components.port == (scheme == "https" ? 443 : 80) {
            components.port = nil
        }
        components.fragment = nil
        return components.string
    }

    private static func effectivePort(of url: URL) -> Int? {
        if let port = url.port {
            return port
        }
        switch url.scheme?.lowercased() {
        case "https": return 443
        case "http": return 80
        default: return nil
        }
    }
}
