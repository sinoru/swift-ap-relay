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
