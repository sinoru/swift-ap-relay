import APRelayCore
import _CryptoExtras
import Foundation
import Vapor

/// Protocol for fetching remote ActivityPub actor documents.
protocol ActorFetcher: Sendable {
    func fetchActor(url: String, client: Client) async throws -> RemoteActor
}

/// Default implementation that fetches actors via HTTP with signed GET requests.
struct HTTPActorFetcher: ActorFetcher {
    private let privateKey: _RSA.Signing.PrivateKey
    private let keyID: String
    private let userAgent: String
    private let httpSignature = HTTPSignature()

    /// Deadline for the fetch. The client has no read timeout of its own, so
    /// without this a server that accepts the connection and then stalls
    /// would hold the inbox request, and the fetch claim on the URL,
    /// until the claim expires.
    static let requestTimeoutSeconds: Int64 = 30

    init(privateKey: _RSA.Signing.PrivateKey, keyID: String, userAgent: String) {
        self.privateKey = privateKey
        self.keyID = keyID
        self.userAgent = userAgent
    }

    func fetchActor(url: String, client: Client) async throws -> RemoteActor {
        guard let parsedURL = URL(string: url), let host = parsedURL.host(), !host.isEmpty else {
            throw Abort(.badGateway, reason: "Invalid actor URL: \(url)")
        }
        let rawPath = parsedURL.path
        let path = (rawPath.isEmpty ? "/" : rawPath) + (parsedURL.query.map { "?\($0)" } ?? "")

        let signatureHeaders = try httpSignature.signGET(
            path: path,
            host: host,
            privateKey: privateKey,
            keyID: keyID
        )

        let uri = URI(string: url)
        let response = try await client.get(uri) { req in
            for (name, value) in signatureHeaders {
                req.headers.replaceOrAdd(name: name, value: value)
            }
            req.headers.replaceOrAdd(name: "User-Agent", value: userAgent)
            req.headers.replaceOrAdd(
                name: "Accept",
                value: "application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\", application/activity+json"
            )
            req.timeout = .seconds(Self.requestTimeoutSeconds)
        }
        guard response.status == .ok else {
            throw Abort(.badGateway, reason: "Failed to fetch remote actor: \(url)")
        }
        return try response.content.decode(RemoteActor.self, using: JSONDecoder())
    }
}

// MARK: - App Storage

private struct ActorFetcherKey: StorageKey {
    typealias Value = any ActorFetcher
}

extension Application {
    var actorFetcher: any ActorFetcher {
        get {
            guard let fetcher = storage[ActorFetcherKey.self] else {
                fatalError("ActorFetcher not configured. Call configure() first.")
            }
            return fetcher
        }
        set {
            storage[ActorFetcherKey.self] = newValue
        }
    }
}
