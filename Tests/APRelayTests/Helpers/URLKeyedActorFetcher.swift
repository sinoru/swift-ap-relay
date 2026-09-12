import APRelayCore
import Vapor
@testable import APRelay

/// Records every URL fetched through a ``URLKeyedActorFetcher``.
actor ActorFetchLog {
    private(set) var urls: [String] = []

    func record(_ url: String) {
        urls.append(url)
    }
}

/// Actor fetcher that serves a different document per URL, so tests can model
/// a keyID that resolves to one document while the id it claims resolves to
/// another. Unknown URLs fail like an unreachable host would.
struct URLKeyedActorFetcher: ActorFetcher {
    let documents: [String: RemoteActor]
    let log = ActorFetchLog()

    func fetchActor(url: String, client: Client) async throws -> RemoteActor {
        await log.record(url)
        guard let document = documents[url] else {
            throw Abort(.badGateway, reason: "Mock: no document at \(url)")
        }
        return document
    }
}

extension RemoteActor {
    /// An actor document advertising `publicKeyPEM`, owned by itself unless
    /// `owner` says otherwise (`.some(nil)` omits the field).
    static func actorDocument(
        id: String,
        inbox: String = "https://remote.example/inbox",
        publicKeyPEM: String,
        owner: String?? = nil
    ) -> RemoteActor {
        RemoteActor(
            id: id,
            type: "Application",
            inbox: inbox,
            endpoints: nil,
            publicKey: APPublicKey(
                id: "\(id)#main-key",
                owner: owner ?? id,
                publicKeyPem: publicKeyPEM
            )
        )
    }

    /// A standalone Key document, as served at a Misskey `/publickey` or
    /// GoToSocial `/main-key` URL.
    static func keyDocument(id: String, owner: String, publicKeyPEM: String) -> RemoteActor {
        RemoteActor(
            id: id,
            type: "Key",
            inbox: nil,
            endpoints: nil,
            publicKey: nil,
            owner: owner,
            publicKeyPem: publicKeyPEM
        )
    }
}
