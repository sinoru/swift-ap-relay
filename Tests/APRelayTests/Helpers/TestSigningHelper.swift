import APRelayCore
import Crypto
import _CryptoExtras
import Foundation
import Vapor

/// Utilities for generating signed HTTP requests in tests.
enum TestSigning {
    static let privateKey = try! _RSA.Signing.PrivateKey(keySize: .bits2048)
    static var publicKeyPEM: String { privateKey.publicKey.pemRepresentation }
    static let httpSignature = HTTPSignature()

    static let testActorID = "https://remote.example/actor"
    static let testActorDomain = "remote.example"
    static let testInboxURL = "https://remote.example/inbox"
    static let testSharedInboxURL = "https://remote.example/inbox"

    /// Signs a request body and returns the headers to include.
    static func signedHeaders(
        path: String = "/inbox",
        host: String = "localhost",
        body: Data,
        keyID: String = "\(testActorID)#main-key"
    ) throws -> [String: String] {
        try httpSignature.sign(
            method: "post",
            path: path,
            host: host,
            body: body,
            privateKey: privateKey,
            keyID: keyID
        )
    }

    /// Creates a Follow activity (Mastodon style: object = public collection).
    static func makeFollowActivity(
        id: String = "https://remote.example/activities/\(UUID().uuidString)",
        actor: String = testActorID,
        objectURI: String = "https://www.w3.org/ns/activitystreams#Public"
    ) -> APActivity {
        APActivity(
            context: .default,
            id: id,
            type: "Follow",
            actor: actor,
            object: .uri(objectURI),
            to: nil,
            cc: nil,
            published: nil
        )
    }

    /// Creates an Undo activity wrapping a Follow.
    static func makeUndoActivity(
        id: String = "https://remote.example/activities/\(UUID().uuidString)",
        actor: String = testActorID,
        followID: String = "https://remote.example/activities/follow-1",
        innerActor: String? = nil,
        objectAsURI: Bool = false
    ) -> APActivity {
        let object: APObject
        if objectAsURI {
            object = .uri(followID)
        } else {
            object = .activity(APActivity(
                context: nil,
                id: followID,
                type: "Follow",
                actor: innerActor ?? actor,
                object: .uri("https://www.w3.org/ns/activitystreams#Public"),
                to: nil,
                cc: nil,
                published: nil
            ))
        }
        return APActivity(
            context: .default,
            id: id,
            type: "Undo",
            actor: actor,
            object: object,
            to: nil,
            cc: nil,
            published: nil
        )
    }

    /// Creates raw Undo JSON whose nested Follow lacks an actor, so it
    /// decodes as a generic object rather than a full activity.
    static func makeUndoJSONWithBareFollowObject(
        actor: String = testActorID,
        followID: String = "https://remote.example/activities/follow-1"
    ) -> Data {
        Data("""
            {
                "@context": "https://www.w3.org/ns/activitystreams",
                "id": "https://remote.example/activities/\(UUID().uuidString)",
                "type": "Undo",
                "actor": "\(actor)",
                "object": {
                    "id": "\(followID)",
                    "type": "Follow"
                }
            }
            """.utf8)
    }

    /// Creates a Create activity.
    static func makeCreateActivity(
        id: String = "https://remote.example/activities/\(UUID().uuidString)",
        actor: String = testActorID
    ) -> APActivity {
        APActivity(
            context: .default,
            id: id,
            type: "Create",
            actor: actor,
            object: .uri("https://remote.example/notes/\(UUID().uuidString)"),
            to: .single("https://www.w3.org/ns/activitystreams#Public"),
            cc: nil,
            published: Date.ISO8601FormatStyle.apRelay.format(Date())
        )
    }

    /// Creates a Delete activity.
    static func makeDeleteActivity(
        id: String = "https://remote.example/activities/\(UUID().uuidString)",
        actor: String = testActorID
    ) -> APActivity {
        APActivity(
            context: .default,
            id: id,
            type: "Delete",
            actor: actor,
            object: .uri("https://remote.example/notes/\(UUID().uuidString)"),
            to: .single("https://www.w3.org/ns/activitystreams#Public"),
            cc: nil,
            published: nil
        )
    }

    /// Creates a Move activity (account migration).
    static func makeMoveActivity(
        id: String = "https://remote.example/activities/\(UUID().uuidString)",
        actor: String = testActorID
    ) -> APActivity {
        APActivity(
            context: .default,
            id: id,
            type: "Move",
            actor: actor,
            object: .uri(actor),
            to: .single("https://www.w3.org/ns/activitystreams#Public"),
            cc: nil,
            published: nil
        )
    }

    /// Creates an Add activity (e.g. pinning a post).
    static func makeAddActivity(
        id: String = "https://remote.example/activities/\(UUID().uuidString)",
        actor: String = testActorID
    ) -> APActivity {
        APActivity(
            context: .default,
            id: id,
            type: "Add",
            actor: actor,
            object: .uri("https://remote.example/notes/\(UUID().uuidString)"),
            to: .single("https://www.w3.org/ns/activitystreams#Public"),
            cc: nil,
            published: nil
        )
    }

    /// Creates a Remove activity (e.g. unpinning a post).
    static func makeRemoveActivity(
        id: String = "https://remote.example/activities/\(UUID().uuidString)",
        actor: String = testActorID
    ) -> APActivity {
        APActivity(
            context: .default,
            id: id,
            type: "Remove",
            actor: actor,
            object: .uri("https://remote.example/notes/\(UUID().uuidString)"),
            to: .single("https://www.w3.org/ns/activitystreams#Public"),
            cc: nil,
            published: nil
        )
    }

    /// Creates an Undo activity wrapping an Announce.
    static func makeUndoAnnounceActivity(
        id: String = "https://remote.example/activities/\(UUID().uuidString)",
        actor: String = testActorID,
        announceID: String = "https://remote.example/activities/announce-1"
    ) -> APActivity {
        APActivity(
            context: .default,
            id: id,
            type: "Undo",
            actor: actor,
            object: .activity(APActivity(
                context: nil,
                id: announceID,
                type: "Announce",
                actor: actor,
                object: .uri("https://remote.example/notes/1"),
                to: nil,
                cc: nil,
                published: nil
            )),
            to: nil,
            cc: nil,
            published: nil
        )
    }

    /// Creates an Accept activity wrapping the relay's outbound Follow.
    static func makeAcceptActivity(
        id: String = "https://remote.example/activities/\(UUID().uuidString)",
        actor: String = testActorID,
        followActivityID: String,
        relayActorURL: String
    ) -> APActivity {
        APActivity(
            context: .default,
            id: id,
            type: "Accept",
            actor: actor,
            object: .activity(APActivity(
                context: nil,
                id: followActivityID,
                type: "Follow",
                actor: relayActorURL,
                object: .uri(actor),
                to: nil,
                cc: nil,
                published: nil
            )),
            to: .single(relayActorURL),
            cc: nil,
            published: nil
        )
    }

    /// Creates a Reject activity wrapping the relay's outbound Follow.
    static func makeRejectActivity(
        id: String = "https://remote.example/activities/\(UUID().uuidString)",
        actor: String = testActorID,
        followActivityID: String,
        relayActorURL: String
    ) -> APActivity {
        APActivity(
            context: .default,
            id: id,
            type: "Reject",
            actor: actor,
            object: .activity(APActivity(
                context: nil,
                id: followActivityID,
                type: "Follow",
                actor: relayActorURL,
                object: .uri(actor),
                to: nil,
                cc: nil,
                published: nil
            )),
            to: .single(relayActorURL),
            cc: nil,
            published: nil
        )
    }

    /// Encodes an activity and returns the signed (headers, body) pair.
    static func signedRequest(
        activity: APActivity,
        path: String = "/inbox",
        host: String = "localhost"
    ) throws -> (headers: HTTPHeaders, body: ByteBuffer) {
        try signedRequest(
            json: JSONEncoder().encode(activity),
            path: path,
            host: host
        )
    }

    static func signedRequest(
        json: Data,
        path: String = "/inbox",
        host: String = "localhost"
    ) throws -> (headers: HTTPHeaders, body: ByteBuffer) {
        let sigHeaders = try signedHeaders(path: path, host: host, body: json)
        var headers = HTTPHeaders()
        for (name, value) in sigHeaders {
            headers.add(name: name, value: value)
        }
        return (headers, ByteBuffer(data: json))
    }
}
