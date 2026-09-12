import Foundation
import Testing
@testable import APRelayCore

@Suite("ActorIdentity Tests")
struct ActorIdentityTests {
    @Test(
        "Equivalent identifiers match",
        arguments: [
            ("https://example.org/actor", "https://example.org/actor"),
            ("https://Example.ORG/actor", "https://example.org/actor"),
            ("HTTPS://example.org/actor", "https://example.org/actor"),
            ("https://example.org:443/actor", "https://example.org/actor"),
            ("http://example.org:80/actor", "http://example.org/actor"),
            ("https://example.org/actor#main-key", "https://example.org/actor"),
            ("https://example.org/actor?x=1", "https://example.org/actor?x=1"),
        ]
    )
    func equivalent(lhs: String, rhs: String) {
        #expect(ActorIdentity.matches(lhs, rhs))
        #expect(ActorIdentity.matches(rhs, lhs))
    }

    @Test(
        "Distinct identifiers do not match",
        arguments: [
            ("https://example.org/actor", "https://example.org/Actor"),
            ("https://example.org/actor", "https://example.org/actor/"),
            ("https://example.org/actor", "http://example.org/actor"),
            ("https://example.org/actor", "https://example.org:8443/actor"),
            ("https://example.org/actor", "https://example.net/actor"),
            ("https://example.org/actor?x=1", "https://example.org/actor?x=2"),
            ("https://example.org/actor", "https://example.org/actor?x=1"),
        ]
    )
    func distinct(lhs: String, rhs: String) {
        #expect(!ActorIdentity.matches(lhs, rhs))
        #expect(!ActorIdentity.matches(rhs, lhs))
    }

    @Test("Non-URL identifiers fall back to string equality")
    func nonURLFallback() {
        #expect(ActorIdentity.matches("acct:alice@example.org", "acct:alice@example.org"))
        #expect(!ActorIdentity.matches("acct:alice@example.org", "acct:bob@example.org"))
    }
}

@Suite("RemoteActor Document Tests")
struct RemoteActorDocumentTests {
    @Test("Decodes a Misskey-style standalone Key document")
    func decodesKeyDocument() throws {
        let json = """
        {
          "id": "https://misskey.example/users/9abc/publickey",
          "type": "Key",
          "owner": "https://misskey.example/users/9abc",
          "publicKeyPem": "-----BEGIN PUBLIC KEY-----\\nabc\\n-----END PUBLIC KEY-----"
        }
        """
        let document = try JSONDecoder().decode(RemoteActor.self, from: Data(json.utf8))
        #expect(document.isKeyDocument)
        #expect(document.owner == "https://misskey.example/users/9abc")
        #expect(document.publicKey == nil)
    }

    @Test("Decodes an actor document whose publicKey omits owner")
    func decodesActorWithoutOwner() throws {
        let json = """
        {
          "id": "https://example.org/actor",
          "type": "Application",
          "inbox": "https://example.org/inbox",
          "publicKey": {
            "id": "https://example.org/actor#main-key",
            "publicKeyPem": "-----BEGIN PUBLIC KEY-----\\nabc\\n-----END PUBLIC KEY-----"
          }
        }
        """
        let document = try JSONDecoder().decode(RemoteActor.self, from: Data(json.utf8))
        #expect(!document.isKeyDocument)
        #expect(document.publicKey?.owner == nil)
    }
}
