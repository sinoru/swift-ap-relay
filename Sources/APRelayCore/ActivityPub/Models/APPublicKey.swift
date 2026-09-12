import Foundation

/// ActivityPub public key block embedded in an actor document.
package struct APPublicKey: Codable, Sendable {
    package let id: String
    /// The actor that owns this key. Every major implementation sets it, but
    /// it is optional here so a document that omits it is not rejected outright.
    package let owner: String?
    package let publicKeyPem: String

    package init(id: String, owner: String?, publicKeyPem: String) {
        self.id = id
        self.owner = owner
        self.publicKeyPem = publicKeyPem
    }
}
