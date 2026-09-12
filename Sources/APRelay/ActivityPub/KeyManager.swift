import Crypto
import _CryptoExtras
import Foundation

/// Manages RSA key pair generation, storage, and retrieval.
struct KeyManager: Sendable {
    private static let privateKeySetting = "rsa_private_key_pem"
    private static let publicKeySetting = "rsa_public_key_pem"

    private let repository: any RelayRepository

    init(repository: any RelayRepository) {
        self.repository = repository
    }

    /// Retrieves the existing private key from the repository, or generates and stores a new one.
    ///
    /// Several replicas may boot against an empty store at the same time and
    /// each generate a candidate key. Exactly one candidate wins the
    /// `setSettingIfAbsent` write; every other replica discards its own and
    /// adopts the stored key, so all replicas sign with the same key pair.
    func getOrCreatePrivateKey() async throws -> _RSA.Signing.PrivateKey {
        if let pem = try await repository.getSetting(key: Self.privateKeySetting) {
            return try _RSA.Signing.PrivateKey(pemRepresentation: pem)
        }

        let candidate = try _RSA.Signing.PrivateKey(keySize: .bits4096)
        let privateKey: _RSA.Signing.PrivateKey
        if try await repository.setSettingIfAbsent(
            key: Self.privateKeySetting,
            value: candidate.pemRepresentation
        ) {
            privateKey = candidate
        } else {
            guard let pem = try await repository.getSetting(key: Self.privateKeySetting) else {
                throw KeyManagerError.privateKeyUnavailable
            }
            privateKey = try _RSA.Signing.PrivateKey(pemRepresentation: pem)
        }

        // Derived from the winning private key by every replica, so whichever
        // write lands first stores the matching public key.
        _ = try await repository.setSettingIfAbsent(
            key: Self.publicKeySetting,
            value: privateKey.publicKey.pemRepresentation
        )
        return privateKey
    }

    /// Retrieves the public key PEM string.
    func getPublicKeyPEM() async throws -> String {
        if let pem = try await repository.getSetting(key: Self.publicKeySetting) {
            return pem
        }
        let publicPEM = try await getOrCreatePrivateKey().publicKey.pemRepresentation
        _ = try await repository.setSettingIfAbsent(key: Self.publicKeySetting, value: publicPEM)
        return publicPEM
    }
}

enum KeyManagerError: Error {
    /// The private key lost the bootstrap race but the winning key could not
    /// be read back afterwards.
    case privateKeyUnavailable
}
