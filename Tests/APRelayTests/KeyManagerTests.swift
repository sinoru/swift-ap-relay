import _CryptoExtras
import Testing
@testable import APRelay

@Suite("Key Manager Tests")
struct KeyManagerTests {
    private static let privateKeySetting = "rsa_private_key_pem"
    private static let publicKeySetting = "rsa_public_key_pem"

    @Test("Generates and stores a matching key pair when the store is empty")
    func bootstrapsEmptyStore() async throws {
        let repository = MockRelayRepository()
        let manager = KeyManager(repository: repository)

        let privateKey = try await manager.getOrCreatePrivateKey()

        let storedPrivate = try await repository.getSetting(key: Self.privateKeySetting)
        let storedPublic = try await repository.getSetting(key: Self.publicKeySetting)
        #expect(storedPrivate == privateKey.pemRepresentation)
        #expect(storedPublic == privateKey.publicKey.pemRepresentation)
    }

    @Test("Reuses the stored key on later boots")
    func reusesStoredKey() async throws {
        let repository = MockRelayRepository()
        let existing = try _RSA.Signing.PrivateKey(keySize: .bits2048)
        try await repository.setSetting(key: Self.privateKeySetting, value: existing.pemRepresentation)
        let manager = KeyManager(repository: repository)

        let privateKey = try await manager.getOrCreatePrivateKey()

        #expect(privateKey.pemRepresentation == existing.pemRepresentation)
    }

    @Test("Adopts the key another replica stored first instead of overwriting it")
    func losesBootstrapRace() async throws {
        // The other replica's key is already stored, but this replica's
        // initial read predates it and reports the setting as absent.
        let base = MockRelayRepository()
        let winner = try _RSA.Signing.PrivateKey(keySize: .bits2048)
        try await base.setSetting(key: Self.privateKeySetting, value: winner.pemRepresentation)
        let repository = StaleReadRelayRepository(
            base: base,
            staleAbsentSettingKeys: [Self.privateKeySetting]
        )
        let manager = KeyManager(repository: repository)

        let privateKey = try await manager.getOrCreatePrivateKey()

        // The generated candidate is discarded; the stored pair stays consistent.
        #expect(privateKey.pemRepresentation == winner.pemRepresentation)
        let storedPrivate = try await base.getSetting(key: Self.privateKeySetting)
        let storedPublic = try await base.getSetting(key: Self.publicKeySetting)
        #expect(storedPrivate == winner.pemRepresentation)
        #expect(storedPublic == winner.publicKey.pemRepresentation)
    }

    @Test("Public key is derived and stored when only the private key exists")
    func derivesMissingPublicKey() async throws {
        let repository = MockRelayRepository()
        let existing = try _RSA.Signing.PrivateKey(keySize: .bits2048)
        try await repository.setSetting(key: Self.privateKeySetting, value: existing.pemRepresentation)
        let manager = KeyManager(repository: repository)

        let publicPEM = try await manager.getPublicKeyPEM()

        #expect(publicPEM == existing.publicKey.pemRepresentation)
        let storedPublic = try await repository.getSetting(key: Self.publicKeySetting)
        #expect(storedPublic == existing.publicKey.pemRepresentation)
    }
}
