import APRelayCore
import Vapor
import XCTQueues
@testable import APRelay

/// Configures the app for testing with mock repository, deduplicator, instance info cache,
/// actor cache, and actor fetcher.
func testConfigure(_ app: Application) async throws {
    app.repositoryOverride = MockRelayRepository()
    app.deduplicatorOverride = MockActivityDeduplicator()
    app.instanceInfoCacheOverride = MockInstanceInfoCache()
    app.actorCacheOverride = MockActorCache()
    app.queues.use(.asyncTest)
    try await APRelay.configure(app)
    app.relayConfig = RelayConfiguration(
        baseURL: "http://localhost",
        adminToken: "test-token"
    )
    app.actorFetcher = MockActorFetcher()
}

/// Configures the app with manual accept mode enabled.
func testConfigureManualAccept(_ app: Application) async throws {
    app.repositoryOverride = MockRelayRepository()
    app.deduplicatorOverride = MockActivityDeduplicator()
    app.instanceInfoCacheOverride = MockInstanceInfoCache()
    app.actorCacheOverride = MockActorCache()
    app.queues.use(.asyncTest)
    try await APRelay.configure(app)
    app.relayConfig = RelayConfiguration(
        baseURL: "http://localhost",
        adminToken: "test-token",
        manualAccept: true
    )
    app.actorFetcher = MockActorFetcher()
}

/// Configures the app with restricted mode enabled.
func testConfigureRestricted(_ app: Application) async throws {
    app.repositoryOverride = MockRelayRepository()
    app.deduplicatorOverride = MockActivityDeduplicator()
    app.instanceInfoCacheOverride = MockInstanceInfoCache()
    app.actorCacheOverride = MockActorCache()
    app.queues.use(.asyncTest)
    try await APRelay.configure(app)
    app.relayConfig = RelayConfiguration(
        baseURL: "http://localhost",
        adminToken: "test-token",
        restrictedMode: true
    )
    app.actorFetcher = MockActorFetcher()
}
