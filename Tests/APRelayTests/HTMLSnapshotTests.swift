import APRelayCore
import Foundation
import Testing
import VaporTesting
@testable import APRelay

/// Visual snapshot tests that write self-contained HTML files to disk.
///
/// These tests only produce output when the `HTML_SNAPSHOT_DIR` environment
/// variable is set. Run them like so:
///
///     HTML_SNAPSHOT_DIR=html-snapshots swift test --filter HTMLSnapshotTests
///     open html-snapshots/
///
/// Each snapshot inlines the CSS so it renders correctly when opened directly
/// in a browser — no running server needed.
///
/// Output is organized by locale:
///
///     html-snapshots/
///       en/
///         default.html
///         restricted-mode.html
///         with-subscribers.html
///         ...
///       ja/
///         ...
///       ko/
///         ...
@Suite(
    "HTML Snapshot Tests",
    .serialized,
    .enabled(if: ProcessInfo.processInfo.environment["HTML_SNAPSHOT_DIR"] != nil)
)
struct HTMLSnapshotTests {

    /// A fixed date for deterministic snapshot output (2026-01-01T00:00:00Z).
    private static let fixedDate = Date(timeIntervalSince1970: 1767225600)

    // MARK: - Default State

    @Test("Default page")
    func defaultPage() async throws {
        try await withApp(configure: testConfigure) { app in
            try await saveHTMLSnapshotsForAllLocales(name: "default", app: app)
        }
    }

    // MARK: - Long Strings Without Spaces

    @Test("Long strings without spaces")
    func longStringsWithoutSpaces() async throws {
        try await withApp(configure: { app in
            try await testConfigure(app)
            app.relayConfig = RelayConfiguration(
                baseURL: "http://localhost",
                adminToken: "test-token",
                relayName: LocalizedString([
                    "und": "ThisIsAnExtremelyLongRelayNameWithoutAnySpacesThatShouldTriggerHorizontalScrollingBehaviorAsReportedInIssueNumber3"
                ]),
                relayDescription: LocalizedString([
                    "und": "<p>ThisIsAlsoAVeryLongDescriptionStringWithoutAnySpacesThatMightCauseHorizontalScrollingInTheDescriptionAreaOfTheWebUI</p>"
                ]),
                relayFooter: LocalizedString([
                    "und": "<span>AndThisIsAnExtremelyLongFooterTextWithNoSpacesAtAllToTestOverflowBehaviorInTheCustomFooterSection</span>"
                ])
            )
        }) { app in
            try await saveHTMLSnapshotsForAllLocales(name: "long-strings", app: app)
        }
    }

    // MARK: - With Subscribers

    @Test("Page with subscribers and instance info")
    func withSubscribers() async throws {
        try await withApp(configure: { app in
            try await testConfigure(app)
            app.relayConfig = RelayConfiguration(
                baseURL: "http://localhost",
                adminToken: "test-token",
                relayDescription: LocalizedString(["und": "<p>A federated relay for the fediverse.</p>"])
            )
        }) { app in
            try await app.repository.seedSubscriber(Subscriber(
                domain: "mastodon.social",
                inboxURL: "https://mastodon.social/inbox",
                actorID: "https://mastodon.social/actor",
                state: .accepted,
                followActivityID: "https://mastodon.social/activity/1",
                createdAt: Self.fixedDate
            ))
            try await app.repository.seedSubscriber(Subscriber(
                domain: "misskey.io",
                inboxURL: "https://misskey.io/inbox",
                actorID: "https://misskey.io/actor",
                state: .accepted,
                followActivityID: "https://misskey.io/activity/1",
                createdAt: Self.fixedDate
            ))
            try await app.repository.seedSubscriber(Subscriber(
                domain: "pleroma.example.com",
                inboxURL: "https://pleroma.example.com/inbox",
                actorID: "https://pleroma.example.com/actor",
                state: .accepted,
                followActivityID: "https://pleroma.example.com/activity/1",
                createdAt: Self.fixedDate
            ))

            let cache = app.instanceInfoCacheOverride as! MockInstanceInfoCache
            try await cache.setInstanceInfo(domain: "mastodon.social", info: InstanceInfo(
                softwareName: "Mastodon",
                softwareVersion: "4.3.0",
                openRegistrations: true,
                staffAccounts: ["https://mastodon.social/@admin"],
                isReachable: true,
                lastCheckedAt: Self.fixedDate
            ))
            try await cache.setInstanceInfo(domain: "misskey.io", info: InstanceInfo(
                softwareName: "Misskey",
                softwareVersion: "2024.11.0",
                openRegistrations: false,
                isReachable: true,
                lastCheckedAt: Self.fixedDate
            ))
            try await cache.setInstanceInfo(domain: "pleroma.example.com", info: InstanceInfo(
                softwareName: "Pleroma",
                isReachable: false,
                lastCheckedAt: Self.fixedDate
            ))

            try await saveHTMLSnapshotsForAllLocales(name: "with-subscribers", app: app)
        }
    }

    // MARK: - Restricted Mode

    @Test("Restricted mode page")
    func restrictedMode() async throws {
        try await withApp(configure: { app in
            try await testConfigure(app)
            app.relayConfig = RelayConfiguration(
                baseURL: "http://localhost",
                adminToken: "test-token",
                restrictedMode: true,
                relayDescription: LocalizedString(["und": "<p>This is a restricted relay.</p>"])
            )
        }) { app in
            try await saveHTMLSnapshotsForAllLocales(name: "restricted-mode", app: app)
        }
    }

    // MARK: - Manual Accept Mode

    @Test("Manual accept mode page")
    func manualAcceptMode() async throws {
        try await withApp(configure: { app in
            try await testConfigure(app)
            app.relayConfig = RelayConfiguration(
                baseURL: "http://localhost",
                adminToken: "test-token",
                manualAccept: true
            )
        }) { app in
            try await saveHTMLSnapshotsForAllLocales(name: "manual-accept-mode", app: app)
        }
    }
}
