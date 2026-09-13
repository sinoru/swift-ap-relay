import APRelayCore
import Foundation
import SwiftSoup
import Testing
import VaporTesting
@testable import APRelay

/// Minimal Leaf context for testing version-info template branches
/// without going through the controller.
private struct VersionInfoTestContext: Encodable {
    let softwareVersion: String
    let shortCommit: String?
    let sourceURL: String?
}

@Suite("HTML Render Tests", .serialized)
struct HTMLRenderTests {

    // MARK: - Mode Badges

    @Test("Default config shows open badge, auto accept badge, and no notices")
    func defaultModeBadges() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.testing().test(.GET, "/") { res async throws in
                let doc = try SwiftSoup.parse(res.body.string)
                #expect(try !doc.select(".header-meta .badge.badge-open").isEmpty())
                #expect(try doc.select(".header-meta .badge.badge-restricted").isEmpty())
                #expect(try !doc.select(".header-meta .badge.badge-muted").isEmpty())
                #expect(try doc.select(".notice.notice-restricted").isEmpty())
            }
        }
    }

    @Test("Restricted mode shows restricted badge and notice")
    func restrictedModeBadge() async throws {
        try await withApp(configure: testConfigureRestricted) { app in
            try await app.testing().test(.GET, "/") { res async throws in
                let doc = try SwiftSoup.parse(res.body.string)
                #expect(try !doc.select(".header-meta .badge.badge-restricted").isEmpty())
                #expect(try doc.select(".header-meta .badge.badge-open").isEmpty())
                #expect(try !doc.select(".notice.notice-restricted").isEmpty())
            }
        }
    }

    @Test("Manual accept mode shows manual accept badge and notice")
    func manualAcceptModeBadge() async throws {
        try await withApp(configure: testConfigureManualAccept) { app in
            try await app.testing().test(.GET, "/") { res async throws in
                let doc = try SwiftSoup.parse(res.body.string)
                #expect(try !doc.select(".header-meta .badge.badge-muted").isEmpty())
                #expect(try !doc.select(".notice.notice-restricted").isEmpty())
            }
        }
    }

    // MARK: - Description

    @Test("Description present renders description div and both meta tags")
    func descriptionPresent() async throws {
        try await withApp(configure: { app in
            try await testConfigure(app)
            app.relayConfig = RelayConfiguration(
                baseURL: "http://localhost",
                adminToken: "test-token",
                relayDescription: LocalizedString(["und": "<p>Test relay description</p>"])
            )
        }) { app in
            try await app.testing().test(.GET, "/") { res async throws in
                let doc = try SwiftSoup.parse(res.body.string)
                let descDiv = try doc.select(".description")
                #expect(!descDiv.isEmpty())
                #expect(try descDiv.first()?.text() == "Test relay description")

                let metaDesc = try doc.select("[name=description]")
                #expect(!metaDesc.isEmpty())
                #expect(try metaDesc.first()?.attr("content") == "Test relay description")

                let ogDesc = try doc.select(#"[property="og:description"]"#)
                #expect(!ogDesc.isEmpty())
                #expect(try ogDesc.first()?.attr("content") == "Test relay description")
            }
        }
    }

    @Test("Empty description does not render description div or meta description tags")
    func descriptionAbsent() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.testing().test(.GET, "/") { res async throws in
                let doc = try SwiftSoup.parse(res.body.string)
                #expect(try doc.select(".description").isEmpty())
                #expect(try doc.select("[name=description]").isEmpty())
                #expect(try doc.select(#"[property="og:description"]"#).isEmpty())
            }
        }
    }

    // MARK: - Footer

    @Test("Footer present renders custom-footer div")
    func footerPresent() async throws {
        try await withApp(configure: { app in
            try await testConfigure(app)
            app.relayConfig = RelayConfiguration(
                baseURL: "http://localhost",
                adminToken: "test-token",
                relayFooter: LocalizedString(["und": "<span>Custom Footer</span>"])
            )
        }) { app in
            try await app.testing().test(.GET, "/") { res async throws in
                let doc = try SwiftSoup.parse(res.body.string)
                let footerDiv = try doc.select("footer .custom-footer")
                #expect(!footerDiv.isEmpty())
                #expect(try footerDiv.first()?.text() == "Custom Footer")
            }
        }
    }

    @Test("Empty footer does not render custom-footer div")
    func footerAbsent() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.testing().test(.GET, "/") { res async throws in
                let doc = try SwiftSoup.parse(res.body.string)
                #expect(try doc.select("footer .custom-footer").isEmpty())
            }
        }
    }

    // MARK: - Subscriber List (Empty)

    @Test("No subscribers shows empty state message")
    func noSubscribers() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.testing().test(.GET, "/") { res async throws in
                let doc = try SwiftSoup.parse(res.body.string)
                #expect(try !doc.select(".empty-state").isEmpty())
                #expect(try doc.select(".instance-grid").isEmpty())
            }
        }
    }

    // MARK: - Subscriber List (Populated)

    @Test("Subscribers present renders instance grid with domains")
    func subscribersPresent() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.repository.seedSubscriber(Subscriber(
                domain: "mastodon.example.com",
                inboxURL: "https://mastodon.example.com/inbox",
                actorID: "https://mastodon.example.com/actor",
                state: .accepted,
                followActivityID: "https://mastodon.example.com/activity/1",
                createdAt: Date()
            ))

            try await app.testing().test(.GET, "/") { res async throws in
                let doc = try SwiftSoup.parse(res.body.string)
                #expect(try !doc.select(".instance-grid").isEmpty())
                #expect(try !doc.select(".instance-domain:contains(mastodon.example.com)").isEmpty())
                #expect(try doc.select(".empty-state").isEmpty())
            }
        }
    }

    // MARK: - Instance Details (with InstanceInfo)

    @Test("Subscriber with instance info renders software name and version")
    func subscriberWithSoftwareInfo() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.repository.seedSubscriber(Subscriber(
                domain: "info.example.com",
                inboxURL: "https://info.example.com/inbox",
                actorID: "https://info.example.com/actor",
                state: .accepted,
                followActivityID: "https://info.example.com/activity/1",
                createdAt: Date()
            ))
            let cache = app.instanceInfoCacheOverride as! MockInstanceInfoCache
            try await cache.setInstanceInfo(domain: "info.example.com", info: InstanceInfo(
                softwareName: "Mastodon",
                softwareVersion: "4.2.0",
                isReachable: true,
                lastCheckedAt: Date()
            ))

            try await app.testing().test(.GET, "/") { res async throws in
                let doc = try SwiftSoup.parse(res.body.string)
                let software = try doc.select(".instance-software")
                #expect(!software.isEmpty())
                let text = try software.first()?.text() ?? ""
                #expect(text.contains("Mastodon"))
                #expect(text.contains("4.2.0"))
            }
        }
    }

    @Test("Reachable subscriber shows online status dot")
    func reachableStatusDot() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.repository.seedSubscriber(Subscriber(
                domain: "online.example.com",
                inboxURL: "https://online.example.com/inbox",
                actorID: "https://online.example.com/actor",
                state: .accepted,
                followActivityID: "https://online.example.com/activity/1",
                createdAt: Date()
            ))
            let cache = app.instanceInfoCacheOverride as! MockInstanceInfoCache
            try await cache.setInstanceInfo(domain: "online.example.com", info: InstanceInfo(
                isReachable: true,
                lastCheckedAt: Date()
            ))

            try await app.testing().test(.GET, "/") { res async throws in
                let doc = try SwiftSoup.parse(res.body.string)
                #expect(try !doc.select(".status-dot.status-ok").isEmpty())
            }
        }
    }

    @Test("Unreachable subscriber shows down status dot")
    func unreachableStatusDot() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.repository.seedSubscriber(Subscriber(
                domain: "down.example.com",
                inboxURL: "https://down.example.com/inbox",
                actorID: "https://down.example.com/actor",
                state: .accepted,
                followActivityID: "https://down.example.com/activity/1",
                createdAt: Date()
            ))
            let cache = app.instanceInfoCacheOverride as! MockInstanceInfoCache
            try await cache.setInstanceInfo(domain: "down.example.com", info: InstanceInfo(
                isReachable: false,
                lastCheckedAt: Date()
            ))

            try await app.testing().test(.GET, "/") { res async throws in
                let doc = try SwiftSoup.parse(res.body.string)
                #expect(try !doc.select(".status-dot.status-down").isEmpty())
            }
        }
    }

    @Test("Subscriber without instance info shows unknown status dot")
    func uncheckedStatusDot() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.repository.seedSubscriber(Subscriber(
                domain: "unchecked.example.com",
                inboxURL: "https://unchecked.example.com/inbox",
                actorID: "https://unchecked.example.com/actor",
                state: .accepted,
                followActivityID: "https://unchecked.example.com/activity/1",
                createdAt: Date()
            ))

            try await app.testing().test(.GET, "/") { res async throws in
                let doc = try SwiftSoup.parse(res.body.string)
                #expect(try !doc.select(".status-dot.status-unknown").isEmpty())
            }
        }
    }

    @Test("Open registrations badge is rendered")
    func openRegistrationsBadge() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.repository.seedSubscriber(Subscriber(
                domain: "openreg.example.com",
                inboxURL: "https://openreg.example.com/inbox",
                actorID: "https://openreg.example.com/actor",
                state: .accepted,
                followActivityID: "https://openreg.example.com/activity/1",
                createdAt: Date()
            ))
            let cache = app.instanceInfoCacheOverride as! MockInstanceInfoCache
            try await cache.setInstanceInfo(domain: "openreg.example.com", info: InstanceInfo(
                softwareName: "Mastodon",
                openRegistrations: true,
                isReachable: true,
                lastCheckedAt: Date()
            ))

            try await app.testing().test(.GET, "/") { res async throws in
                let doc = try SwiftSoup.parse(res.body.string)
                #expect(try !doc.select(".instance-meta .badge.badge-sm.badge-open").isEmpty())
            }
        }
    }

    @Test("Closed registrations badge is rendered")
    func closedRegistrationsBadge() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.repository.seedSubscriber(Subscriber(
                domain: "closedreg.example.com",
                inboxURL: "https://closedreg.example.com/inbox",
                actorID: "https://closedreg.example.com/actor",
                state: .accepted,
                followActivityID: "https://closedreg.example.com/activity/1",
                createdAt: Date()
            ))
            let cache = app.instanceInfoCacheOverride as! MockInstanceInfoCache
            try await cache.setInstanceInfo(domain: "closedreg.example.com", info: InstanceInfo(
                softwareName: "Mastodon",
                openRegistrations: false,
                isReachable: true,
                lastCheckedAt: Date()
            ))

            try await app.testing().test(.GET, "/") { res async throws in
                let doc = try SwiftSoup.parse(res.body.string)
                #expect(try !doc.select(".instance-meta .badge.badge-sm.badge-muted").isEmpty())
            }
        }
    }

    @Test("Staff accounts are rendered as links")
    func staffAccountsRendered() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.repository.seedSubscriber(Subscriber(
                domain: "staff.example.com",
                inboxURL: "https://staff.example.com/inbox",
                actorID: "https://staff.example.com/actor",
                state: .accepted,
                followActivityID: "https://staff.example.com/activity/1",
                createdAt: Date()
            ))
            let cache = app.instanceInfoCacheOverride as! MockInstanceInfoCache
            try await cache.setInstanceInfo(domain: "staff.example.com", info: InstanceInfo(
                staffAccounts: ["https://staff.example.com/@admin"],
                isReachable: true,
                lastCheckedAt: Date()
            ))

            try await app.testing().test(.GET, "/") { res async throws in
                let doc = try SwiftSoup.parse(res.body.string)
                let staffLinks = try doc.select(".instance-staff .staff-link")
                #expect(!staffLinks.isEmpty())
                #expect(try staffLinks.first()?.attr("href") == "https://staff.example.com/@admin")
                #expect(try staffLinks.first()?.text() == "https://staff.example.com/@admin")
            }
        }
    }

    @Test("Subscriber count is displayed")
    func subscriberCountDisplayed() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.repository.seedSubscriber(Subscriber(
                domain: "one.example.com",
                inboxURL: "https://one.example.com/inbox",
                actorID: "https://one.example.com/actor",
                state: .accepted,
                followActivityID: "https://one.example.com/activity/1",
                createdAt: Date()
            ))
            try await app.repository.seedSubscriber(Subscriber(
                domain: "two.example.com",
                inboxURL: "https://two.example.com/inbox",
                actorID: "https://two.example.com/actor",
                state: .accepted,
                followActivityID: "https://two.example.com/activity/1",
                createdAt: Date()
            ))

            try await app.testing().test(.GET, "/") { res async throws in
                let doc = try SwiftSoup.parse(res.body.string)
                let items = try doc.select(".instance-grid .instance-item")
                #expect(items.size() == 2)
            }
        }
    }

    @Test("Favicon URL uses instance info when available")
    func faviconFromInstanceInfo() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.repository.seedSubscriber(Subscriber(
                domain: "favicon.example.com",
                inboxURL: "https://favicon.example.com/inbox",
                actorID: "https://favicon.example.com/actor",
                state: .accepted,
                followActivityID: "https://favicon.example.com/activity/1",
                createdAt: Date()
            ))
            let cache = app.instanceInfoCacheOverride as! MockInstanceInfoCache
            try await cache.setInstanceInfo(domain: "favicon.example.com", info: InstanceInfo(
                faviconURL: "https://cdn.example.com/favicon.png",
                isReachable: true,
                lastCheckedAt: Date()
            ))

            try await app.testing().test(.GET, "/") { res async throws in
                let doc = try SwiftSoup.parse(res.body.string)
                let favicon = try doc.select(".instance-favicon")
                #expect(!favicon.isEmpty())
                let src = try favicon.first()?.attr("src") ?? ""
                #expect(src == "https://cdn.example.com/favicon.png")
            }
        }
    }

    // MARK: - Software Name Without Version

    @Test("Software name without version renders name only")
    func softwareNameWithoutVersion() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.repository.seedSubscriber(Subscriber(
                domain: "nover.example.com",
                inboxURL: "https://nover.example.com/inbox",
                actorID: "https://nover.example.com/actor",
                state: .accepted,
                followActivityID: "https://nover.example.com/activity/1",
                createdAt: Date()
            ))
            let cache = app.instanceInfoCacheOverride as! MockInstanceInfoCache
            try await cache.setInstanceInfo(domain: "nover.example.com", info: InstanceInfo(
                softwareName: "Pleroma",
                isReachable: true,
                lastCheckedAt: Date()
            ))

            try await app.testing().test(.GET, "/") { res async throws in
                let doc = try SwiftSoup.parse(res.body.string)
                let software = try doc.select(".instance-software")
                #expect(!software.isEmpty())
                let text = try software.first()?.text() ?? ""
                #expect(text == "Pleroma", "Software name should render without a version when softwareVersion is nil")
                // No registration info → no registration badge
                #expect(try doc.select(".instance-meta .badge.badge-sm").isEmpty())
            }
        }
    }

    // MARK: - Subscriber Without Instance Info (absence assertions)

    @Test("Subscriber without instance info has no instance-meta or instance-staff")
    func subscriberWithoutInstanceInfoHasNoMeta() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.repository.seedSubscriber(Subscriber(
                domain: "bare.example.com",
                inboxURL: "https://bare.example.com/inbox",
                actorID: "https://bare.example.com/actor",
                state: .accepted,
                followActivityID: "https://bare.example.com/activity/1",
                createdAt: Date()
            ))

            try await app.testing().test(.GET, "/") { res async throws in
                let doc = try SwiftSoup.parse(res.body.string)
                let item = try doc.select(".instance-item")
                #expect(!item.isEmpty())
                #expect(try doc.select(".instance-meta").isEmpty())
                #expect(try doc.select(".instance-staff").isEmpty())
            }
        }
    }

    // MARK: - Joined Date

    @Test("Subscriber with createdAt renders joined date")
    func joinedAtPresent() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.repository.seedSubscriber(Subscriber(
                domain: "joined.example.com",
                inboxURL: "https://joined.example.com/inbox",
                actorID: "https://joined.example.com/actor",
                state: .accepted,
                followActivityID: "https://joined.example.com/activity/1",
                createdAt: Date()
            ))

            try await app.testing().test(.GET, "/") { res async throws in
                let doc = try SwiftSoup.parse(res.body.string)
                #expect(try !doc.select(".instance-joined").isEmpty())
            }
        }
    }

    @Test("Subscriber without createdAt does not render joined date")
    func joinedAtAbsent() async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.repository.seedSubscriber(Subscriber(
                domain: "nodate.example.com",
                inboxURL: "https://nodate.example.com/inbox",
                actorID: "https://nodate.example.com/actor",
                state: .accepted,
                followActivityID: "https://nodate.example.com/activity/1",
                createdAt: nil
            ))

            try await app.testing().test(.GET, "/") { res async throws in
                let doc = try SwiftSoup.parse(res.body.string)
                #expect(try !doc.select(".instance-item").isEmpty())
                #expect(try doc.select(".instance-joined").isEmpty())
            }
        }
    }

    // MARK: - Version Info / Commit

    @Test("Version info renders commit link when SOURCE_COMMIT is set")
    func versionInfoRendersCommitLinkWithCommit() async throws {
        let knownCommit = "da39a3ee5e6b4b0d3255bfef95601890afd80709"
        let previous = ProcessInfo.processInfo.environment["SOURCE_COMMIT"]
        unsafe setenv("SOURCE_COMMIT", knownCommit, 1)
        defer {
            if let previous {
                unsafe setenv("SOURCE_COMMIT", previous, 1)
            } else {
                unsafe unsetenv("SOURCE_COMMIT")
            }
        }

        try await withApp(configure: testConfigure) { app in
            try await app.testing().test(.GET, "/") { res async throws in
                let doc = try SwiftSoup.parse(res.body.string)
                let versionInfo = try doc.select(".version-info")
                #expect(!versionInfo.isEmpty())
                let text = try versionInfo.first()?.text() ?? ""
                #expect(text.contains("APRelay"))

                let commitLink = try doc.select(".version-info a[href]")
                #expect(!commitLink.isEmpty())
                let href = try commitLink.first()?.attr("href") ?? ""
                #expect(href.contains(knownCommit))
            }
        }
    }

    @Test("Version info renders without commit link when shortCommit is nil")
    func versionInfoRendersWithoutCommitLink() async throws {
        try await withApp(configure: testConfigure) { app in
            let context = VersionInfoTestContext(
                softwareVersion: "1.0.0",
                shortCommit: nil,
                sourceURL: nil
            )
            let buf = try await app.view.render("index", context).data
            let html = String(buffer: buf)
            let doc = try SwiftSoup.parse(html)

            let versionInfo = try doc.select(".version-info")
            #expect(!versionInfo.isEmpty())
            let text = try versionInfo.first()?.text() ?? ""
            #expect(text.contains("APRelay"))
            #expect(text.contains("1.0.0"))

            let commitLink = try doc.select(".version-info a[href]")
            #expect(commitLink.isEmpty())
        }
    }
}
