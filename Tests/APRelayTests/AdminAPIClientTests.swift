import Testing
import Vapor
import VaporTesting
@testable import APRelay

@Suite("AdminAPIClient Tests", .serialized)
struct AdminAPIClientTests {

    private func withAdminClient(
        _ body: (Application, AdminAPIClient) async throws -> Void
    ) async throws {
        try await withApp(configure: testConfigure) { app in
            try await app.server.start(address: .hostname("localhost", port: 0))
            do {
                guard let port = app.http.server.shared.localAddress?.port else {
                    throw Abort(.internalServerError, reason: "Failed to get port")
                }
                let client = AdminAPIClient(
                    client: app.client,
                    baseURL: "http://localhost:\(port)",
                    adminToken: "test-token"
                )
                try await body(app, client)
                await app.server.shutdown()
            } catch {
                await app.server.shutdown()
                throw error
            }
        }
    }

    // MARK: - Subscribers

    @Test("listSubscribers returns empty list")
    func listSubscribersEmpty() async throws {
        try await withAdminClient { _, client in
            let subscribers = try await client.listSubscribers()
            #expect(subscribers.isEmpty)
        }
    }

    @Test("listSubscribers returns subscribers")
    func listSubscribers() async throws {
        try await withAdminClient { app, client in
            let sub = Subscriber(
                domain: "example.com",
                inboxURL: "https://example.com/inbox",
                actorID: "https://example.com/actor",
                state: .accepted,
                followActivityID: "https://example.com/follow/1",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.seedSubscriber(sub)

            let subscribers = try await client.listSubscribers()
            #expect(subscribers.count == 1)
            #expect(subscribers.first?.domain == "example.com")
            #expect(subscribers.first?.state == .accepted)
        }
    }

    @Test("listSubscribers filters by state")
    func listSubscribersFilterState() async throws {
        try await withAdminClient { app, client in
            let accepted = Subscriber(
                domain: "accepted.example",
                inboxURL: "https://accepted.example/inbox",
                actorID: "https://accepted.example/actor",
                state: .accepted,
                followActivityID: "https://accepted.example/follow/1",
                createdAt: Date(),
                updatedAt: Date()
            )
            let pending = Subscriber(
                domain: "pending.example",
                inboxURL: "https://pending.example/inbox",
                actorID: "https://pending.example/actor",
                state: .pending,
                followActivityID: "https://pending.example/follow/1",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.seedSubscriber(accepted)
            try await app.repository.seedSubscriber(pending)

            let subscribers = try await client.listSubscribers(state: "pending")
            #expect(subscribers.count == 1)
            #expect(subscribers.first?.domain == "pending.example")
        }
    }

    @Test("acceptSubscriber changes state to accepted")
    func acceptSubscriber() async throws {
        try await withAdminClient { app, client in
            let sub = Subscriber(
                domain: "test.example",
                inboxURL: "https://test.example/inbox",
                actorID: "https://test.example/actor",
                state: .pending,
                followActivityID: "https://test.example/follow/1",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.seedSubscriber(sub)

            let response = try await client.acceptSubscriber(domain: "test.example")
            #expect(response.status == "accepted")
            #expect(response.domain == "test.example")

            let updated = try await app.repository.getSubscriber(domain: "test.example")
            #expect(updated?.state == .accepted)
        }
    }

    @Test("acceptSubscriber throws notFound for non-existent subscriber")
    func acceptNonExistent() async throws {
        try await withAdminClient { _, client in
            await #expect(throws: AdminAPIError.self) {
                _ = try await client.acceptSubscriber(domain: "nonexistent.example")
            }
        }
    }

    @Test("rejectSubscriber changes state to rejected")
    func rejectSubscriber() async throws {
        try await withAdminClient { app, client in
            let sub = Subscriber(
                domain: "test.example",
                inboxURL: "https://test.example/inbox",
                actorID: "https://test.example/actor",
                state: .pending,
                followActivityID: "https://test.example/follow/1",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.seedSubscriber(sub)

            let response = try await client.rejectSubscriber(domain: "test.example")
            #expect(response.status == "rejected")

            let updated = try await app.repository.getSubscriber(domain: "test.example")
            #expect(updated?.state == .rejected)
        }
    }

    // MARK: - Blocked Domains

    @Test("listBlockedDomains returns empty list")
    func listBlockedDomainsEmpty() async throws {
        try await withAdminClient { _, client in
            let domains = try await client.listBlockedDomains()
            #expect(domains.isEmpty)
        }
    }

    @Test("listBlockedDomains returns blocked domains")
    func listBlockedDomains() async throws {
        try await withAdminClient { app, client in
            _ = try await app.repository.blockDomain("bad.example", reason: "spam")

            let domains = try await client.listBlockedDomains()
            #expect(domains.count == 1)
            #expect(domains.first?.domain == "bad.example")
            #expect(domains.first?.reason == "spam")
        }
    }

    @Test("blockDomain blocks domain and removes subscriber")
    func blockDomain() async throws {
        try await withAdminClient { app, client in
            let sub = Subscriber(
                domain: "bad.example",
                inboxURL: "https://bad.example/inbox",
                actorID: "https://bad.example/actor",
                state: .accepted,
                followActivityID: "https://bad.example/follow/1",
                createdAt: Date(),
                updatedAt: Date()
            )
            try await app.repository.seedSubscriber(sub)

            let response = try await client.blockDomain("bad.example", reason: "spam")
            #expect(response.status == "blocked")
            #expect(response.domain == "bad.example")

            let subscribers = try await app.repository.getAllSubscribers(state: nil)
            #expect(subscribers.count == 0)

            let isBlocked = try await app.repository.isBlocked(domain: "bad.example")
            #expect(isBlocked)
        }
    }

    @Test("blockDomain throws conflict for already-blocked domain")
    func blockAlreadyBlocked() async throws {
        try await withAdminClient { app, client in
            _ = try await app.repository.blockDomain("bad.example", reason: nil)

            await #expect(throws: AdminAPIError.self) {
                _ = try await client.blockDomain("bad.example")
            }
        }
    }

    @Test("unblockDomain removes blocked domain")
    func unblockDomain() async throws {
        try await withAdminClient { app, client in
            _ = try await app.repository.blockDomain("bad.example", reason: nil)

            let response = try await client.unblockDomain("bad.example")
            #expect(response.status == "unblocked")

            let isBlocked = try await app.repository.isBlocked(domain: "bad.example")
            #expect(!isBlocked)
        }
    }

    @Test("unblockDomain throws notFound for non-blocked domain")
    func unblockNonBlocked() async throws {
        try await withAdminClient { _, client in
            await #expect(throws: AdminAPIError.self) {
                _ = try await client.unblockDomain("nonexistent.example")
            }
        }
    }
}
