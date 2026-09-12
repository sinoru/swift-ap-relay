import APRelayCore
import Foundation
import Queues
import Vapor

/// Payload identifying a single domain whose instance info should be fetched.
struct InstanceInfoFetchPayload: Codable, Sendable {
    let domain: String

    /// Token of the pending-fetch claim held for this job by
    /// ``InstanceInfoCheckJob`` (the job's own id); released when the job
    /// finishes. `nil` for fetches triggered directly (new or newly accepted
    /// subscriber), which hold no claim.
    let claimToken: String?

    init(domain: String, claimToken: String? = nil) {
        self.domain = domain
        self.claimToken = claimToken
    }
}

/// Fetches and caches instance info for a single remote domain.
///
/// Dispatched by ``InstanceInfoCheckJob`` — one job per subscriber domain.
/// Queue worker count naturally limits concurrent outbound requests.
struct InstanceInfoFetchJob: AsyncJob {
    typealias Payload = InstanceInfoFetchPayload

    /// Backoff after consecutive failures: 60s doubling per failure, capped at
    /// 30 minutes, with equal jitter.
    static let backoffBaseSeconds = 60
    static let backoffMaxSeconds = 30 * 60

    /// Deadline for each of the HTTP requests a fetch makes. The client has
    /// no read timeout of its own, so without this a server that accepts the
    /// connection and then stalls would hold the worker, and the domain,
    /// indefinitely.
    static let requestTimeoutSeconds: Int64 = 30

    /// How long a job holds its domain once it starts fetching. A fetch makes
    /// at most three requests, so it is bounded by roughly three request
    /// timeouts plus connection setup, well inside this lease; a healthy job
    /// therefore never loses its claim mid-flight, while a worker crashing
    /// mid-fetch frees the domain within minutes.
    static let runningLeaseSeconds = 600

    func dequeue(_ context: QueueContext, _ payload: InstanceInfoFetchPayload) async throws {
        let app = context.application
        let cache = app.instanceInfoCache
        let client = app.client

        guard await beginFetch(payload, cache: cache, logger: context.logger) else {
            context.logger.info(
                "Skipping superseded instance info fetch for \(payload.domain): a later tick re-dispatched the domain"
            )
            return
        }

        let info = try await fetchInstanceInfo(domain: payload.domain, client: client)
        try await cache.setInstanceInfo(domain: payload.domain, info: info)
        await releaseClaim(for: payload, cache: cache, logger: context.logger)
        context.logger.debug("Instance info check succeeded for \(payload.domain)")
    }

    func error(_ context: QueueContext, _ error: any Error, _ payload: InstanceInfoFetchPayload) async throws {
        let cache = context.application.instanceInfoCache
        let now = Date()

        // The cache applies the failure atomically (keeps last-known metadata,
        // flips reachability, increments the counter) so two failures for the
        // same domain cannot lose an increment.
        do {
            let updated = try await cache.recordFailure(
                domain: payload.domain,
                at: now,
                nextAttemptAt: Self.nextAttemptSchedule(from: now)
            )
            let nextAttemptAt = updated.nextAttemptAt.map { "\($0)" } ?? "none"
            context.logger.warning(
                "Instance info check failed for \(payload.domain) (failures=\(updated.consecutiveFailures), nextAttemptAt=\(nextAttemptAt)): \(error)"
            )
        } catch let cacheError {
            context.logger.warning(
                "Instance info check failed for \(payload.domain) and the failure could not be recorded (\(cacheError)): \(error)"
            )
        }
        await releaseClaim(for: payload, cache: cache, logger: context.logger)
    }

    /// Next-attempt dates for failure counts 1, 2, ... up to the count at which
    /// the backoff reaches its cap; the cache picks the entry for the count it
    /// lands on, and the last entry covers every higher count.
    static func nextAttemptSchedule(from now: Date) -> [Date] {
        var cappedAttempt = 1
        while backoffBaseSeconds << (cappedAttempt - 1) < backoffMaxSeconds {
            cappedAttempt += 1
        }
        return (1...cappedAttempt).map { attempt in
            let delay = Self.exponentialBackoffSeconds(
                attempt: attempt,
                base: backoffBaseSeconds,
                maxInterval: backoffMaxSeconds
            )
            return now.addingTimeInterval(TimeInterval(delay))
        }
    }

    /// Takes the running lease for a tick-dispatched job, atomically checking
    /// that it is still the current fetch for its domain.
    ///
    /// Succeeds when the claim is still this job's queued claim, or when no
    /// claim is held (the queued claim lapsed with no replacement; several
    /// such copies race and exactly one wins). Fails when a later tick has
    /// claimed the domain for a newer job, which then runs instead. Directly
    /// triggered fetches hold no claim and always run.
    ///
    /// If the cache cannot be reached, the job runs anyway: a cache error
    /// must not turn into a recorded reachability failure for the instance.
    func beginFetch(
        _ payload: InstanceInfoFetchPayload,
        cache: any InstanceInfoCaching,
        logger: Logger
    ) async -> Bool {
        guard let token = payload.claimToken else { return true }
        do {
            return try await cache.startFetch(
                domain: payload.domain,
                token: token,
                leaseSeconds: Self.runningLeaseSeconds
            )
        } catch {
            logger.warning("Failed to take the instance info fetch lease for \(payload.domain); running the fetch: \(error)")
            return true
        }
    }

    /// Best-effort release of the tick's pending-fetch claim. A failure only
    /// means the claim lasts until its TTL, so it is logged, not propagated.
    private func releaseClaim(
        for payload: InstanceInfoFetchPayload,
        cache: any InstanceInfoCaching,
        logger: Logger
    ) async {
        guard let token = payload.claimToken else { return }
        do {
            try await cache.releaseFetch(domain: payload.domain, token: token)
        } catch {
            logger.warning("Failed to release instance info fetch claim for \(payload.domain): \(error)")
        }
    }
}

// MARK: - Instance Info Fetching

private let nodeInfoSchemas = [
    "http://nodeinfo.diaspora.software/ns/schema/2.1",
    "http://nodeinfo.diaspora.software/ns/schema/2.0",
]

private func fetchInstanceInfo(domain: String, client: any Client) async throws -> InstanceInfo {
    // Step 1: Discover NodeInfo endpoint via well-known
    let wellKnownURL = URI(string: "https://\(domain)/.well-known/nodeinfo")
    let wellKnownResponse = try await client.get(wellKnownURL) { req in
        req.headers.add(name: .accept, value: "application/json")
        req.timeout = .seconds(InstanceInfoFetchJob.requestTimeoutSeconds)
    }

    guard wellKnownResponse.status == .ok else {
        throw InstanceInfoFetchError.wellKnownFailed(domain, wellKnownResponse.status)
    }

    let wellKnown = try wellKnownResponse.content.decode(NodeInfoWellKnown.self)

    // Step 2: Find the best NodeInfo link (prefer 2.1, then 2.0)
    guard let link = nodeInfoSchemas.lazy.compactMap({ schema in
        wellKnown.links.first { $0.rel == schema }
    }).first else {
        throw InstanceInfoFetchError.noSupportedSchema(domain)
    }

    // Step 3: Fetch the NodeInfo document
    let nodeInfoURL = URI(string: link.href)
    let nodeInfoResponse = try await client.get(nodeInfoURL) { req in
        req.headers.add(name: .accept, value: "application/json")
        req.timeout = .seconds(InstanceInfoFetchJob.requestTimeoutSeconds)
    }

    guard nodeInfoResponse.status == .ok else {
        throw InstanceInfoFetchError.nodeInfoFailed(domain, nodeInfoResponse.status)
    }

    let nodeInfo = try nodeInfoResponse.content.decode(NodeInfoResponse.self)

    let safeStaffAccounts = nodeInfo.metadata["staffAccounts"]?.array?
        .compactMap(\.string)
        .filter { uri in
            guard let colonIndex = uri.firstIndex(of: ":") else { return false }
            return allowedSchemes.contains(uri[..<colonIndex].lowercased())
        }

    // Step 4: Fetch favicon URL from the instance homepage
    let faviconURL = await fetchFaviconURL(domain: domain, client: client)

    return InstanceInfo(
        softwareName: nodeInfo.software.name,
        softwareVersion: nodeInfo.software.version,
        openRegistrations: nodeInfo.openRegistrations,
        staffAccounts: safeStaffAccounts,
        faviconURL: faviconURL,
        isReachable: true,
        lastCheckedAt: Date(),
        consecutiveFailures: 0,
        nextAttemptAt: nil
    )
}

// MARK: - Favicon Fetching

private func fetchFaviconURL(domain: String, client: any Client) async -> String? {
    do {
        let homepageURL = URI(string: "https://\(domain)/")

        let response = try await client.get(homepageURL) { req in
            req.headers.add(name: .accept, value: "text/html")
            req.timeout = .seconds(InstanceInfoFetchJob.requestTimeoutSeconds)
        }

        guard response.status == .ok else { return nil }

        let contentType = response.headers.first(name: .contentType) ?? ""
        guard contentType.contains("text/html") else { return nil }

        guard let body = response.body,
              let html = body.getString(at: body.readerIndex, length: min(body.readableBytes, 32_768))
        else { return nil }

        guard let baseURL = URL(string: "https://\(domain)/") else { return nil }
        return extractFaviconURL(fromHTML: html, baseURL: baseURL)
    } catch {
        return nil
    }
}

// MARK: - Staff Account URI Filtering

private let allowedSchemes: Set<String> = [
    "https", "http", "mailto", "xmpp", "matrix", "tel",
]

private enum InstanceInfoFetchError: Error, CustomStringConvertible {
    case wellKnownFailed(String, HTTPResponseStatus)
    case noSupportedSchema(String)
    case nodeInfoFailed(String, HTTPResponseStatus)

    var description: String {
        switch self {
        case .wellKnownFailed(let domain, let status):
            return "Well-known fetch failed for \(domain): \(status)"
        case .noSupportedSchema(let domain):
            return "No supported NodeInfo schema for \(domain)"
        case .nodeInfoFailed(let domain, let status):
            return "NodeInfo fetch failed for \(domain): \(status)"
        }
    }
}
