import APRelayCore
import Vapor
@testable import APRelay

/// Holds fetches until the test opens the gate, so two requests can be made
/// to overlap on the same URL. Records fetches like ``URLKeyedActorFetcher``.
struct GatedActorFetcher: ActorFetcher {
    let documents: [String: RemoteActor]
    let log = ActorFetchLog()
    let gate = FetchGate()

    func fetchActor(url: String, client: Client) async throws -> RemoteActor {
        await log.record(url)
        await gate.wait()
        guard let document = documents[url] else {
            throw Abort(.badGateway, reason: "Mock: no document at \(url)")
        }
        return document
    }
}

/// A gate that suspends callers until opened.
actor FetchGate {
    private var isOpen = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func wait() async {
        if isOpen { return }
        await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }

    func open() {
        isOpen = true
        for waiter in waiters {
            waiter.resume()
        }
        waiters.removeAll()
    }
}
