import NIOCore
import Queues
@testable import APRelay

/// A queue whose every write fails, as a queue would while Redis is
/// unreachable.
struct FailingQueue: Queue {
    struct Unavailable: Error {}

    let context: QueueContext

    func get(_ id: JobIdentifier) -> EventLoopFuture<JobData> {
        context.eventLoop.makeFailedFuture(Unavailable())
    }

    func set(_ id: JobIdentifier, to data: JobData) -> EventLoopFuture<Void> {
        context.eventLoop.makeFailedFuture(Unavailable())
    }

    func clear(_ id: JobIdentifier) -> EventLoopFuture<Void> {
        context.eventLoop.makeFailedFuture(Unavailable())
    }

    func pop() -> EventLoopFuture<JobIdentifier?> {
        context.eventLoop.makeFailedFuture(Unavailable())
    }

    func push(_ id: JobIdentifier) -> EventLoopFuture<Void> {
        context.eventLoop.makeFailedFuture(Unavailable())
    }
}
