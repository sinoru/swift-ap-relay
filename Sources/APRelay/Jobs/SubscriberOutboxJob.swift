import Foundation
import Queues
import Vapor

/// Delivers subscriber notifications whose writer did not queue them.
///
/// A request delivers the notifications it recorded right after its write
/// commits. One that failed to queue a job, or crashed first, leaves the
/// entry in the outbox; once the entry's lease lapses this job claims it and
/// queues the job. Scheduled jobs run on every replica, and claiming leases
/// each entry to one of them.
struct SubscriberOutboxJob: AsyncScheduledJob {
    /// Upper bound on batches per run, so a run cannot spin on entries that
    /// keep failing; whatever is left is picked up by the next run.
    static let maxBatchesPerRun = 20

    func run(context: QueueContext) async throws {
        let app = context.application
        let repository = app.repository
        let policy = app.subscriberOutboxPolicy
        let queue = app.queues.queue(.default)

        for _ in 0..<Self.maxBatchesPerRun {
            let entries = try await repository.claimOutboxEntries(
                limit: policy.batchSize,
                leaseSeconds: policy.leaseSeconds
            )
            guard !entries.isEmpty else { return }
            context.logger.info("Delivering \(entries.count) pending subscriber notifications")
            await SubscriberOutboxDelivery.deliver(
                entries, repository: repository, queue: queue, logger: context.logger
            )
            guard entries.count == policy.batchSize else { return }
        }
    }
}
