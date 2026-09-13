import Vapor

enum SubscriberState: String, CaseIterable, Codable, Sendable {
    case pending
    case accepted
    case rejected
}

struct Subscriber: Codable, Content, Sendable {
    let domain: String
    var inboxURL: String
    var actorID: String
    var state: SubscriberState
    var followActivityID: String
    var followObjectURI: String?
    var outboundFollowActivityID: String?
    var createdAt: Date?
    var updatedAt: Date?
}
