@preconcurrency @unsafe import RediStack

extension RedisClient {
    /// Runs a Lua script with `EVAL`, exposing `keys` as `KEYS` and
    /// `arguments` as `ARGV`.
    ///
    /// A script executes atomically on the server, so multi-key updates that
    /// must not interleave with writes from other replicas go through here.
    func evaluate(
        _ script: String,
        keys: [RedisKey],
        arguments: [String]
    ) async throws -> RESPValue {
        var command: [RESPValue] = [.init(from: script), .init(from: keys.count)]
        command += keys.map { RESPValue(from: $0) }
        command += arguments.map { RESPValue(from: $0) }
        return try await send(command: "EVAL", with: command).get()
    }
}
