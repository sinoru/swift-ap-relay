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

    /// Deletes `key` only while it still holds `value`, in one atomic step.
    ///
    /// Releases a claim without touching one that expired and was taken by
    /// another holder in the meantime.
    func delete(_ key: RedisKey, ifEqualTo value: String) async throws {
        _ = try await evaluate(compareAndDeleteScript, keys: [key], arguments: [value])
    }
}

/// KEYS: [1] key. ARGV: [1] expected value.
private let compareAndDeleteScript = """
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('DEL', KEYS[1])
    end
    return 0
    """
