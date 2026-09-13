// swift-tools-version: 6.3

import PackageDescription

let package = Package(
    name: "APRelay",
    platforms: [
        .macOS(.v15),
    ],
    dependencies: [
        .package(url: "https://github.com/vapor/vapor.git", from: "4.99.0"),
        .package(url: "https://github.com/vapor/queues-redis-driver.git", from: "1.1.0"),
        .package(url: "https://github.com/vapor/queues.git", from: "1.12.0"),
        .package(url: "https://github.com/apple/swift-crypto.git", "3.0.0" ..< "5.0.0"),
        .package(url: "https://github.com/swift-server/swift-prometheus.git", from: "2.0.0"),
        .package(url: "https://github.com/vapor/leaf.git", from: "4.4.0"),
        .package(url: "https://github.com/sinoru/swift-json.git", from: "0.2.0"),
        .package(url: "https://github.com/scinfu/SwiftSoup.git", from: "2.13.4"),
        .package(
            url: "https://github.com/sinoru/swift-synchronization-kit.git",
            from: "1.0.1",
            traits: ["RWLock"]
        ),
    ],
    targets: [
        .plugin(
            name: "VersionGeneratorPlugin",
            capability: .buildTool()
        ),
        .target(
            name: "APRelayCore",
            dependencies: [
                .product(name: "Crypto", package: "swift-crypto"),
                .product(name: "_CryptoExtras", package: "swift-crypto"),
            ],
            swiftSettings: [.strictMemorySafety()]
        ),
        .executableTarget(
            name: "APRelay",
            dependencies: [
                "APRelayCore",
                .product(name: "Vapor", package: "vapor"),
                .product(name: "QueuesRedisDriver", package: "queues-redis-driver"),
                .product(name: "Prometheus", package: "swift-prometheus"),
                .product(name: "Leaf", package: "leaf"),
                .product(name: "JSON", package: "swift-json"),
                .product(name: "SynchronizationKit", package: "swift-synchronization-kit"),
            ],
            swiftSettings: [.strictMemorySafety()],
            plugins: [
                .plugin(name: "VersionGeneratorPlugin"),
            ]
        ),
        .testTarget(
            name: "APRelayCoreTests",
            dependencies: ["APRelayCore"],
            swiftSettings: [.strictMemorySafety()]
        ),
        .testTarget(
            name: "APRelayTests",
            dependencies: [
                "APRelay",
                "SwiftSoup",
                .product(name: "VaporTesting", package: "vapor"),
                .product(name: "XCTQueues", package: "queues"),
            ],
            swiftSettings: [.strictMemorySafety()]
        ),
    ],
    swiftLanguageModes: [.v6]
)
