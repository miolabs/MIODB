// swift-tools-version:5.9
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "MIODB",
    platforms: [
        .macOS(.v12),
    ],
    products: [
        // Products define the executables and libraries produced by a package, and make them visible to other packages.
        .library(
            name: "MIODB",
            targets: ["MIODB"]),
    ],
    dependencies: [
        // Dependencies declare other packages that this package depends on.
        .package(url: "https://github.com/miolabs/MIOCore.git", branch: "master" ),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages which this package depends on.
        .target(
            name: "MIODB",
            dependencies: [
                .product(name: "MIOCore", package: "MIOCore"),
                .product(name: "MIOCoreLogger", package: "MIOCore"),
            ]),
        .testTarget(
            name: "MIODBTests",
            dependencies: ["MIODB"]),
    ]
)
