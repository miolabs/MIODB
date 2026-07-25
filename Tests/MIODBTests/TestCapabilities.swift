//
//  TestCapabilities.swift
//  MIODBTests
//
//  Created by Javier Segura Perez on 25/07/2026.
//  Copyright © 2026 Javier Segura Perez. All rights reserved.
//

import XCTest
@testable import MIODB

/// A minimal embedded backend: no credentials, no schemes. Compiling and
/// running against the base API is the regression test that the capability
/// split keeps `MIODB`/`MDBConnection` usable without network concepts.
fileprivate class EmbeddedDB : MIODB
{
    var connected = false
    override func connect( _ to_db: String? = nil ) throws {
        connected = true
        try super.connect( to_db )
    }
    override func disconnect() {
        connected = false
        super.disconnect()
    }
}

fileprivate class NetworkDB : MIONetworkDB {}

class TestCapabilities: XCTestCase
{
    func testEmbeddedBackendHasNoCapabilities() throws {
        let db = EmbeddedDB( database: "/tmp/test.sqlite" )
        XCTAssertNil( db as? MDBCredentials )
        XCTAssertNil( db as? MDBSchemes )

        try db.connect()
        XCTAssertTrue( db.connected )
        XCTAssertEqual( db.database, "/tmp/test.sqlite" )
    }

    func testNetworkBackendExposesCapabilities() throws {
        let db = NetworkDB( host: "localhost", port: 5432, user: "u", password: "p", database: "db", scheme: "venue_a" )

        let creds = try XCTUnwrap( db as? MDBCredentials )
        XCTAssertEqual( creds.host, "localhost" )
        XCTAssertEqual( creds.port, 5432 )
        XCTAssertEqual( creds.user, "u" )
        XCTAssertEqual( creds.password, "p" )

        let schemed = try XCTUnwrap( db as? MDBSchemes )
        XCTAssertEqual( schemed.scheme, "venue_a" )
        try schemed.changeScheme( "venue_b" )
        XCTAssertEqual( db.scheme, "venue_b" )
    }

    func testNetworkConnectionExposesCredentials() {
        let conn = MDBNetworkConnection( host: "localhost", user: "u", password: "p" )
        XCTAssertNotNil( conn as? MDBCredentials )
    }
}
