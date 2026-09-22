//
//  TestValueStorage.swift
//  MIODBTests
//

import XCTest
import MIODB

class TestValueStorage: XCTestCase
{
    /// Runs `body` with `NSTimeZone.default` forced to `identifier` — the renderer emits
    /// the LOCAL wall clock (see MDBTimestamp), so every absolute expectation needs a
    /// pinned zone or it only holds on machines that happen to run in it.
    private func withTimeZone ( _ identifier: String, _ body: () throws -> Void ) rethrows {
        let saved = NSTimeZone.default
        NSTimeZone.default = TimeZone( identifier: identifier )!
        defer { NSTimeZone.default = saved }
        try body()
    }

    func testBytesRenderAsPostgreSQLHexDecode ( ) throws {
        let v = try MDBValue( Data( [ 0x01, 0xAB, 0xFF ] ) )
        guard case .bytes( let d ) = v.storage else { return XCTFail( "Data must be stored as .bytes, got \(v.storage)" ) }
        XCTAssertEqual( d, Data( [ 0x01, 0xAB, 0xFF ] ) )
        XCTAssertEqual( v.value, "decode('01abff','hex')" )
        XCTAssertEqual( try MDBValue( Data() ).value, "decode('','hex')" )
        XCTAssertEqual( try MDBValue( [ Data( [ 1 ] ), Data( [ 2 ] ) ] ).value, "(decode('01','hex'),decode('02','hex'))" )
    }

    func testTimestampRender ( ) throws {
        // The renderer emits the wall clock of the process zone: the Calendar
        // reference must agree with it in ANY zone, not just UTC.
        for tzID in [ "UTC", "Europe/Madrid", "Asia/Dubai" ] {
            try withTimeZone( tzID ) {
                var cal = Calendar( identifier: .gregorian )
                cal.timeZone = NSTimeZone.default

                let samples: [TimeInterval] = [ 0                        // epoch
                                              , 1_700_000_000.123456     // recent, with microseconds
                                              , 951_827_696              // 2000-02-29 (leap day)
                                              , -86_400                  // 1969-12-31 (pre-epoch)
                                              , 4_102_444_799.999999     // 2099-12-31 23:59:59.999999
                                              ]

                for t in samples {
                    let date = Date( timeIntervalSince1970: t )
                    let s = MDBSQLTimestampString( date )
                    let c = cal.dateComponents( [.year,.month,.day,.hour,.minute,.second,.nanosecond], from: date )
                    let expected = String( format: "%04d-%02d-%02d %02d:%02d:%02d",
                                           c.year!, c.month!, c.day!, c.hour!, c.minute!, c.second! )
                    XCTAssertEqual( String( s.prefix( 19 ) ), expected, "timestamp \(t) in \(tzID)" )
                    XCTAssertEqual( s.count, 26, "fixed width with microseconds: \(s)" )
                }
            }
        }

        // exact digits: UTC wall under UTC, and the shifted wall clock elsewhere
        withTimeZone( "UTC" ) {
            XCTAssertEqual( MDBSQLTimestampString( Date( timeIntervalSince1970: 1_700_000_000.123456 ) ),
                            "2023-11-14 22:13:20.123456" )
            XCTAssertEqual( MDBSQLTimestampString( Date( timeIntervalSince1970: 0 ) ),
                            "1970-01-01 00:00:00.000000" )
        }
        withTimeZone( "Europe/Madrid" ) {   // November = CET, +01:00
            XCTAssertEqual( MDBSQLTimestampString( Date( timeIntervalSince1970: 1_700_000_000.123456 ) ),
                            "2023-11-14 23:13:20.123456" )
        }
    }

    func testDateValueRender ( ) throws {
        try withTimeZone( "UTC" ) {
            let v = try MDBValue.fromValue( Date( timeIntervalSince1970: 1_700_000_000 ) )
            XCTAssertEqual( v.value, "'2023-11-14 22:13:20.000000'" )
        }
    }

    func testJSONEscaping ( ) throws {
        // apostrophes inside JSON must be escaped exactly once when rendered
        let v = try MDBValue.fromValue( ["name": "L'Oréal"] )
        XCTAssertEqual( v.value, "'{\"name\":\"L''Oréal\"}'" )

        if case .json( let j ) = v.storage {
            XCTAssertEqual( j, "{\"name\":\"L'Oréal\"}", "storage keeps the unescaped JSON" )
        } else {
            XCTFail( "expected .json storage" )
        }
    }

    func testStorageAccess ( ) throws {
        if case .string( let s ) = try MDBValue.fromValue( "it's" ).storage { XCTAssertEqual( s, "it's" ) }
        else { XCTFail( "expected .string" ) }

        if case .int( let i ) = try MDBValue.fromValue( Int8( 7 ) ).storage { XCTAssertEqual( i, 7 ) }
        else { XCTFail( "expected .int" ) }

        if case .null = try MDBValue.fromValue( nil ).storage { }
        else { XCTFail( "expected .null" ) }

        // NSNull is what MDBResultSet returns for SQL NULL, so a fetched row
        // must be usable as update/insert values without any filtering.
        if case .null = try MDBValue.fromValue( NSNull() ).storage { }
        else { XCTFail( "expected .null for NSNull" ) }

        if case .raw( let r ) = MDBValue( raw: "now()" ).storage { XCTAssertEqual( r, "now()" ) }
        else { XCTFail( "expected .raw" ) }
    }

    func testRenderIsCachedAndStable ( ) throws {
        let v = try MDBValue.fromValue( "a'b" )
        XCTAssertEqual( v.value, "'a''b'" )
        XCTAssertEqual( v.value, "'a''b'", "second access identical" )
    }
}
