//
//  TestValueStorage.swift
//  MIODBTests
//

import XCTest
import MIODB

class TestValueStorage: XCTestCase
{
    func testTimestampRender ( ) throws {
        // reference rendering via Calendar/UTC for a spread of dates
        var cal = Calendar( identifier: .gregorian )
        cal.timeZone = TimeZone( identifier: "UTC" )!

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
            XCTAssertEqual( String( s.prefix( 19 ) ), expected, "timestamp \(t)" )
            XCTAssertEqual( s.count, 26, "fixed width with microseconds: \(s)" )
        }

        // exact microsecond digits
        XCTAssertEqual( MDBSQLTimestampString( Date( timeIntervalSince1970: 1_700_000_000.123456 ) ),
                        "2023-11-14 22:13:20.123456" )
        XCTAssertEqual( MDBSQLTimestampString( Date( timeIntervalSince1970: 0 ) ),
                        "1970-01-01 00:00:00.000000" )
    }

    func testDateValueRender ( ) throws {
        let v = try MDBValue.fromValue( Date( timeIntervalSince1970: 1_700_000_000 ) )
        XCTAssertEqual( v.value, "'2023-11-14 22:13:20.000000'" )
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
