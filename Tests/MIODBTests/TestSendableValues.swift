//
//  TestSendableValues.swift
//  MIODBTests
//

import XCTest
import Foundation
@testable import MIODB

/// Result set over canned `Any` cells, standing in for a backend.
private final class CannedResultSet : MDBResultSet
{
    let rows: [[Any?]]

    init ( columns: [String], rows: [[Any?]] ) {
        self.rows = rows
        super.init( columns: columns, rowCount: rows.count, affectedRowCount: 0 )
    }

    override func isNull ( row: Int, col: Int ) -> Bool { rows[ row ][ col ] is NSNull }
    override func value ( row: Int, col: Int ) throws -> Any? { rows[ row ][ col ] }
}

final class TestSendableValues : XCTestCase
{
    func testScalarsKeepTheirType ( ) throws {
        let date = Date( timeIntervalSince1970: 1_000 )
        let uuid = UUID()
        let data = Data( [ 1, 2, 3 ] )

        XCTAssertEqual( try MDBSendable.value( true ) as? Bool, true )
        XCTAssertEqual( try MDBSendable.value( Int16( 7 ) ) as? Int16, 7 )
        XCTAssertEqual( try MDBSendable.value( Int32( 7 ) ) as? Int32, 7 )
        XCTAssertEqual( try MDBSendable.value( Int64( 7 ) ) as? Int64, 7 )
        XCTAssertEqual( try MDBSendable.value( Decimal( string: "1.5" )! ) as? Decimal, Decimal( string: "1.5" ) )
        XCTAssertEqual( try MDBSendable.value( "x" ) as? String, "x" )
        XCTAssertEqual( try MDBSendable.value( date ) as? Date, date )
        XCTAssertEqual( try MDBSendable.value( uuid ) as? UUID, uuid )
        XCTAssertEqual( try MDBSendable.value( data ) as? Data, data )
        XCTAssertTrue( try MDBSendable.value( NSNull() ) is NSNull )
        XCTAssertNil( try MDBSendable.value( nil as Any? ) )
    }

    func testJSONObjectsAreRebuiltRecursively ( ) throws {
        let json = "{\"flag\":true,\"count\":1,\"ratio\":1.5,\"name\":\"n\",\"none\":null,\"list\":[1,[true],{\"k\":\"v\"}]}"
        let obj = try JSONSerialization.jsonObject( with: json.data( using: .utf8 )!, options: [] )

        let dict = try XCTUnwrap( try MDBSendable.value( obj ) as? [String: any Sendable] )

        // JSON `true` and `1` must not collapse into each other on Darwin,
        // where NSNumber bridges to both Bool and Int.
        XCTAssertEqual( dict[ "flag" ] as? Bool, true )
        XCTAssertTrue( dict[ "count" ] is Int )
        XCTAssertEqual( dict[ "count" ] as? Int, 1 )
        XCTAssertEqual( dict[ "ratio" ] as? Double, 1.5 )
        XCTAssertEqual( dict[ "name" ] as? String, "n" )
        XCTAssertTrue( dict[ "none" ] is NSNull )

        let list = try XCTUnwrap( dict[ "list" ] as? [any Sendable] )
        XCTAssertEqual( list.count, 3 )
        XCTAssertEqual( list[ 0 ] as? Int, 1 )
        XCTAssertEqual( ( list[ 1 ] as? [any Sendable] )?.first as? Bool, true )
        XCTAssertEqual( ( list[ 2 ] as? [String: any Sendable] )?[ "k" ] as? String, "v" )
    }

    func testNonSendableValueThrows ( ) {
        final class Opaque { }
        XCTAssertThrowsError( try MDBSendable.value( Opaque() ) ) { error in
            guard case MDBError.notSendable( let name ) = error else { return XCTFail( "Expected notSendable, got \(error)" ) }
            XCTAssertEqual( name, "Opaque" )
        }
    }

    func testRowAndResultSetAccessors ( ) throws {
        let json = try JSONSerialization.jsonObject( with: "{\"a\":[1,2]}".data( using: .utf8 )!, options: [] )
        let rs = CannedResultSet( columns: [ "id", "payload", "gone" ],
                                  rows: [ [ Int64( 1 ), json, NSNull() ],
                                          [ Int64( 2 ), "plain", nil ] ] )

        let row = rs[ 0 ]
        XCTAssertEqual( try row.sendableValue( "id" ) as? Int64, 1 )
        XCTAssertTrue( try row.sendableValue( "gone" ) is NSNull )
        XCTAssertNil( try row.sendableValue( "missing" ) )

        let d0 = try row.sendableDictionary()
        XCTAssertEqual( Set( d0.keys ), [ "id", "payload", "gone" ] )
        let payload = try XCTUnwrap( d0[ "payload" ] as? [String: any Sendable] )
        XCTAssertEqual( payload[ "a" ] as? [Int], [ 1, 2 ] )

        let all = try rs.sendableDictionaries()
        XCTAssertEqual( all.count, 2 )
        XCTAssertEqual( all[ 1 ][ "payload" ] as? String, "plain" )
        XCTAssertNil( all[ 1 ][ "gone" ], "a nil cell is skipped like the legacy dictionaries()" )

        // The whole shape is accepted where `Sendable` is required.
        func needsSendable ( _ v: any Sendable ) { }
        needsSendable( all )
    }
}
