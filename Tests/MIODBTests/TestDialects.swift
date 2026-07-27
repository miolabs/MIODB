//
//  TestDialects.swift
//  MIODBTests
//
//  Created by Javier Segura Perez on 27/07/2026.
//  Copyright © 2026 Javier Segura Perez. All rights reserved.
//

import XCTest
import MIODB

/// A dialect exercising every override point: the SQLite-style adaptations
/// (drop FOR UPDATE, ILIKE -> LIKE), an Oracle-style boolean literal, and
/// unsupported constructs that throw.
private class TestDialect : MDBDialect
{
    override func forUpdateClause ( ) throws -> String { return "" }

    override func whereOperator ( _ op: WHERE_LINE_OPERATOR ) throws -> String {
        switch op {
        case .ILIKE:          return "LIKE"
        case .JSON_EXISTS_IN: throw MDBError.unsupported( "?|", "test" )
        default:              return op.rawValue
        }
    }

    override func renderValue ( _ v: MDBValue ) -> String {
        if case .bool( let b ) = v.storage { return b ? "1" : "0" }
        return super.renderValue( v )
    }

    override func returningClause ( _ q: MDBQuery ) throws -> String {
        let clause = try super.returningClause( q )
        if !clause.isEmpty { throw MDBError.unsupported( "RETURNING", "test" ) }
        return clause
    }
}

class TestDialects: XCTestCase
{
    fileprivate let dialect = TestDialect()

    // The default dialect must render exactly what rawQuery() always produced.
    func testAnsiIsTheDefault ( ) throws {
        let queries: [MDBQuery] = [
            MDBQuery( "product" ).select( "name", "price" ).orderBy( "name", .DESC ).limit( 5 ).offset( 10 ),
            try MDBQuery( "product" ).select().andWhere( "price", .GT, 100 ).beginGroup().andWhere( "enabled", true ).orWhere( "stock", 0 ).endGroup(),
            try MDBQuery( "product" ).select().join( table: "category", to: "category" ),
            try MDBQuery( "product" ).insert( [ "name": "Beer", "enabled": true ] ).returning( "id" ),
            try MDBQuery( "product" ).insert( [ [ "name": "A" ], [ "name": "B" ] ] ),
            try MDBQuery( "product" ).update( [ "price": 5, "date": Date( timeIntervalSince1970: 0 ) ] ).andWhere( "id", 1 ),
            try MDBQuery( "product" ).update( [ [ "id": 1, "price": 5 ], [ "id": 2, "price": 6 ] ], [ "id" ] ),
            try MDBQuery( "product" ).upsert( [ "id": 1, "name": "A" ], "id" ),
            try MDBQuery( "product" ).upsert( [ [ "id": 1, "name": "A" ], [ "id": 2, "name": "B" ] ], "id" ),
            MDBQuery( "product" ).delete(),
        ]

        for q in queries {
            XCTAssertEqual( try q.rawQuery( dialect: .ansi ), q.rawQuery() )
        }
    }

    func testForUpdateOverride ( ) throws {
        let q = MDBQuery( "product" ).select_for_update()
        XCTAssertTrue( q.rawQuery().hasSuffix( " FOR UPDATE" ), q.rawQuery() )
        XCTAssertEqual( try q.rawQuery( dialect: dialect ), "SELECT * FROM \"product\"" )
    }

    func testWhereOperatorOverride ( ) throws {
        let q = try MDBQuery( "product" ).select().andWhere( "name", .ILIKE, "beer%" )
        XCTAssertEqual( q.rawQuery(), "SELECT * FROM \"product\" WHERE \"name\" ILIKE 'beer%'" )
        XCTAssertEqual( try q.rawQuery( dialect: dialect ), "SELECT * FROM \"product\" WHERE \"name\" LIKE 'beer%'" )
    }

    // WHERE values are typed (MDBValue), so a dialect can render the literal
    // its own way — the Oracle boolean case.
    func testValueRenderOverride ( ) throws {
        let q = try MDBQuery( "product" ).select().andWhere( "enabled", true )
        XCTAssertEqual( q.rawQuery(), "SELECT * FROM \"product\" WHERE \"enabled\" = TRUE" )
        XCTAssertEqual( try q.rawQuery( dialect: dialect ), "SELECT * FROM \"product\" WHERE \"enabled\" = 1" )

        let u = try MDBQuery( "product" ).update( [ "enabled": false ] )
        XCTAssertEqual( u.rawQuery(), "UPDATE \"product\" SET \"enabled\"=FALSE" )
        XCTAssertEqual( try u.rawQuery( dialect: dialect ), "UPDATE \"product\" SET \"enabled\"=0" )
    }

    // ILIKE mapping happens on the operator, never inside string literals.
    func testOperatorMappingDoesNotTouchLiterals ( ) throws {
        let q = try MDBQuery( "product" ).select().andWhere( "name", "a ILIKE b" )
        XCTAssertEqual( try q.rawQuery( dialect: dialect ), "SELECT * FROM \"product\" WHERE \"name\" = 'a ILIKE b'" )
    }

    func testUnsupportedConstructsThrow ( ) throws {
        let json = try MDBQuery( "product" ).select().addWhereLine( .AND, "tags", .JSON_EXISTS_IN, [ "a" ] )
        XCTAssertTrue( json.rawQuery().contains( "?|" ) ) // fine on the default dialect
        XCTAssertThrowsError( try json.rawQuery( dialect: dialect ) ) { error in
            guard case MDBError.unsupported = error else { return XCTFail( "expected .unsupported, got \(error)" ) }
        }

        let ret = try MDBQuery( "product" ).insert( [ "id": 1 ] ).returning( "id" )
        XCTAssertThrowsError( try ret.rawQuery( dialect: dialect ) )
    }
}
