//
//  TestSelectForUpdate.swift
//  MIODBTests
//
//  Covers the variadic-splat bug: select_for_update passed its [Any] into the
//  variadic select as a single element, so `field as! String` trapped on an array
//  and every call with at least one argument killed the process.
//

import XCTest

import MIODB


class TestSelectForUpdate: XCTestCase
{
    /// The regression itself. Before the array overload existed this trapped rather
    /// than failing an assertion, so a crash here is the bug returning.
    func testSelectForUpdateWithFieldsDoesNotTrap ( ) throws {
        let query = MDBQuery( "user" ).select_for_update( "id", "name" )

        XCTAssertEqual( query.selectFieldsRaw( ), "\"id\",\"name\"" )
    }

    func testSelectForUpdateWithNoFieldsSelectsEverything ( ) throws {
        let query = MDBQuery( "user" ).select_for_update( )

        XCTAssertEqual( query.selectFieldsRaw( ), "*" )
    }

    /// The array overload must behave identically to the variadic one, since
    /// select_for_update now routes through it.
    func testArrayAndVariadicSelectAgree ( ) throws {
        let variadic = MDBQuery( "user" ).select( "id", "name" )
        let array    = MDBQuery( "user" ).select( [ "id", "name" ] as [Any] )

        XCTAssertEqual( variadic.selectFieldsRaw( ), array.selectFieldsRaw( ) )
    }

    /// Guards the overload resolution rather than the output: if `select( args )`
    /// inside the variadic overload ever bound back to itself, this would recurse
    /// until the stack ran out instead of returning.
    func testVariadicSelectDoesNotRecurse ( ) throws {
        let query = MDBQuery( "user" ).select( "id" )

        XCTAssertEqual( query.selectFieldsRaw( ), "\"id\"" )
    }

    /// The exact shape the live callers use — DualLinkServerKit's EntityQuery calls
    /// select_for_update with one String, three times over. That is what was crashing.
    func testSelectForUpdateWithASingleFieldMatchesProductionCallers ( ) throws {
        let joined = MDBQuery( "entity" ).select_for_update( "id,name,updated_at" )
        XCTAssertEqual( joined.selectFieldsRaw( ), "\"id\",\"name\",\"updated_at\"" )

        let aliased = MDBQuery( "entity" ).select_for_update( "a.id AS identifier" )
        XCTAssertEqual( aliased.selectFieldsRaw( ), "\"a\".\"id\" AS \"identifier\"" )
    }

    func testSelectForUpdateAcceptsAnArrayDirectly ( ) throws {
        let query = MDBQuery( "user" ).select_for_update( [ "id" ] as [Any] )

        XCTAssertEqual( query.selectFieldsRaw( ), "\"id\"" )
    }
}
