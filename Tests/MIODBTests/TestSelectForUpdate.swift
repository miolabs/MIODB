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


    // MARK: - What each argument shape resolves to
    //
    // The overload pair makes an array argument mean "these fields" rather than "one
    // field that is an array". That is a deliberate change of meaning and worth pinning
    // down, because the old behaviour for an array was to trap.

    func testSingleString ( ) throws {
        XCTAssertEqual( MDBQuery( "t" ).select( "id" ).selectFieldsRaw( ), "\"id\"" )
    }

    func testTwoStringsStayInOrder ( ) throws {
        XCTAssertEqual( MDBQuery( "t" ).select( "id", "name" ).selectFieldsRaw( ), "\"id\",\"name\"" )
    }

    /// A `[String]` coerces to `[Any]`, so it binds to the array overload and splats.
    func testStringArraySplatsRatherThanBecomingOneField ( ) throws {
        let fields = [ "id", "name" ]

        XCTAssertEqual( MDBQuery( "t" ).select( fields ).selectFieldsRaw( )
                      , MDBQuery( "t" ).select( "id", "name" ).selectFieldsRaw( ) )
    }

    /// An `[Any]` built at runtime behaves the same — this is the shape that used to trap.
    func testAnyArraySplats ( ) throws {
        let fields:[Any] = [ "id", "name" ]

        XCTAssertEqual( MDBQuery( "t" ).select( fields ).selectFieldsRaw( ), "\"id\",\"name\"" )
    }

    func testMDBValueElementsPassThrough ( ) throws {
        let raw = MDBValue( fromTable: "count(*) AS total" )

        XCTAssertEqual( MDBQuery( "t" ).select( raw ).selectFieldsRaw( )
                      , MDBQuery( "t" ).select( [ raw ] as [Any] ).selectFieldsRaw( ) )
    }

    func testStringsAndMDBValuesMix ( ) throws {
        let raw   = MDBValue( fromTable: "count(*) AS total" )
        let query = MDBQuery( "t" ).select( "id", raw )

        XCTAssertTrue( query.selectFieldsRaw( ).contains( "\"id\"" ) )
        XCTAssertTrue( query.selectFieldsRaw( ).contains( "total" ) )
    }

    /// An Int is a caller bug, but it must not kill the process: `field as! String`
    /// used to trap here. It is logged and stringified, producing a column the database
    /// will reject — recoverable, unlike a crash.
    func testNumberDoesNotTrap ( ) throws {
        let query = MDBQuery( "t" ).select( "id", 42 )

        XCTAssertTrue( query.selectFieldsRaw( ).contains( "\"id\"" ) )
        XCTAssertTrue( query.selectFieldsRaw( ).contains( "42" ) )
    }

    /// A `[MDBValue]` is the other typed array that binds to the variadic overload rather
    /// than to `[Any]`, so it has to splat too.
    func testMDBValueArraySplats ( ) throws {
        let values = [ MDBValue( fromTable: "id" ), MDBValue( fromTable: "name" ) ]

        XCTAssertEqual( MDBQuery( "t" ).select( values ).selectFieldsRaw( ), "\"id\",\"name\"" )
    }

    /// Nesting flattens, for the same reason: the meaning of an array should not depend on
    /// how deep it is or which overload caught it.
    func testNestedArrayFlattens ( ) throws {
        let nested:[Any] = [ [ "id", "name" ], "email" ]

        XCTAssertEqual( MDBQuery( "t" ).select( nested ).selectFieldsRaw( ), "\"id\",\"name\",\"email\"" )
    }

    /// Order is preserved and nothing is dropped, even where the caller passed nonsense.
    func testMixedTypesAreKeptInOrderAndDoNotTrap ( ) throws {
        let mixed:[Any] = [ "id", 42, true ]
        let fields = MDBQuery( "t" ).select( mixed ).selectFieldsRaw( ).split( separator: "," )

        XCTAssertEqual( fields.count, 3 )
        XCTAssertEqual( String( fields[ 0 ] ), "\"id\"" )
        XCTAssertTrue( String( fields[ 1 ] ).contains( "42" ) )
    }
}
