//
//  TestDBHelper.swift
//  DLAPIServerTests
//
//  Created by David Trallero on 01/07/2020.
//

import XCTest

import MIODB

class TestDBHelper: XCTestCase {

    override func setUpWithError() throws {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDownWithError() throws {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }
    
    func testValue ( ) {
        XCTAssertTrue( MDBValue.fromValue(1).value == "1", "1" )
        XCTAssertTrue( MDBValue.fromValue(true).value == "TRUE", "true" )
        XCTAssertTrue( MDBValue.fromValue(nil).value == "NULL", "NULL" )
        XCTAssertTrue( MDBValue.fromValue("hello").value == "'hello'", "'hello'" )
        XCTAssertTrue( MDBValue.fromValue([10,"hello",true]).value == "(10,'hello',TRUE)", "[10,'hello',true]" )
    }
    

    func testJoin() throws {
        let db = MIODB( )
        let query = MDBQuery( db: db ).selectFields( "*" )
                                      .fromTable( "product" )
                                      .join( table: "productCategory", fromTable: "product", column: "category" )
                                      .rawQuery( )

        XCTAssertTrue(
             (query == "SELECT * FROM \"product\" INNER JOIN \"productCategory\" ON \"productCategory\".\"id\" = \"product\".\"category\"")
           , query )
    }

    func testTwoJoin() throws {
        let db = MIODB( )
        let query = MDBQuery( db: db ).selectFields( "*" )
                                      .fromTable( "product" )
                                      .join( table: "ProductModifier", fromTable: "product", column: "productModifier" )
                                      .join( table: "ProductCategoryModifier", fromTable: "productModifier", column: "productModifierCategory" )
                                      .rawQuery( )

        XCTAssertTrue(
             (query == "SELECT * FROM \"product\" INNER JOIN \"ProductModifier\" ON \"ProductModifier\".\"id\" = \"product\".\"productModifier\" INNER JOIN \"ProductCategoryModifier\" ON \"ProductCategoryModifier\".\"id\" = \"productModifier\".\"productModifierCategory\"" )
           , query )
    }

    func testSelect ( ) throws {
        let query = MDBQuery( "product" ).select( ).rawQuery( ) ;
        
        XCTAssert( query == "SELECT * FROM \"product\"", query )

        let query2 = MDBQuery( "product" ).select( "name" ).rawQuery( ) ;
        
        XCTAssert( query2 == "SELECT \"name\" FROM \"product\"", query2 )

        let query3 = MDBQuery( "product" ).select( "name", "price" ).rawQuery( ) ;
        
        XCTAssert( query3 == "SELECT \"name\",\"price\" FROM \"product\"", query3 )

        // DO NOT USE "as"
        let query4 = MDBQuery( "product" ).select( "product.*", "modifier.price AS mod_price" ).rawQuery( ) ;
        
        XCTAssert( query4 == "SELECT \"product\".*,\"modifier\".\"price\" AS \"mod_price\" FROM \"product\"", query4 )
    }


    func testInsert ( ) throws {
        let query = MDBQuery( "product" ).insert( [ "modifier": 10, "str": "Hello" ] )
        let query_str = query.rawQuery( ) ;
        
        XCTAssert( query_str == "INSERT INTO \"product\" (\"modifier\",\"str\") VALUES (10,'Hello')", query_str )

        _ = query.mergeValues( [ "modifier": 15, "extra": "world" ] )
        let query2_str = query.rawQuery()
        
        XCTAssert( query2_str == "INSERT INTO \"product\" (\"extra\",\"modifier\",\"str\") VALUES ('world',15,'Hello')", query2_str )

    }

    
    func testUpdate ( ) throws {
        let query = MDBQuery( "product" ).update( [ "modifier": 10.5, "str": "Hello" ] ).rawQuery( ) ;
        
        XCTAssert( query == "UPDATE \"product\" SET \"modifier\"=10.5,\"str\"='Hello'", query )
    }

    
    func testSelectWhere ( ) throws {
        let query = MDBQuery( "product" ).select( ).andWhere("name", .LT, "hello world").rawQuery( ) ;
        
        XCTAssert( query == "SELECT * FROM \"product\" WHERE \"name\" < 'hello world'", query )


        let query2 = MDBQuery( "product" ).select( )
                                         .orWhere("name", .LT, "hello world")
                                         .orWhere("price", .GE, 15 )
                                         .rawQuery( ) ;

        XCTAssert( query2 == "SELECT * FROM \"product\" WHERE \"name\" < 'hello world' OR \"price\" >= 15", query2 )

        
        let query2_1 = MDBQuery( "product" ).select( )
                                         .orWhere("name", .LT, "hello world")
                                         .orWhere("price", .GE, 15 )
                                            .limit( 10 )
                                            .offset( 20 )
                                            .rawQuery( ) ;
        
        // SELECT * FROM "product" WHERE "name" < 'hello world' OR "price" >= 15 LIMIT 10 OFFSET 20
        XCTAssert( query2_1 == "SELECT * FROM \"product\" WHERE \"name\" < 'hello world' OR \"price\" >= 15 LIMIT 10 OFFSET 20", query2_1 )

        
        
        let query3 = MDBQuery( "product" ).select( )
                                           .beginGroup()
                                             .orWhere("name", .LT, "hello world")
                                             .orWhere("price", .GE, 15 )
                                           .endGroup()
                                           .beginGroup()
                                             .andWhere( "max", .EQ, 1234 )
                                           .endGroup()
                                         .rawQuery( ) ;
        
        XCTAssert( query3 == "SELECT * FROM \"product\" WHERE (\"name\" < 'hello world' OR \"price\" >= 15) AND (\"max\" = 1234)", query3 )

        
        let query3_1 = MDBQuery( "product" ).select( )
                                           .beginGroup()
                                               .beginGroup()
                                                 .orWhere("name", .LT, "hello world")
                                                 .orWhere("price", .GE, 15 )
                                               .endGroup()
                                               .beginGroup()
                                                 .andWhere( "max", .EQ, 1234 )
                                               .endGroup()
                                           .endGroup()
                                           .rawQuery( ) ;
        // SELECT * FROM "product" WHERE (("name" < 'hello world' OR "price" >= 15) AND ("max" = 1234))
        XCTAssert( query3_1 == "SELECT * FROM \"product\" WHERE ((\"name\" < 'hello world' OR \"price\" >= 15) AND (\"max\" = 1234))", query3_1 )

        
        let query3_2 = MDBQuery( "product" ).select( )
                                           .beginGroup()
                                             .orWhere("name", .LT, "hello world")
                                             .orWhere("price", .GE, 15 )
                                           .endGroup()
                                           .andWhere( "max", .EQ, 1234 )
                                         .rawQuery( ) ;
        
        XCTAssert( query3_2 == "SELECT * FROM \"product\" WHERE (\"name\" < 'hello world' OR \"price\" >= 15) AND \"max\" = 1234", query3_2 )

        
        let query4 = MDBQuery( "product" ).select( )
                                         .beginGroup()
                                           .orWhere("name", .LT, "hello world")
                                           .orWhere("price", .GE, 15 )
                                           .beginGroup()
                                             .andWhere( "max", .GT, 1234 )
                                             .andWhere( "max", .LT, 2000 )
                                           .endGroup()
                                           .orWhere("name2", .LT, "hello world")
                                           .orWhere("price2", .GE, 15 )
                                         .endGroup()
                                         .rawQuery( ) ;
        // SELECT * FROM "product" WHERE ("name" < 'hello world' OR "price" >= 15 AND ("max" > 1234 AND "max" < 2000) OR "name2" < 'hello world' OR "price2" >= 15)
        XCTAssert( query4 == "SELECT * FROM \"product\" WHERE (\"name\" < 'hello world' OR \"price\" >= 15 AND (\"max\" > 1234 AND \"max\" < 2000) OR \"name2\" < 'hello world' OR \"price2\" >= 15)", query4 )

    }
    
    
    func testInsertWhere ( ) throws {
        let query = MDBQuery( "product" ).beginGroup()
                                           .orWhere("name", .LT, "hello world")
                                           .orWhere("price", .GE, 15 )
                                         .endGroup()
                                         .insert( [ "modifier": 10, "str": "Hello" ] )
        let query_str = query.rawQuery( ) ;
        
        XCTAssert( query_str == "INSERT INTO \"product\" (\"modifier\",\"str\") VALUES (10,'Hello') WHERE (\"name\" < 'hello world' OR \"price\" >= 15)", query_str )
    }

    
    func testInsertReturning ( ) throws {
        let query = MDBQuery( "product" ).andWhere("price", 15 )
                                         .returning( "name", "price" )
                                         .insert( [ "modifier": 10, "str": "Hello" ] )
        let query_str = query.rawQuery( ) ;
        
        XCTAssert( query_str == "INSERT INTO \"product\" (\"modifier\",\"str\") VALUES (10,'Hello') WHERE \"price\" = 15 RETURNING \"name\",\"price\"", query_str )
    }

    
    func testMultipleInsertWhere ( ) throws {
        let query = MDBQuery( "product" ).beginGroup()
                                           .orWhere("name", .LT, "hello world")
                                           .orWhere("price", .GE, 15 )
                                         .endGroup()
                                         .insert( [ [ "modifier": 10, "str": "Hello" ], [ "modifier": -4, "str": "World" ] ] )
        let query_str = query.rawQuery( ) ;
        
        XCTAssert( query_str == "INSERT INTO \"product\" (\"modifier\",\"str\") VALUES (10,'Hello'),(-4,'World') WHERE (\"name\" < 'hello world' OR \"price\" >= 15)", query_str )
    }

    
    func testUpdateWhere ( ) throws {
        let query = MDBQuery( "product" ).beginGroup()
                                           .orWhere("name", .LT, "hello world")
                                           .orWhere("price", .GE, 15 )
                                         .endGroup()
                                         .update( [ "modifier": 10, "str": "Hello" ] )
        let query_str = query.rawQuery( ) ;
        
        XCTAssert( query_str == "UPDATE \"product\" SET \"modifier\"=10,\"str\"='Hello' WHERE (\"name\" < 'hello world' OR \"price\" >= 15)", query_str )
    }
    
    
    func testJoins ( ) throws {
        let query = MDBQuery( "product" ).join( table: "modifier", to: "product.productModifier" )
                                         .select( ).rawQuery( ) ;
        
        // SELECT * FROM "product" INNER JOIN "modifier" ON "modifier"."id" = "product"."productModifier"
        XCTAssert( query == "SELECT * FROM \"product\" INNER JOIN \"modifier\" ON \"modifier\".\"id\" = \"product\".\"productModifier\"", query )


        let query2 = MDBQuery( "product" ).join( table: "modifier", from: "modifier.identifier", to: "product.productModifier", joinType: .FULL )
                                         .select( ).rawQuery( ) ;
        
        XCTAssert( query2 == "SELECT * FROM \"product\" FULL JOIN \"modifier\" ON \"modifier\".\"identifier\" = \"product\".\"productModifier\"", query2 )
    }

    
     func testOrder ( ) throws {
         let query = MDBQuery( "product" ).orderBy( "name", .ASC )
                                          .orderBy( "surname", .DESC )
                                          .select( ).rawQuery( ) ;
         
         // SELECT * FROM "product" INNER JOIN "modifier" ON "modifier"."id" = "product"."productModifier"
         XCTAssert( query == "SELECT * FROM \"product\" ORDER BY \"name\" ASC,\"surname\" DESC", query )
     }

    
    func testDoubleJoin ( ) throws {
        let query = MDBQuery( "product" ).select( ).join(table: "modifier", to: "prod").join(table: "modifier", to: "prod").rawQuery( ) ;
        
        // SELECT * FROM "product" INNER JOIN "modifier" ON "modifier"."id" = "product"."productModifier"
        XCTAssert( query == "SELECT * FROM \"product\" INNER JOIN \"modifier\" ON \"modifier\".\"id\" = \"prod\"", query )
    }

}
