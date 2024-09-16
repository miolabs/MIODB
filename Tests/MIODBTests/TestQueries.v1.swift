//
//  TestDBHelper.swift
//  DLAPIServerTests
//
//  Created by David Trallero on 01/07/2020.
//

import XCTest

import MIODB

class TestDBHelperV1: XCTestCase
{
    override func setUpWithError() throws {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDownWithError() throws {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }
    
    func testValue ( ) throws {
        XCTAssertTrue( try MDBValue.fromValue(1).value == "1", "1" )
        XCTAssertTrue( try MDBValue.fromValue(true).value == "TRUE", "true" )
        XCTAssertTrue( try MDBValue.fromValue(nil).value == "NULL", "NULL" )
        XCTAssertTrue( try MDBValue.fromValue("hello").value == "'hello'", "'hello'" )
        XCTAssertTrue( try MDBValue.fromValue([10,"hello",true] as [Any]).value == "(10,'hello',TRUE)", "[10,'hello',true]" )
        XCTAssertTrue( try MDBValue.fromValue("don't put damn \' or we have to scape the '").value == "'don''t put damn '' or we have to scape the '''", "'don''t put damn '' or we have to scape the '''" )
    }
    

    func testJoin() throws {  // temporarily removed. See Readme.md
        //let query = try MDBQueryV1( "product" ).select( "*" )
        //                              .join( table: "productCategory", to: "category" )
        //                              .rawQuery( )

        //XCTAssertTrue(
        //     (query == "SELECT * FROM \"product\" INNER JOIN \"productCategory\" ON \"productCategory\".\"id\" = \"product\".\"category\"")
        //   , query )
        // Actual behaviour:
        //              SELECT * FROM "product" INNER JOIN "productCategory" ON "productCategory"."id" = "category"
    }

    func testTwoJoin() throws {  // temporarily removed. See Readme.md
        //let query = try MDBQueryV1( "product" ).select( "*" )
        //                              .join( table: "ProductModifier", to: "productModifier" )
        //                              .join( table: "ProductCategoryModifier", from: "productModifier", to: "productModifierCategory" )
        //                              .rawQuery( )

        //XCTAssertTrue(
        //     (query == "SELECT * FROM \"product\" INNER JOIN \"ProductModifier\" ON \"ProductModifier\".\"id\" = \"product\".\"productModifier\" INNER JOIN \"ProductCategoryModifier\" ON \"ProductCategoryModifier\".\"id\" = \"productModifier\".\"productModifierCategory\"" )
        //   , query )
        // Actual behaviour:
        //             SELECT * FROM "product" INNER JOIN "ProductModifier" ON "ProductModifier"."id" = "productModifier" INNER JOIN
        //        "ProductCategoryModifier" ON "productModifier" = "productModifierCategory"
    }

    func testSelect ( ) throws {
        let query = MDBQueryV1( "product" ).select( ).rawQuery( ) ;
        
        XCTAssert( query == "SELECT * FROM \"product\"", query )

        let query2 = MDBQueryV1( "product" ).select( "name" ).rawQuery( ) ;
        
        XCTAssert( query2 == "SELECT \"name\" FROM \"product\"", query2 )

        let query3 = MDBQueryV1( "product" ).select( "name", "price" ).rawQuery( ) ;
        
        XCTAssert( query3 == "SELECT \"name\",\"price\" FROM \"product\"", query3 )

        // DO NOT USE "as"
        let query4 = MDBQueryV1( "product" ).select( "product.*", "modifier.price AS mod_price" ).rawQuery( ) ;
        
        XCTAssert( query4 == "SELECT \"product\".*,\"modifier\".\"price\" AS \"mod_price\" FROM \"product\"", query4 )
    }


    func testInsert ( ) throws {
        let query = try MDBQueryV1( "product" ).insert( [ "modifier": 10, "str": "Hello" ] ).test()
        let query_str = query.rawQuery( ) ;
        
        XCTAssert( query_str == "INSERT INTO \"product\" (\"modifier\",\"str\") VALUES (10,'Hello')", query_str )

        _ = try query.mergeValues( [ "modifier": 15, "extra": "world" ] )
        let query2_str = query.rawQuery()
        print("QUERY 2: \(query2_str)")
        
        XCTAssert( query2_str == "INSERT INTO \"product\" (\"extra\",\"modifier\",\"str\") VALUES ('world',15,'Hello')", query2_str )

    }

    
    func testUpdate ( ) throws {
        let query = try MDBQueryV1( "product" ).update( [ "modifier": 10.5, "str": "Hello" ] ).test().rawQuery( ) ;
        
        XCTAssert( query == "UPDATE \"product\" SET \"modifier\"=10.5,\"str\"='Hello'", query )
    }

    
    func testSelectWhere ( ) throws {
        let query = try MDBQueryV1( "product" ).select( ).andWhere("name", .LT, "hello world").rawQuery( ) ;
        
        XCTAssert( query == "SELECT * FROM \"product\" WHERE \"name\" < 'hello world'", query )


        let query2 = try MDBQueryV1( "product" ).select( )
                                         .orWhere("name", .LT, "hello world")
                                         .orWhere("price", .GE, 15 )
                                         .rawQuery( ) ;

        XCTAssert( query2 == "SELECT * FROM \"product\" WHERE \"name\" < 'hello world' OR \"price\" >= 15", query2 )

        
        let query2_1 = try MDBQueryV1( "product" ).select( )
                                         .orWhere("name", .LT, "hello world")
                                         .orWhere("price", .GE, 15 )
                                            .limit( 10 )
                                            .offset( 20 )
                                            .rawQuery( ) ;
        
        // SELECT * FROM "product" WHERE "name" < 'hello world' OR "price" >= 15 LIMIT 10 OFFSET 20
        XCTAssert( query2_1 == "SELECT * FROM \"product\" WHERE \"name\" < 'hello world' OR \"price\" >= 15 LIMIT 10 OFFSET 20", query2_1 )

        
        
        let query3 = try MDBQueryV1( "product" ).select( )
                                           .beginGroup()
                                             .orWhere("name", .LT, "hello world")
                                             .orWhere("price", .GE, 15 )
                                           .endGroup()
                                           .beginGroup()
                                             .andWhere( "max", .EQ, 1234 )
                                           .endGroup()
                                         .rawQuery( ) ;
        
        XCTAssert( query3 == "SELECT * FROM \"product\" WHERE (\"name\" < 'hello world' OR \"price\" >= 15) AND (\"max\" = 1234)", query3 )

        
        let query3_1 = try MDBQueryV1( "product" ).select( )
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

        
        let query3_2 = try MDBQueryV1( "product" ).select( )
                                           .beginGroup()
                                             .orWhere("name", .LT, "hello world")
                                             .orWhere("price", .GE, 15 )
                                           .endGroup()
                                           .andWhere( "max", .EQ, 1234 )
                                         .rawQuery( ) ;
        
        XCTAssert( query3_2 == "SELECT * FROM \"product\" WHERE (\"name\" < 'hello world' OR \"price\" >= 15) AND \"max\" = 1234", query3_2 )

        
        let query4 = try MDBQueryV1( "product" ).select( )
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

    func testSelectWhere2 ( ) throws {
      let query1 = try MDBQueryV1( "product" ).select( )
                                         .beginGroup()
                                           .orWhere("a", .LT, "1")
                                           .orWhere("b", .GE, 15 )
                                           .beginGroup()
                                             .orWhere( "c", .GT, 2 )
                                             .andWhere( "d", .LT, 3 )
                                             .beginGroup()
                                               .andWhere( "e", .GT, 4 )
                                               .orWhere( "f", .LT, 5 )
                                             .endGroup()
                                           .endGroup()
                                           .orWhere("name2", .LT, "hello world")
                                           .orWhere("price2", .GE, 15 )
                                         .endGroup()
                                         .rawQuery( ) ;
      XCTAssert( query1 == "SELECT * FROM \"product\" WHERE (\"a\" < '1' OR \"b\" >= 15 OR (\"c\" > 2 AND \"d\" < 3 AND (\"e\" > 4 OR \"f\" < 5)) OR \"name2\" < 'hello world' OR \"price2\" >= 15)", query1 )
    }
    
    func testInsertWhere ( ) throws {
        let query = try MDBQueryV1( "product" ).beginGroup()
                                           .orWhere("name", .LT, "hello world")
                                           .orWhere("price", .GE, 15 )
                                         .endGroup()
                                         .insert( [ "modifier": 10, "str": "Hello" ] ).test()
        let query_str = query.rawQuery( ) ;
        
        XCTAssert( query_str == "INSERT INTO \"product\" (\"modifier\",\"str\") VALUES (10,'Hello') WHERE (\"name\" < 'hello world' OR \"price\" >= 15)", query_str )
    }

    
    func testInsertReturning ( ) throws {
        let query = try MDBQueryV1( "product" ).andWhere("price", 15 )
                                         .returning( "name", "price" )
                                         .insert( [ "modifier": 10, "str": "Hello" ] ).test()
        let query_str = query.rawQuery( ) ;
        
        XCTAssert( query_str == "INSERT INTO \"product\" (\"modifier\",\"str\") VALUES (10,'Hello') WHERE \"price\" = 15 RETURNING \"name\",\"price\"", query_str )
    }

    
    func testMultipleInsertWhere ( ) throws {
        let query = try MDBQueryV1( "product" ).beginGroup()
                                           .orWhere("name", .LT, "hello world")
                                           .orWhere("price", .GE, 15 )
                                         .endGroup()
                                         .insert( [ [ "modifier": 10, "str": "Hello" ], [ "modifier": -4, "str": "World" ] ] )
                                         .test()
        let query_str = query.rawQuery( ) ;
        
        XCTAssert( query_str == "INSERT INTO \"product\" (\"modifier\",\"str\") VALUES (10,'Hello'),(-4,'World') WHERE (\"name\" < 'hello world' OR \"price\" >= 15)", query_str )
    }

    
    func testUpdateWhere ( ) throws {
        let query = try MDBQueryV1( "product" ).beginGroup()
                                           .orWhere("name", .LT, "hello world")
                                           .orWhere("price", .GE, 15 )
                                         .endGroup()
                                         .update( [ "modifier": 10, "str": "Hello" ] )
                                         .test()
        let query_str = query.rawQuery( ) ;
        
        XCTAssert( query_str == "UPDATE \"product\" SET \"modifier\"=10,\"str\"='Hello' WHERE (\"name\" < 'hello world' OR \"price\" >= 15)", query_str )
    }
    
    
    func testJoins ( ) throws {
        let query = try MDBQueryV1( "product" ).join( table: "modifier", to: "product.productModifier" )
                                         .select( ).rawQuery( ) ;
        
        // SELECT * FROM "product" INNER JOIN "modifier" ON "modifier"."id" = "product"."productModifier"
        XCTAssert( query == "SELECT * FROM \"product\" INNER JOIN \"modifier\" ON \"modifier\".\"id\" = \"product\".\"productModifier\"", query )


        let query2 = try MDBQueryV1( "product" ).join( table: "modifier", from: "modifier.identifier", to: "product.productModifier", joinType: .FULL )
                                         .select( ).rawQuery( ) ;
        
        XCTAssert( query2 == "SELECT * FROM \"product\" FULL JOIN \"modifier\" ON \"modifier\".\"identifier\" = \"product\".\"productModifier\"", query2 )
    }

    
     func testOrder ( ) throws {
         let query = MDBQueryV1( "product" ).orderBy( "name", .ASC )
                                          .orderBy( "surname", .DESC )
                                          .select( ).rawQuery( ) ;
         
         // SELECT * FROM "product" INNER JOIN "modifier" ON "modifier"."id" = "product"."productModifier"
         XCTAssert( query == "SELECT * FROM \"product\" ORDER BY \"name\" ASC,\"surname\" DESC", query )
     }

    
    func testDoubleJoin ( ) throws {
        let query = try MDBQueryV1( "product" ).select( ).join(table: "modifier", to: "prod").join(table: "modifier", to: "prod").rawQuery( ) ;
        
        // SELECT * FROM "product" INNER JOIN "modifier" ON "modifier"."id" = "product"."productModifier"
        XCTAssert( query == "SELECT * FROM \"product\" INNER JOIN \"modifier\" ON \"modifier\".\"id\" = \"prod\"", query )
    }

    
    func testGroups ( ) throws {
        let place_entities = ["A","B"]
        let device_entities = ["C","D"]
        let sync_id = 15
        let app_id = "APP_ID"
        let query = try MDBQueryV1( "product" ).select( )
                    .andWhere( "syncID", .GT, sync_id )
                    .beginGroup()
                       .andWhereIN( "classname", place_entities )
                       .beginGroup()
                         .orWhereIN( "classname", device_entities )
                         .andWhere( "appID", app_id )
                       .endGroup()
                    .endGroup()
                    .orderBy("syncID", .ASC)
                    .limit( 100 ).rawQuery( )
        
        // SELECT * FROM "product" INNER JOIN "modifier" ON "modifier"."id" = "product"."productModifier"
        XCTAssert( query == "SELECT * FROM \"product\" WHERE \"syncID\" > 15 AND (\"classname\" IN ('A','B') OR (\"classname\" IN ('C','D') AND \"appID\" = 'APP_ID')) ORDER BY \"syncID\" ASC LIMIT 100", query )
    }

    
    func testUpsertSimple ( ) throws {
        let query = try MDBQueryV1( "product" ).upsert( [ "id": 1, "name": "patata" ], "id" ).test().rawQuery( ) ;
        
        // SELECT * FROM "product" INNER JOIN "modifier" ON "modifier"."id" = "product"."productModifier"
        XCTAssert( query == "INSERT INTO \"product\" (\"id\",\"name\") VALUES (1,'patata') ON CONFLICT (id) DO UPDATE SET \"id\"=1,\"name\"='patata'", query )
    }

    func testUpsertMultiple ( ) throws {
        let query = try MDBQueryV1( "product" ).upsert( [ [ "id": 1, "name": "patata" ], [ "id": 2, "name": "tomate" ] ], "id" ).test().rawQuery( ) ;
        
        // SELECT * FROM "product" INNER JOIN "modifier" ON "modifier"."id" = "product"."productModifier"
        XCTAssert( query == "INSERT INTO \"product\" (\"id\",\"name\") VALUES (1,'patata'),(2,'tomate') ON CONFLICT (id) DO UPDATE SET \"name\" = excluded.\"name\"", query )
    }
    
    
    func testBitwise ( ) throws {
        let query = try MDBQueryV1( "product" ).select( ).addWhereLine(.AND, MDBValue( raw: "field & 1"), .EQ, 0 ).rawQuery( ) ;
        
        XCTAssert( query == "SELECT * FROM \"product\" WHERE field & 1 = 0", query )
    }


    func testMultiInsert ( ) throws {
        do
        {
          _ = try MDBQueryV1( "product" ).insert( [ ["a": 1], ["a": 2, "b": "hello" ] ] )
          XCTFail()
        }
        catch {
           
        }


        do
        {
          _ = try MDBQueryV1( "product" ).insert( [ ["a": 1, "b": 2], ["a": 2 ] ] )
          XCTFail()
        }
        catch {
           
        }

        do
        {
          _ = try MDBQueryV1( "product" ).insert( [ ["a": 1, "b": 2], ["a": 2, "b": 3 ] ] )
        }
        catch {
          XCTFail()
        }

    }
}
