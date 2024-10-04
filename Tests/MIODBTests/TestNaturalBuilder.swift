
import XCTest
@testable import MIODB

final class TestNaturalBuilder: XCTestCase {

// MARK: - select
    func testSelect_01 ( ) throws {
        var nQuery = try MDBQuery("table") {
            Select( "a", "b", "c" )
            OrderBy("a", .DESC)
            OrderBy("b", .ASC) 
        }
        var fQuery = MDBQuery("table").select("a", "b", "c").orderBy("a", .DESC).orderBy("b", .ASC)
        XCTAssertEqual( MDBQueryEncoderSQL(nQuery).rawQuery(), MDBQueryEncoderSQL(fQuery).rawQuery() )

        nQuery = try MDBQuery("table") {
            Select()
            OrderBy("date", .DESC)
        }
        fQuery = MDBQuery("table").select().orderBy("date", .DESC)
        XCTAssertEqual( MDBQueryEncoderSQL(nQuery).rawQuery(), MDBQueryEncoderSQL(fQuery).rawQuery() )
    }

    func testSelect_02( ) throws {
        let nQuery = try MDBQuery("table") {
            Select( "a", alias("b", "bAlias"), "c" )
        }
        let fQuery = MDBQuery("table").select("a", alias("b", "bAlias"), "c")
        XCTAssertEqual( MDBQueryEncoderSQL(nQuery).rawQuery(), MDBQueryEncoderSQL(fQuery).rawQuery() )
    }

    func testSelect_03( ) throws {
        let nQuery = try MDBQuery("product") {
            Select(alias("name", "n1"), alias("price", "p1"))
            TableAlias("PR1")
        }
        let fQuery = MDBQuery( "product" ).select( alias("name", "n1"), alias("price", "p1")).tableAlias("PR1")
        XCTAssertEqual( MDBQueryEncoderSQL(nQuery).rawQuery(), MDBQueryEncoderSQL(fQuery).rawQuery() )
    }

    func testSelect_04( ) throws {
        let nQuery = try MDBQuery("product") {
            Select("name", "price")
            DistinctOn(["name"])
        }
        let fQuery = MDBQuery( "product" ).select( "name", "price").distinctOn(["name"])
        XCTAssertEqual( MDBQueryEncoderSQL(nQuery).rawQuery(), MDBQueryEncoderSQL(fQuery).rawQuery() )
    }

    func testSelect_05( ) throws {
        let nQuery = try MDBQuery("product") {
            Select("name", "price")
            GroupBy("name")
        }
        let fQuery = MDBQuery( "product" ).select( "name", "price").groupBy("name")
        XCTAssertEqual( MDBQueryEncoderSQL(nQuery).rawQuery(), MDBQueryEncoderSQL(fQuery).rawQuery() )
    }

    func testSelect_06( ) throws {
        let nQuery = try MDBQuery("product") {
            Select("name", "price")
            Limit(10)
            Offset(50)
        }
        let fQuery = MDBQuery( "product" ).select( "name", "price").limit(10).offset(50)
        XCTAssertEqual( MDBQueryEncoderSQL(nQuery).rawQuery(), MDBQueryEncoderSQL(fQuery).rawQuery() )
    }

// MARK: - join

    func testJoin_01 ( ) throws {  
        var nQuery = try MDBQuery("product") {
            Select()
            Join( table: "modifier", to: "product.productModifier" )
        }
        var fQuery = try MDBQuery( "product" ).join( table: "modifier", to: "product.productModifier" ).select( )
        XCTAssertEqual( MDBQueryEncoderSQL(nQuery).rawQuery(), MDBQueryEncoderSQL(fQuery).rawQuery() )

        nQuery = try MDBQuery("product") {
            Select()
            Join( table: "modifier", from: "modifier.identifier", to: "product.productModifier", joinType: .FULL )
        }
        fQuery = try MDBQuery( "product" ).join( table: "modifier", from: "modifier.identifier", to: "product.productModifier", joinType: .FULL )
                                         .select( )
        XCTAssertEqual( MDBQueryEncoderSQL(nQuery).rawQuery(), MDBQueryEncoderSQL(fQuery).rawQuery() )
    }

    func testTwoJoin() throws {
        let nQuery = try MDBQuery("product") {
            Select()
            Join( table: "ProductModifier", to: "productModifier" )
            Join( table: "ProductCategoryModifier", from: "productModifier", to: "productModifierCategory" )
        }
        let fQuery = try MDBQuery( "product" ).select( "*" )
                                     .join( table: "ProductModifier", to: "productModifier" )
                                     .join( table: "ProductCategoryModifier", from: "productModifier", to: "productModifierCategory" )
        XCTAssertEqual( MDBQueryEncoderSQL(nQuery).rawQuery(), MDBQueryEncoderSQL(fQuery).rawQuery() )
    }

    func testDontRepeatJoins ( ) throws {
        let nQuery = try MDBQuery("product") {
            Select()
            Join( table: "modifier", to: "prod" )
            Join( table: "modifier", to: "prod" )
        }
        let fQuery = try MDBQuery( "product" ).select( ).join(table: "modifier", to: "prod").join(table: "modifier", to: "prod")
        XCTAssertEqual( MDBQueryEncoderSQL(nQuery).rawQuery(), MDBQueryEncoderSQL(fQuery).rawQuery() )
    }

    func testTwoJoinAndOrder() throws {
        let nQuery = try MDBQuery("product") {
            Select()
            Join( table: "ProductModifier", to: "productModifier" )
            Join( table: "ProductCategoryModifier", from: "productModifier", to: "productModifierCategory" )
            OrderBy("syncID", .ASC)
            OrderBy("name", .DESC)
        }
        let fQuery = try MDBQuery( "product" ).select( "*" )
                                     .join( table: "ProductModifier", to: "productModifier" )
                                     .join( table: "ProductCategoryModifier", from: "productModifier", to: "productModifierCategory" )
                                     .orderBy("syncID", .ASC)
                                     .orderBy("name", .DESC)

         XCTAssertEqual( MDBQueryEncoderSQL(nQuery).rawQuery(), MDBQueryEncoderSQL(fQuery).rawQuery() )
    }

// MARK: - insert  

    func testInsert_01( ) throws {
        let nQuery = try MDBQuery("product") {
            Insert( [ "modifier": 10, "str": "Hello" ] )
            Test()
        }
        let fQuery = try MDBQuery( "product" ).insert( [ "modifier": 10, "str": "Hello" ] ).test()
        XCTAssertEqual( MDBQueryEncoderSQL(nQuery).rawQuery(), MDBQueryEncoderSQL(fQuery).rawQuery() )
    }

    func testInsertReturning ( ) throws {
        let nQuery = try MDBQuery("product") {
            Insert( [ "modifier": 10, "str": "Hello" ] )
            Returning("name", "price" )
            Test()
        }
        let fQuery = try MDBQuery( "product" )
                                         .returning( "name", "price" )
                                         .insert( [ "modifier": 10, "str": "Hello" ] ).test()
        XCTAssertEqual( MDBQueryEncoderSQL(nQuery).rawQuery(), MDBQueryEncoderSQL(fQuery).rawQuery() )
    }

    func testMultiInsertWhere ( ) throws {
        let nQuery = try MDBQuery("product") {
            Insert( [ [ "modifier": 10, "str": "Hello" ], [ "modifier": -4, "str": "World" ] ] )
            Test()
        }
        let fQuery = try MDBQuery( "product" )
                                         .insert( [ [ "modifier": 10, "str": "Hello" ], [ "modifier": -4, "str": "World" ] ] )
                                         .test()
        XCTAssertEqual( MDBQueryEncoderSQL(nQuery).rawQuery(), MDBQueryEncoderSQL(fQuery).rawQuery() )
     }

// MARK: - update  

    func testUpdate_01( ) throws {
        let nQuery = try MDBQuery("product") {
            Update( [ "modifier": 10.5, "str": "Hello" ] )
            Test()
        }
        let fQuery = try MDBQuery( "product" ).update( [ "modifier": 10.5, "str": "Hello" ] ).test()
        XCTAssertEqual( MDBQueryEncoderSQL(nQuery).rawQuery(), MDBQueryEncoderSQL(fQuery).rawQuery() )
    }

    func testUpdateWhere ( ) throws {
        let nQuery = try MDBQuery("product") {
            Update( [ "modifier": 10, "str": "Hello" ] )
            Where {
                Or {
                    ( "name", .LT, "hello world" )
                    ( "price", .GE, 15 )
                }
            }
            Test()
        }
        let fQuery = try MDBQuery( "product" ).beginGroup()
                                           .orWhere("name", .LT, "hello world")
                                           .orWhere("price", .GE, 15 )
                                         .endGroup()
                                         .update( [ "modifier": 10, "str": "Hello" ] )
                                         .test()
        XCTAssertEqual( MDBQueryEncoderSQL(nQuery).rawQuery(), MDBQueryEncoderSQL(fQuery).rawQuery() )                                         
    }



// MARK: - where
    func testWhere_01 ( ) throws {
        let nQuery = try MDBQuery("table") {
            Select( "a", "b", "c" )
            Where {
                ( "a", .GT, 1 )
            }
        }
        let fQuery = try MDBQuery("table").select("a", "b", "c").where().addCondition( "a", .GT, 1 )
        XCTAssertEqual( MDBQueryEncoderSQL(nQuery).rawQuery(), MDBQueryEncoderSQL(fQuery).rawQuery() )
    }

    func testWhere_02 ( ) throws {
        let nQuery = try MDBQuery("table") {
            Select( "a", "b", "c" )
            Where {
                And {
                    ( "a", .GT, 1 )
                    ( "b", .equal, 2 )
                    Or {
                        ( "c", .LT, 3 )
                        ( "d", .EQ, 4 )
                    }
                }
            }
            OrderBy("a", .DESC)
            OrderBy("b", .ASC) 
        }
        let fQuery = try MDBQuery("table").select("a", "b", "c").where()
                            .beginAndGroup()
                                .addCondition( "a", .GT, 1 )
                                .addCondition( "b", .EQ, 2 )
                                .beginOrGroup()
                                    .addCondition( "c", .LT, 3 )
                                    .addCondition( "d", .EQ, 4 )
                                .endGroup()
                            .endGroup()
                            .orderBy("a", .DESC).orderBy("b", .ASC)
        XCTAssertEqual( MDBQueryEncoderSQL(nQuery).rawQuery(), MDBQueryEncoderSQL(fQuery).rawQuery() )
    }

    func testWhere_03 ( ) throws {
        let key1 = "b"
        let nQuery = try MDBQuery("table") {
            Select( "a", "b", "c" )
            Where {
                And {
                    ("a", .equal, 1)
                    Or {
                        ( "c", .lessThan, 3 )
                        ( key1, .equal, 4 )
                    }
                }
            }
        }
        let fQuery = try MDBQuery("table").select("a", "b", "c")
                    .where()
                        .beginAndGroup()
                            .addCondition( "a", .equal, 1 )
                            .beginOrGroup()
                                    .addCondition( "c", .LT, 3 )
                                    .addCondition( key1, .EQ, 4 )
                                .endGroup()
                        .endGroup()
        XCTAssertEqual( MDBQueryEncoderSQL(nQuery).rawQuery(), MDBQueryEncoderSQL(fQuery).rawQuery() )
    }
}
