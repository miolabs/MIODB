
import XCTest
@testable import MIODB

final class TestNaturalBuilder: XCTestCase {

    func testSelect_01 ( ) throws {
        let nQuery = try MDBQuery("table") {
            Select( "a", "b", "c" )
            OrderBy("a", .DESC)
            OrderBy("b", .ASC) 
        }
        let fQuery = MDBQuery("table").select("a", "b", "c").orderBy("a", .DESC).orderBy("b", .ASC)
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

    func testWhere_01 ( ) throws {
        let nQuery = try MDBQuery("table") {
            Select( "a", "b", "c" )
            Where {
                Condition( "a", .GT, 1 )
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
                    Condition( "a", .GT, 1 )
                    Condition( "b", .equal, 2 )
                    Or {
                        Condition( "c", .LT, 3 )
                        Condition( "d", .EQ, 4 )
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
}
