


import XCTest

import MIODB

class TestDBWhereGroups: XCTestCase
{
    override func setUpWithError() throws {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDownWithError() throws {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    func testTwoWhereFails ( ) throws {
        do
        {
            _ = try MDBQuery( "product" ).select().where().beginAndGroup().where()
            XCTFail("It should have thrown exception")
        } catch MDBError.whereAlreadyDefined {
            XCTAssertTrue(true)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }

    func testWhereMustExist ( ) throws {
        do
        {
            _ = try MDBQuery( "product" ).select().beginAndGroup()
            XCTFail("It should have thrown exception")
        } catch MDBError.whereNotFound {
            XCTAssertTrue(true)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        do
        {
            _ = try MDBQuery( "product" ).select().beginOrGroup()
            XCTFail("It should have thrown exception")
        } catch MDBError.whereNotFound {
            XCTAssertTrue(true)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        do
        {
            _ = try MDBQuery( "product" ).select().addCondition("name", .LT, "hello world")
            XCTFail("It should have thrown exception")
        } catch MDBError.whereNotFound {
            XCTAssertTrue(true)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }
   
    func testRootCanOnlyHaveGroupsOrASingleLine() throws {
        do
        {
            _ = try MDBQuery( "product" ).select().where().addCondition("a", .LT, 1).addCondition("b", .LT, 2)
            XCTFail("It should have thrown exception")
        } catch MDBError.rootCanOnlyHaveOneGroupOrLine {
            XCTAssertTrue(true)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        do
        {
            _ = try MDBQuery( "product" ).select().where().beginAndGroup().endGroup().beginAndGroup()
            XCTFail("It should have thrown exception")
        } catch MDBError.rootCanOnlyHaveOneGroupOrLine {
            XCTAssertTrue(true)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        do
        {
            _ = try MDBQuery( "product" ).select().where().beginAndGroup().endGroup().addCondition("a", .LT, 1)
            XCTFail("It should have thrown exception")
        } catch MDBError.rootCanOnlyHaveOneGroupOrLine {
            XCTAssertTrue(true)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        do
        {
            _ = try MDBQuery( "product" ).select().where().addCondition("a", .LT, 1).beginOrGroup()
            XCTFail("It should have thrown exception")
        } catch MDBError.rootCanOnlyHaveOneGroupOrLine {
            XCTAssertTrue(true)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        do
        {
            _ = try MDBQuery( "product" ).select().where().addCondition("a", .LT, 1)
            XCTAssertTrue(true)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        do
        {
            _ = try MDBQuery( "product" ).select().where().beginAndGroup().addCondition("a", .LT, 1).endGroup()
            XCTAssertTrue(true)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }

    func testDontMixBuilders() throws {
        do
        {
            _ = try MDBQuery( "product" ).select().where().beginGroup().addCondition("a", .LT, 1)
            XCTFail("It should have thrown exception")
        } catch MDBError.usingDeprecatedFunctionWithNewFunctions {
            XCTAssertTrue(true)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        do
        {
            _ = try MDBQuery( "product" ).select().where().orWhere("price", .GE, 15 )
            XCTFail("It should have thrown exception")
        } catch MDBError.usingDeprecatedFunctionWithNewFunctions {
            XCTAssertTrue(true)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        do
        {
            _ = try MDBQuery( "product" ).select().where().beginAndGroup().andWhere("price", .GE, 15 )
            XCTFail("It should have thrown exception")
        } catch MDBError.usingDeprecatedFunctionWithNewFunctions {
            XCTAssertTrue(true)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }

    func testSelectWhere ( ) throws {
        let query_v1 = try MDBQueryEncoderSQL(MDBQuery( "product" ).select( ).andWhere("name", .LT, "hello world")).rawQuery( ) ;
        let query = try MDBQueryEncoderSQL(MDBQuery( "product" ).select( ).where().addCondition("name", .LT, "hello world")).rawQuery( ) ;
        XCTAssert( query_v1 == query, query_v1 )
        XCTAssert( query == "SELECT * FROM \"product\" WHERE \"name\" < 'hello world'", query )

        var query2_v1 = try MDBQueryEncoderSQL(MDBQuery( "product" ).select( )
                                         .orWhere("name", .LT, "hello world")
                                         .orWhere("price", .GE, 15 )
                                         ).rawQuery( ) ;
        let query2 = try MDBQueryEncoderSQL(MDBQuery( "product" ).select( )
                                        .where()
                                            .beginOrGroup()
                                                .addCondition("name", .LT, "hello world")
                                                .addCondition("price", .GE, 15 )
                                            .endGroup()
                                         ).rawQuery( ) ;
        // Behaviour change v.1 - v.2: in v.1 parenthesis were added only when there were beginGroup 
        var oldCondition = "\"name\" < 'hello world' OR \"price\" >= 15"
        var newCondition = "(\"name\" < 'hello world' OR \"price\" >= 15)"
        query2_v1 = query2_v1.replacingOccurrences(of: oldCondition, with: newCondition)
        XCTAssert( query2_v1 == query2, query2_v1 )
        XCTAssert( query2 == "SELECT * FROM \"product\" WHERE (\"name\" < 'hello world' OR \"price\" >= 15)", query2 )


        var query2_1_v1 = try MDBQueryEncoderSQL(MDBQuery( "product" ).select( )
                                         .orWhere("name", .LT, "hello world")
                                         .orWhere("price", .GE, 15 )
                                            .limit( 10 )
                                            .offset( 20 )
                                            ).rawQuery( ) ;
        let query2_1 = try MDBQueryEncoderSQL(MDBQuery( "product" ).select( )
                                        .where()
                                            .beginOrGroup()
                                                .addCondition("name", .LT, "hello world")
                                                .addCondition("price", .GE, 15 )
                                            .endGroup()
                                            .limit( 10 )
                                            .offset( 20 )
                                         ).rawQuery( ) ;
        // Behaviour change v.1 - v.2: in v.1 parenthesis were added only when there were beginGroup 
        oldCondition = "\"name\" < 'hello world' OR \"price\" >= 15"
        newCondition = "(\"name\" < 'hello world' OR \"price\" >= 15)"
        query2_1_v1 = query2_1_v1.replacingOccurrences(of: oldCondition, with: newCondition)
        XCTAssert( query2_1_v1 == query2_1, query2_1_v1 )
        XCTAssert( query2_1 == "SELECT * FROM \"product\" WHERE (\"name\" < 'hello world' OR \"price\" >= 15) LIMIT 10 OFFSET 20", query2_1 )

        let query3_v1 = try MDBQueryEncoderSQL(MDBQuery( "product" ).select( )
                                           .beginGroup()
                                             .orWhere("name", .LT, "hello world")
                                             .orWhere("price", .GE, 15 )
                                           .endGroup()
                                           .beginGroup()
                                             .andWhere( "max", .EQ, 1234 )
                                           .endGroup()
                                         ).rawQuery( ) ;
        let query3 = try MDBQueryEncoderSQL(MDBQuery( "product" ).select( )
                                        .where()
                                            .beginAndGroup()
                                                .beginOrGroup()
                                                    .addCondition("name", .LT, "hello world")
                                                    .addCondition("price", .GE, 15 )
                                                .endGroup()
                                                .addCondition( "max", .EQ, 1234 )
                                            .endGroup()
                                         ).rawQuery( ) ;                                         
        XCTAssert( query3_v1 == "SELECT * FROM \"product\" WHERE (\"name\" < 'hello world' OR \"price\" >= 15) AND (\"max\" = 1234)", query3_v1 )
        XCTAssert( query3 == "SELECT * FROM \"product\" WHERE ((\"name\" < 'hello world' OR \"price\" >= 15) AND \"max\" = 1234)", query3 )

        let query3_1_v1 = try MDBQueryEncoderSQL(MDBQuery( "product" ).select( )
                                           .beginGroup()
                                               .beginGroup()
                                                 .orWhere("name", .LT, "hello world")
                                                 .orWhere("price", .GE, 15 )
                                               .endGroup()
                                               .beginGroup()
                                                 .andWhere( "max", .EQ, 1234 )
                                               .endGroup()
                                           .endGroup()
                                           ).rawQuery( ) ;
        let query3_1 = try MDBQueryEncoderSQL(MDBQuery( "product" ).select( )
                                        .where()
                                            .beginAndGroup()
                                                .beginOrGroup()
                                                    .addCondition("name", .LT, "hello world")
                                                    .addCondition("price", .GE, 15 )
                                                .endGroup()
                                                .beginAndGroup()
                                                    .addCondition( "max", .EQ, 1234 )
                                                .endGroup()
                                            .endGroup()
                                         ).rawQuery( ) ;       
        XCTAssert( query3_1_v1 == query3_1, query3_1_v1 )                                                                                
        XCTAssert( query3_1 == "SELECT * FROM \"product\" WHERE ((\"name\" < 'hello world' OR \"price\" >= 15) AND (\"max\" = 1234))", query3_1 )
    }

    func testSelectWhere2 ( ) throws {
      let query_v1 = try MDBQueryEncoderSQL(MDBQuery( "product" ).select( )
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
                                ).rawQuery( ) ;
      let query = try MDBQueryEncoderSQL(MDBQuery( "product" ).select( )
                                        .where()
                                         .beginOrGroup()
                                           .addCondition("a", .LT, "1")
                                           .addCondition("b", .GE, 15 )
                                           .beginAndGroup()
                                             .addCondition( "c", .GT, 2 )
                                             .addCondition( "d", .LT, 3 )
                                             .beginOrGroup()
                                               .addCondition( "e", .GT, 4 )
                                               .addCondition( "f", .LT, 5 )
                                             .endGroup()
                                           .endGroup()
                                           .addCondition("name2", .LT, "hello world")
                                           .addCondition("price2", .GE, 15 )
                                         .endGroup()
                                ).rawQuery( ) ;      
        XCTAssert( query == query_v1, query )                          
        XCTAssert( query_v1 == "SELECT * FROM \"product\" WHERE (\"a\" < '1' OR \"b\" >= 15 OR (\"c\" > 2 AND \"d\" < 3 AND (\"e\" > 4 OR \"f\" < 5)) OR \"name2\" < 'hello world' OR \"price2\" >= 15)", query_v1 )
    }

    func testOperatorsFancyNames_01 ( ) throws {
        let query1 = try MDBQuery( "product" ).select( )
                                        .where()
                                         .beginOrGroup()
                                            .addCondition("b", .EQ, "1")
                                            .addCondition("b", .NEQ, 15 )
                                            .addCondition("b", .LT, 1)
                                            .addCondition("b", .LE, 2)
                                            .addCondition("b", .GT, 3)
                                            .addCondition("b", .GE, 4)
                                            .addCondition("b", .NOT_IN, [1,2])
                                            .addCondition("b", .IN, [1,2])
                                            .addCondition("b", .IS, "NULL")
                                            .addCondition("b", .IS_NOT, "NULL")
                                            .addCondition("b", .LIKE, "hello world%")
                                            .addCondition("b", .ILIKE, "%hello world%")
                                            .addCondition("b", .JSON_EXISTS_IN, ["a","b"])
                                            .addCondition("b", .RAW, "b = 1")
                                            .addCondition("b", .BITWISE_AND, 5)
                                            .addCondition("b", .BITWISE_OR, 6)
                                            .addCondition("b", .BITWISE_XOR, 7)
                                            .addCondition("b", .BITWISE_NOT, 8)
                                        .endGroup()
        let query2 = try MDBQuery( "product" ).select( )
                                        .where()
                                         .beginOrGroup()
                                            .addCondition("b", .equal, "1")
                                            .addCondition("b", .notEqual, 15 )
                                            .addCondition("b", .lessThan, 1)
                                            .addCondition("b", .lessThanOrEqual, 2)
                                            .addCondition("b", .greaterThan, 3)
                                            .addCondition("b", .greaterThanOrEqual, 4)
                                            .addCondition("b", .notIn, [1,2])
                                            .addCondition("b", .in, [1,2])
                                            .addCondition("b", .is, "NULL")
                                            .addCondition("b", .isNot, "NULL")
                                            .addCondition("b", .like, "hello world%")
                                            .addCondition("b", .ilike, "%hello world%")
                                            .addCondition("b", .jsonExistsIn, ["a","b"])
                                            .addCondition("b", .raw, "b = 1")
                                            .addCondition("b", .bitwiseAnd, 5)
                                            .addCondition("b", .bitwiseOr, 6)
                                            .addCondition("b", .bitwiseXor, 7)
                                            .addCondition("b", .bitwiseNot, 8)
                                        .endGroup()
        XCTAssertEqual( MDBQueryEncoderSQL(query1).rawQuery(), MDBQueryEncoderSQL(query2).rawQuery() )
    }

}
