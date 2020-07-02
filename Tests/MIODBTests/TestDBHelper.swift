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

    
    func testPerformanceExample() throws {
        // This is an example of a performance test case.
        self.measure {
            // Put the code you want to measure the time of here.
        }
    }

}
