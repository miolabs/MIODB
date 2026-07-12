//
//  TestQueryExecutionRouting.swift
//  MIODBTests
//
//  Tests the execute(MDBQuery) contract that EntityContext (DualLinkServerKit)
//  relies on: the rendered SQL sent to the connection is exactly rawQuery(),
//  which is always defaultRawQuery() — the MDBQueryDelegate indirection was
//  removed once MULTI_UPSERT rendering moved into defaultRawQuery().
//

import XCTest
import MIODB

/// Captures the SQL string that execute(MDBQuery) hands to executeQuery(String).
fileprivate class CapturingDB: MIODB {
    var capturedQueries: [String] = []

    @discardableResult override func executeQuery(_ queryString: String) throws -> MDBResultSet {
        capturedQueries.append(queryString)
        return .empty()
    }
}

class TestQueryExecutionRouting: XCTestCase {

    func testExecutePassesRenderedQueryToExecuteQuery() throws {
        let db = CapturingDB()
        let query = try MDBQuery("product").insert([
            ["name": "a", "price": 1],
            ["name": "b", "price": 2],
        ])

        _ = try db.execute(query)

        XCTAssertEqual(db.capturedQueries.count, 1)
        XCTAssertEqual(db.capturedQueries[0], query.defaultRawQuery())
    }

    func testRawQueryIsDefaultRawQuery() throws {
        let query = try MDBQuery("product").insert(["name": "a"])
        XCTAssertEqual(query.rawQuery(), query.defaultRawQuery())
    }

    func testMultiUpsertRenderingIsDeterministicAndFiltersByClassname() throws {
        let rows: [[String: Any?]] = [
            ["identifier": "B0000000-0000-0000-0000-000000000002", "classname": "ProductPack", "name": "b"],
            ["identifier": "A0000000-0000-0000-0000-000000000001", "classname": "Product", "name": "a"],
        ]
        let sql = try MDBQuery("product").upsert(rows, "identifier").rawQuery()

        // Classnames must be sorted so the same logical query renders the same SQL
        // on every run (plan cache friendliness, diffable logs).
        XCTAssertTrue(sql.contains("ON CONFLICT (identifier) WHERE classname in ('Product','ProductPack') DO UPDATE SET"), sql)

        // Rendering twice must be byte-identical.
        let again = try MDBQuery("product").upsert(rows, "identifier").rawQuery()
        XCTAssertEqual(sql, again)
    }

    func testMultiUpsertWithoutClassnameOmitsConflictFilter() throws {
        let rows: [[String: Any?]] = [
            ["identifier": "A0000000-0000-0000-0000-000000000001", "name": "a"],
            ["identifier": "B0000000-0000-0000-0000-000000000002", "name": "b"],
        ]
        let sql = try MDBQuery("product").upsert(rows, "identifier").rawQuery()

        XCTAssertFalse(sql.contains("WHERE classname"), sql)
        XCTAssertTrue(sql.contains("ON CONFLICT (identifier) DO UPDATE SET"), sql)
    }
}
