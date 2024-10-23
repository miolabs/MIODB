import XCTest
@testable import MIODB

// Class MDBTZ to work with timezones. See examples in DateTimezoneTests.swift in MIODBPostgreSQL project

final class TestTimeZones: XCTestCase {

    func newDateTime(_ date:String, _ secondsFromGMT: Int) -> Date {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd HH:mm:ss"
        formatter.timeZone = TimeZone(secondsFromGMT: secondsFromGMT) 
        if let dateTime = formatter.date(from: date) {
            return dateTime
        }
        XCTFail("Error creating datetime. Expected format: yyyy-MM-dd HH:mm:ss")
        return Date()
    }

// MARK: - comm  
    func test_01( ) throws {
        let date = newDateTime("2024-10-21 12:33:00", 7200)
        let nQuery = try MDBQuery("product") {
            Insert( [ "modifier": try MDBTZ(date), "str": "Hello" ] )
            Test()
        }
        let fQuery = try MDBQuery( "product" ).insert( [ "modifier": try MDBTZ(date), "str": "Hello" ] ).test()
        XCTAssertEqual( MDBQueryEncoderSQL(nQuery).rawQuery(), MDBQueryEncoderSQL(fQuery).rawQuery() )
        XCTAssertEqual( MDBQueryEncoderSQL(nQuery).rawQuery(), 
                        "INSERT INTO \"product\" (\"modifier\",\"str\") VALUES ('2024-10-21T10:33:00Z','Hello')")
    }

}
