//
//  TestSQLTimestamp.swift
//  MIODBTests
//
//  Created by Javier Segura Perez on 08/07/2026.
//
//  The time zone contract under test (the DualLinkDB wall-clock convention):
//  SQL text WITHOUT an offset (`timestamp` columns — every model date) is the
//  local wall clock, taken as-is in the process time zone; text WITH an offset
//  (`timestamptz`) is an absolute instant. Rendering emits the local wall
//  clock with no offset. On the GMT pods this is identical to plain UTC; the
//  Madrid/Dubai cases below are what a POS device or a non-UTC dev machine
//  sees, and they guard against anyone re-pinning the fast path to UTC.
//

import XCTest
import MIODB

final class TestSQLTimestamp: XCTestCase
{
    func parse ( _ str: String ) -> Date? {
        return MDBSQLParseTimestamp( str )
    }

    func iso ( _ str: String ) -> Date {
        let f = ISO8601DateFormatter()
        return f.date( from: str )!
    }

    /// Runs `body` with `NSTimeZone.default` forced to `identifier`. The timestamp
    /// functions are pure math that read the zone per call, so no thread games are needed.
    func withTimeZone ( _ identifier: String, _ body: () -> Void ) {
        let saved = NSTimeZone.default
        NSTimeZone.default = TimeZone( identifier: identifier )!
        defer { NSTimeZone.default = saved }
        body()
    }

    // MARK: - The wall-clock contract

    func testOffsetlessTextIsTheLocalWallClock () {
        withTimeZone( "UTC" ) {
            XCTAssertEqual( parse( "2026-07-08 17:23:40" ), iso( "2026-07-08T17:23:40Z" ) )
        }
        withTimeZone( "Europe/Madrid" ) {   // July = CEST, +02:00
            XCTAssertEqual( parse( "2026-07-08 17:23:40" ), iso( "2026-07-08T17:23:40+02:00" ) )
            XCTAssertEqual( parse( "2026-01-08 17:23:40" ), iso( "2026-01-08T17:23:40+01:00" ) ) // winter, CET
        }
        withTimeZone( "Asia/Dubai" ) {      // +04:00, no DST
            XCTAssertEqual( parse( "2026-07-08 17:23:40" ), iso( "2026-07-08T17:23:40+04:00" ) )
        }
    }

    func testOffsetTextIsAnAbsoluteInstantInAnyZone () {
        for tz in [ "UTC", "Europe/Madrid", "Asia/Dubai" ] {
            withTimeZone( tz ) {
                XCTAssertEqual( parse( "2026-07-08 17:23:40+00" ), iso( "2026-07-08T17:23:40Z" ), tz )
                XCTAssertEqual( parse( "2026-07-08 17:23:40+02" ), iso( "2026-07-08T17:23:40+02:00" ), tz )
                XCTAssertEqual( parse( "2026-07-08 17:23:40-05" ), iso( "2026-07-08T17:23:40-05:00" ), tz )
            }
        }
    }

    func testRenderEmitsTheLocalWallClock () {
        let instant = iso( "2026-07-08T15:23:40Z" )
        withTimeZone( "UTC" )           { XCTAssertEqual( MDBSQLTimestampString( instant ), "2026-07-08 15:23:40.000000" ) }
        withTimeZone( "Europe/Madrid" ) { XCTAssertEqual( MDBSQLTimestampString( instant ), "2026-07-08 17:23:40.000000" ) }
        withTimeZone( "Asia/Dubai" )    { XCTAssertEqual( MDBSQLTimestampString( instant ), "2026-07-08 19:23:40.000000" ) }
    }

    // MARK: - Parsing details (zone-independent, checked under UTC)

    func testFractionalSeconds () {
        withTimeZone( "UTC" ) {
            let base = iso( "2026-07-08T17:23:40Z" ).timeIntervalSince1970

            XCTAssertEqual( parse( "2026-07-08 17:23:40.5" )!.timeIntervalSince1970, base + 0.5, accuracy: 1e-6 )
            XCTAssertEqual( parse( "2026-07-08 17:23:40.123" )!.timeIntervalSince1970, base + 0.123, accuracy: 1e-6 )
            XCTAssertEqual( parse( "2026-07-08 17:23:40.123456" )!.timeIntervalSince1970, base + 0.123456, accuracy: 1e-6 )
            XCTAssertEqual( parse( "2026-07-08 17:23:40.000001+00" )!.timeIntervalSince1970, base + 0.000001, accuracy: 1e-6 )
        }
    }

    func testTimeZoneOffsets () {
        XCTAssertEqual( parse( "2026-07-08 17:23:40+05:30" ), iso( "2026-07-08T17:23:40+05:30" ) )

        // Offsets with seconds exist in Postgres but not in ISO8601DateFormatter
        let base = iso( "2026-07-08T17:23:40Z" ).timeIntervalSince1970
        let offset = Double( 5 * 3600 + 30 * 60 + 15 )
        XCTAssertEqual( parse( "2026-07-08 17:23:40+05:30:15" )!.timeIntervalSince1970, base - offset, accuracy: 1e-6 )

        // Fraction and offset combined
        XCTAssertEqual( parse( "2026-07-08 17:23:40.25+02" )!.timeIntervalSince1970,
                        iso( "2026-07-08T17:23:40+02:00" ).timeIntervalSince1970 + 0.25, accuracy: 1e-6 )
    }

    func testDateOnlyIsLocalMidnight () {
        withTimeZone( "UTC" ) {
            XCTAssertEqual( parse( "2026-07-08" ), iso( "2026-07-08T00:00:00Z" ) )
            XCTAssertEqual( parse( "2024-02-29" ), iso( "2024-02-29T00:00:00Z" ) ) // leap day
            XCTAssertEqual( parse( "1970-01-01" ), Date( timeIntervalSince1970: 0 ) )
        }
        withTimeZone( "Europe/Madrid" ) {
            XCTAssertEqual( parse( "2026-07-08" ), iso( "2026-07-08T00:00:00+02:00" ) )
        }
    }

    /// A wall time inside a spring-forward gap has no instant of its own; it maps
    /// FORWARD past the gap (like Foundation's Calendar), never to the previous hour —
    /// and a date-only value whose midnight falls in a gap-at-00:00 zone must stay on
    /// its own day, not slide to the previous one.
    func testSpringForwardGapMapsForward () {
        withTimeZone( "America/Santiago" ) {   // DST start 2026-09-06: 00:00 -> 01:00
            let d = parse( "2026-09-06" )!
            XCTAssertTrue( MDBSQLTimestampString( d ).hasPrefix( "2026-09-06 01:00:00" ), MDBSQLTimestampString( d ) )
        }
        withTimeZone( "Europe/Madrid" ) {      // DST start 2026-03-29: 02:00 -> 03:00
            let d = parse( "2026-03-29 02:30:00" )!
            XCTAssertTrue( MDBSQLTimestampString( d ).hasPrefix( "2026-03-29 03:30:00" ), MDBSQLTimestampString( d ) )
        }
        withTimeZone( "America/New_York" ) {   // DST start 2026-03-08: 02:00 -> 03:00
            let d = parse( "2026-03-08 02:30:00" )!
            XCTAssertTrue( MDBSQLTimestampString( d ).hasPrefix( "2026-03-08 03:30:00" ), MDBSQLTimestampString( d ) )
        }
    }

    func testHistoricAndPreEpoch () {
        withTimeZone( "UTC" ) {
            XCTAssertEqual( parse( "1969-12-31 23:59:59" ), iso( "1969-12-31T23:59:59Z" ) )
            XCTAssertEqual( parse( "1900-01-01 00:00:00" ), iso( "1900-01-01T00:00:00Z" ) )

            // Postgres uses the proleptic Gregorian calendar for all dates;
            // ISO8601DateFormatter switches to Julian before 1582, so the
            // reference here is the proleptic epoch value, not the formatter.
            XCTAssertEqual( parse( "0001-01-01 00:00:00" ), Date( timeIntervalSince1970: -62_135_596_800 ) )
        }
    }

    func testNonISOInputFallsBack () {
        XCTAssertNil( parse( "infinity" ) )
        XCTAssertNil( parse( "-infinity" ) )
        XCTAssertNil( parse( "0042-07-08 17:23:40 BC" ) )      // trailing era
        XCTAssertNil( parse( "2026-07-08T17:23:40" ) )         // ISO 'T' separator is not SQL wire format
        XCTAssertNil( parse( "2026-13-08 17:23:40" ) )         // invalid month
        XCTAssertNil( parse( "2026-07-32" ) )                  // invalid day
        XCTAssertNil( parse( "2026-07-08 17:23" ) )            // missing seconds
        XCTAssertNil( parse( "2026-07-08 17:23:40." ) )        // empty fraction
        XCTAssertNil( parse( "2026-07-08 17:23:40+2" ) )       // one-digit offset
        XCTAssertNil( parse( "2026-07-08 17:23:40 extra" ) )   // trailing junk
        XCTAssertNil( parse( "" ) )
    }

    /// On the GMT pods (the production case) the fast path must agree with the
    /// formatter path it replaced.
    func testAgreesWithLegacyFormatterOnGMTPods () {
        withTimeZone( "UTC" ) {
            let legacy = ISO8601DateFormatter()
            legacy.formatOptions = [.withInternetDateTime, .withFractionalSeconds]

            let samples = [
                ( "2026-07-08 17:23:40.123", "2026-07-08T17:23:40.123+00:00" ),
                ( "2026-01-01 00:00:00.5+01", "2026-01-01T00:00:00.500+01:00" ),
                ( "1985-11-05 08:15:30.999-06", "1985-11-05T08:15:30.999-06:00" ),
            ]

            for (pg, isoStr) in samples {
                let expected = legacy.date( from: isoStr )!
                XCTAssertEqual( parse( pg )!.timeIntervalSince1970, expected.timeIntervalSince1970, accuracy: 1e-6, "for \(pg)" )
            }
        }
    }

    // MARK: - Symmetry (must hold in EVERY zone: this is what keeps wall times stable)

    /// Parse and render are exact inverses: text -> Date -> text is identity,
    /// whatever the process time zone.
    func testRoundTripTextToText () {
        let samples = [ "2026-07-08 17:23:40.123456"
                      , "1970-01-01 00:00:00.000000"
                      , "1969-12-31 23:59:59.000000"
                      , "2024-02-29 12:00:00.000001"
                      , "2000-02-29 23:59:59.999999"
                      , "1900-01-01 00:00:00.500000"
                      , "9999-12-31 23:59:59.000000"
                      ]
        for tz in [ "UTC", "Europe/Madrid", "Asia/Dubai" ] {
            withTimeZone( tz ) {
                for s in samples {
                    XCTAssertEqual( MDBSQLTimestampString( parse( s )! ), s, "round trip of \(s) in \(tz)" )
                }
            }
        }
    }

    /// Date -> text -> Date is identity to microsecond precision (away from a
    /// DST fold, where a repeated local hour is inherently ambiguous).
    func testRoundTripDateToDate () {
        let samples: [TimeInterval] = [ 0, 1_700_000_000.123456, -86_400, 951_827_696.999999, 4_102_444_799.5 ]
        for tz in [ "UTC", "Europe/Madrid", "Asia/Dubai" ] {
            withTimeZone( tz ) {
                for t in samples {
                    let d = Date( timeIntervalSince1970: t )
                    let back = parse( MDBSQLTimestampString( d ) )!
                    XCTAssertEqual( back.timeIntervalSince1970, d.timeIntervalSince1970, accuracy: 1e-6, "round trip of \(t) in \(tz)" )
                }
            }
        }
    }
}
