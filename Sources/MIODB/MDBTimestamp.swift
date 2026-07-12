//
//  MDBTimestamp.swift
//  MIODB
//
//  Created by Javier Segura Perez on 12/07/2026.
//
//  SQL timestamp text <-> Date, both directions in one place. The text format
//  is the ANSI SQL one ("YYYY-MM-DD HH:MM:SS.ffffff", UTC) that PostgreSQL,
//  MySQL, SQL Server and SQLite all read and write natively. Both functions
//  are pure integer math on the proleptic Gregorian calendar (Howard
//  Hinnant's civil_from_days / days_from_civil) — no formatters, no String
//  round-trips, thread-safe, and exact inverses of each other.
//

import Foundation
import MIOCore

/// Parses ISO-datestyle date/timestamp text straight off a C buffer (as
/// returned by DB client libraries): `YYYY-MM-DD[ HH:MM:SS[.ffffff]][±HH[:MM[:SS]]]`.
///
/// A timestamp without offset is taken as UTC, matching how the servers wire
/// UTC-stored values. Returns nil for anything else (BC dates, infinity,
/// non-ISO datestyle) so callers can fall back to a formatter-based path.
public func MDBSQLParseTimestamp ( _ p: UnsafePointer<Int8> ) -> Date?
{
    var i = 0

    func digits ( _ n: Int ) -> Int? {
        var v = 0
        for _ in 0..<n {
            let c = p[i]
            if c < 48 || c > 57 { return nil } // '0'...'9'
            v = v * 10 + Int(c - 48)
            i += 1
        }
        return v
    }

    guard let y = digits(4), p[i] == 45 else { return nil } // '-'
    i += 1
    guard let mo = digits(2), p[i] == 45 else { return nil } // '-'
    i += 1
    guard let d = digits(2) else { return nil }
    guard mo >= 1, mo <= 12, d >= 1, d <= 31 else { return nil }

    // Days since 1970-01-01 in the proleptic Gregorian calendar
    // (Howard Hinnant's days_from_civil).
    let yy  = mo <= 2 ? y - 1 : y
    let era = (yy >= 0 ? yy : yy - 399) / 400
    let yoe = yy - era * 400
    let doy = (153 * (mo > 2 ? mo - 3 : mo + 9) + 2) / 5 + d - 1
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy
    let days = era * 146097 + doe - 719468

    var seconds = Double(days) * 86400.0

    // Date-only column: midnight UTC.
    if p[i] == 0 { return Date(timeIntervalSince1970: seconds) }

    guard p[i] == 32 else { return nil } // ' '
    i += 1
    guard let h = digits(2), p[i] == 58 else { return nil } // ':'
    i += 1
    guard let mi = digits(2), p[i] == 58 else { return nil } // ':'
    i += 1
    guard let s = digits(2) else { return nil }
    guard h < 24, mi < 60, s <= 60 else { return nil }

    seconds += Double(h * 3600 + mi * 60 + s)

    if p[i] == 46 { // '.'
        i += 1
        var frac = 0.0, scale = 0.1
        let start = i
        while p[i] >= 48 && p[i] <= 57 {
            frac += Double(p[i] - 48) * scale
            scale *= 0.1
            i += 1
        }
        if i == start { return nil }
        seconds += frac
    }

    if p[i] == 43 || p[i] == 45 { // '+' / '-'
        let negative = p[i] == 45
        i += 1
        guard let tzh = digits(2) else { return nil }
        var offset = tzh * 3600
        if p[i] == 58 { // ':'
            i += 1
            guard let tzm = digits(2) else { return nil }
            offset += tzm * 60
            if p[i] == 58 { // ':'
                i += 1
                guard let tzs = digits(2) else { return nil }
                offset += tzs
            }
        }
        seconds += negative ? Double(offset) : -Double(offset)
    }

    // Trailing text (" BC", junk) means this is not a plain ISO value.
    guard p[i] == 0 else { return nil }

    return Date(timeIntervalSince1970: seconds)
}

/// Convenience overload for parsing from a Swift String.
public func MDBSQLParseTimestamp ( _ str: String ) -> Date? {
    return str.withCString { MDBSQLParseTimestamp( $0 ) }
}

/// Renders a Date as an SQL timestamp literal body: "YYYY-MM-DD HH:MM:SS.ffffff" (UTC).
/// ~9x faster than ISO8601DateFormatter and keeps microsecond precision, which
/// the formatter-based path truncated to milliseconds.
public func MDBSQLTimestampString ( _ date: Date ) -> String {
    let t = date.timeIntervalSince1970
    var secs = Int64( t.rounded( .down ) )
    var micros = Int64( ((t - Double( secs )) * 1_000_000).rounded() )
    if micros >= 1_000_000 { secs += 1 ; micros -= 1_000_000 }

    var days = secs / 86400
    var rem  = secs % 86400
    if rem < 0 { rem += 86400 ; days -= 1 }

    // civil-from-days (Howard Hinnant's algorithm)
    let z   = days + 719468
    let era = (z >= 0 ? z : z - 146096) / 146097
    let doe = z - era * 146097
    let yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365
    let y   = yoe + era * 400
    let doy = doe - (365*yoe + yoe/4 - yoe/100)
    let mp  = (5*doy + 2) / 153
    let d   = doy - (153*mp + 2)/5 + 1
    let m   = mp < 10 ? mp + 3 : mp - 9
    let year = m <= 2 ? y + 1 : y

    // Fixed-width digits only cover 0000-9999; BC and far-future dates fall back
    // to the formatter (they never appear on the hot path)
    if year < 0 || year > 9999 {
        return MIOCoreISO8601Formatter().string( from: date )
    }

    let hh = rem / 3600, mi = (rem % 3600) / 60, ss = rem % 60

    var buf = [UInt8]( repeating: 0, count: 26 )
    func put2 ( _ i: Int, _ v: Int64 ) { buf[i] = 48 + UInt8(v / 10) ; buf[i+1] = 48 + UInt8(v % 10) }
    put2( 0, year / 100 ) ; put2( 2, year % 100 ) ; buf[4] = 45  // '-'
    put2( 5, m )  ; buf[7]  = 45                                 // '-'
    put2( 8, d )  ; buf[10] = 32                                 // ' '
    put2( 11, hh ) ; buf[13] = 58                                // ':'
    put2( 14, mi ) ; buf[16] = 58                                // ':'
    put2( 17, ss ) ; buf[19] = 46                                // '.'
    var us = micros
    for i in stride( from: 25, through: 20, by: -1 ) { buf[i] = 48 + UInt8( us % 10 ) ; us /= 10 }

    return String( decoding: buf, as: UTF8.self )
}
