//
//  MDBTimestamp.swift
//  MIODB
//
//  Created by Javier Segura Perez on 12/07/2026.
//
//  SQL timestamp text <-> Date, both directions in one place. The text format
//  is the ANSI SQL one ("YYYY-MM-DD HH:MM:SS.ffffff") that PostgreSQL,
//  MySQL, SQL Server and SQLite all read and write natively. Both functions
//  are pure integer math on the proleptic Gregorian calendar (Howard
//  Hinnant's civil_from_days / days_from_civil) — no formatters, no String
//  round-trips, thread-safe, and exact inverses of each other.
//
//  Time zone contract (the DualLinkDB wall-clock convention):
//  - Text WITHOUT an offset — a `timestamp` column, which is what every model
//    date is — is the wall clock, taken in the process time zone as-is: "16:00"
//    means 16:00 here, and a Date that reads 16:00 here renders as "16:00".
//    On the GMT pods this is byte-identical to the old UTC behavior.
//  - Text WITH an offset — a `timestamptz` column — is an absolute instant:
//    the offset is applied, and the resulting Date shows in whatever the
//    local time zone is.
//  The zone is read from `NSTimeZone.default` per call, matching what the
//  MIOCore wall-clock formatters resolve.
//
//  Known limitation — timestamptz WRITES from a non-UTC process:
//  MDBSQLTimestampString emits no offset, and PostgreSQL reads an offset-less
//  literal in the SESSION time zone (normally UTC), not the client process
//  zone, so a Date written to a `timestamptz` column from a non-UTC process
//  shifts by the local offset. No DualLinkDB model attribute maps to
//  timestamptz and the pods run UTC, so no production write hits this. If such
//  writes ever appear, render the literal with an explicit ±HH:MM offset for
//  the Postgres dialect — Postgres drops it when casting to `timestamp`/`date`
//  and honors it for `timestamptz`. The same applies to the out-of-range-year
//  fallback below (< 0000 / > 9999), which stays on the UTC ISO formatter.
//

import Foundation
import MIOCore

/// Converts civil (wall-clock) seconds-since-epoch into the instant that reads
/// that wall time in the process time zone. The offset is sampled twice so a
/// value near a DST transition resolves against the adjusted instant, and a
/// wall time inside a spring-forward gap — which has no instant of its own,
/// including MIDNIGHT of a gap-at-00:00 date-only value (America/Santiago,
/// Atlantic/Azores) — is mapped FORWARD past the gap, the same way Foundation's
/// Calendar resolves skipped wall times, instead of landing on the previous
/// hour (or previous day).
@inline(__always)
func MDBSQLWallToInstant ( _ civil: Double ) -> Date {
    let tz = NSTimeZone.default
    let offset1 = tz.secondsFromGMT( for: Date( timeIntervalSince1970: civil ) )
    let offset2 = tz.secondsFromGMT( for: Date( timeIntervalSince1970: civil - Double( offset1 ) ) )
    let candidate = civil - Double( offset2 )
    if tz.secondsFromGMT( for: Date( timeIntervalSince1970: candidate ) ) != offset2 {
        // The candidate's own offset disagrees: `civil` sits in a gap. Subtracting
        // the PRE-transition offset — the smaller of the two samples, whichever
        // sample order the zone's sign produced — lands past the gap (forward).
        return Date( timeIntervalSince1970: civil - Double( min( offset1, offset2 ) ) )
    }
    return Date( timeIntervalSince1970: candidate )
}

/// Parses ISO-datestyle date/timestamp text straight off a C buffer (as
/// returned by DB client libraries): `YYYY-MM-DD[ HH:MM:SS[.ffffff]][±HH[:MM[:SS]]]`.
///
/// A timestamp without offset is the local wall clock; one with an offset is an
/// absolute instant (see the header). Returns nil for anything else (BC dates,
/// infinity, non-ISO datestyle) so callers can fall back to a formatter-based path.
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

    // Date-only column: local midnight.
    if p[i] == 0 { return MDBSQLWallToInstant( seconds ) }

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

    var has_offset = false
    if p[i] == 43 || p[i] == 45 { // '+' / '-'
        has_offset = true
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

    // With an offset the value is an absolute instant; without one it is
    // the local wall clock.
    return has_offset ? Date(timeIntervalSince1970: seconds) : MDBSQLWallToInstant( seconds )
}

/// Convenience overload for parsing from a Swift String.
public func MDBSQLParseTimestamp ( _ str: String ) -> Date? {
    return str.withCString { MDBSQLParseTimestamp( $0 ) }
}

/// Renders a Date as an SQL timestamp literal body: "YYYY-MM-DD HH:MM:SS.ffffff",
/// the local wall clock with no offset (a Date that reads 16:00 here renders as
/// "16:00"). ~9x faster than a DateFormatter and keeps microsecond precision,
/// which the formatter-based path truncated to milliseconds.
public func MDBSQLTimestampString ( _ date: Date ) -> String {
    // Shift the epoch value by the local offset so the civil math below yields
    // the local wall clock. Offsets are whole seconds, so the fraction is intact.
    let t = date.timeIntervalSince1970 + Double( NSTimeZone.default.secondsFromGMT( for: date ) )
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
