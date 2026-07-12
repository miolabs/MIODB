//
//  MDBTimestamp.swift
//  MIODB
//
//  Created by Javier Segura Perez on 12/07/2026.
//

import Foundation
import MIOCore

/// Renders a Date as an SQL timestamp literal body: "YYYY-MM-DD HH:MM:SS.ffffff" (UTC).
/// Integer math straight into a byte buffer — ~9x faster than ISO8601DateFormatter and
/// keeps microsecond precision, which the formatter-based path truncated to milliseconds.
/// The render-side counterpart of MDBPostgreSQLParseTimestamp.
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
