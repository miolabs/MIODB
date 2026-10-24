//
//  MDBSendable.swift
//  MIODB
//
//  Created by Javier Segura Perez on 24/10/2026.
//  Copyright © 2026 Javier Segura Perez. All rights reserved.
//

import Foundation

/// Converts the `Any` cell values a backend produces into `Sendable`
/// existentials, so a row can cross a concurrency boundary (network
/// serialization, actors, task groups) without a `@unchecked` box.
///
/// Scalars keep their Swift type (`Bool`, `Int16/32/64`, `Decimal`, `Date`,
/// `UUID`, `Data`, `String`, `NSNull`). JSON columns are rebuilt
/// recursively: `[Any]` becomes `[any Sendable]` and `[String:Any]`
/// becomes `[String: any Sendable]`, with `NSNumber` unboxed to `Bool`,
/// `Int` or `Double` according to its stored type. Any other type (a value a
/// custom conversion produced, for instance) throws `MDBError.notSendable`.
public enum MDBSendable
{
    public static func value ( _ v: Any ) throws -> any Sendable {
        if v is NSNull { return NSNull() }

        // Exact-type checks come first: `as?` bridges on Darwin, so an
        // NSNumber(1) would match Bool and an __NSCFBoolean would match Int.
        switch ObjectIdentifier( type( of: v ) ) {
        case ObjectIdentifier( Bool.self    ): return v as! Bool
        case ObjectIdentifier( Int.self     ): return v as! Int
        case ObjectIdentifier( Int8.self    ): return v as! Int8
        case ObjectIdentifier( Int16.self   ): return v as! Int16
        case ObjectIdentifier( Int32.self   ): return v as! Int32
        case ObjectIdentifier( Int64.self   ): return v as! Int64
        case ObjectIdentifier( UInt.self    ): return v as! UInt
        case ObjectIdentifier( UInt8.self   ): return v as! UInt8
        case ObjectIdentifier( UInt16.self  ): return v as! UInt16
        case ObjectIdentifier( UInt32.self  ): return v as! UInt32
        case ObjectIdentifier( UInt64.self  ): return v as! UInt64
        case ObjectIdentifier( Double.self  ): return v as! Double
        case ObjectIdentifier( Float.self   ): return v as! Float
        case ObjectIdentifier( String.self  ): return v as! String
        case ObjectIdentifier( Decimal.self ): return v as! Decimal
        case ObjectIdentifier( Date.self    ): return v as! Date
        case ObjectIdentifier( UUID.self    ): return v as! UUID
        case ObjectIdentifier( Data.self    ): return v as! Data
        default: break
        }

        // Foundation boxes, mostly out of JSONSerialization.
        if let n = v as? NSNumber {
            switch String( cString: n.objCType ) {
            case "c", "B": return n.boolValue
            case "d", "f": return n.doubleValue
            default      : return Int( n.int64Value )
            }
        }
        if let s = v as? String  { return s }
        if let d = v as? Decimal { return d }
        if let d = v as? Date    { return d }
        if let u = v as? UUID    { return u }
        if let d = v as? Data    { return d }

        // Containers.
        if let dict = v as? [String:Any] { return try dictionary( dict ) }
        if let arr  = v as? [Any]        { return try array( arr ) }

        // `Sendable` is a marker protocol with no runtime witness, so there is
        // no `as? any Sendable` fallback: an unknown type throws.
        throw MDBError.notSendable( String( describing: type( of: v ) ) )
    }

    /// `nil` stays `nil`; everything else goes through ``value(_:)``.
    public static func value ( _ v: Any? ) throws -> (any Sendable)? {
        guard let v = v else { return nil }
        return try value( v )
    }

    public static func array ( _ a: [Any] ) throws -> [any Sendable] {
        var out = [any Sendable]()
        out.reserveCapacity( a.count )
        for item in a { out.append( try value( item ) ) }
        return out
    }

    public static func dictionary ( _ d: [String:Any] ) throws -> [String: any Sendable] {
        var out = [String: any Sendable]( minimumCapacity: d.count )
        for ( k, item ) in d { out[ k ] = try value( item ) }
        return out
    }
}
