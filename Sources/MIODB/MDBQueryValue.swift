//
//  File.swift
//
//
//  Created by David Trallero on 11/07/2020.
//

import Foundation
import MIOCore
//import MIOCoreLogger

public enum MDBValueError: Error {
    case couldNotConvert( _ value: Any )
}

extension MDBValueError: LocalizedError {
public var errorDescription: String? {
    switch self {
    case let .couldNotConvert(value):
        return "Could not initialize becasue its type is not supported. \(value)"
    }
    }
}


public typealias MDBValueTypeConversionCallBack = ( _ targetType: Any ) -> String

struct MDBValueTypeConversion
{
    public let target: AnyClass
    public let convert: MDBValueTypeConversionCallBack

    func canConvert ( _ value: Any ) -> Bool {
        return target == type( of: value )
    }
}


/// The typed value as it entered the query builder. Rendering to an SQL literal
/// happens lazily (see `MDBValue.value`), so backends can access the original
/// value for smarter encodings (COPY, binary params, custom date formats).
public enum MDBValueStorage {
    case null
    case bool( Bool )
    case int( Int64 )
    case float( Float )
    case double( Double )
    case decimal( Decimal )
    case string( String )
    case partialString( String )   // rendered as '%...%' for LIKE
    case uuid( UUID )
    case date( Date )
    case json( String )            // serialized JSON text, unescaped
    case array( [MDBValue] )       // rendered as (a,b,c) for IN clauses
    case raw( String )             // pre-rendered SQL fragment: fromTable/fromField/raw/custom conversions
}


public class MDBValue {
    /// The typed value. Backends read this for non-SQL-literal encodings.
    public let storage: MDBValueStorage

    private var _rendered: String? = nil
    /// The value rendered as an SQL literal. Computed on first access and cached.
    public var value: String {
        if _rendered == nil { _rendered = MDBValue.render( storage ) }
        return _rendered!
    }

    static var convert: [ MDBValueTypeConversion ] = []
    public static func register_type_conversion ( _ target: AnyClass, _ fn: @escaping MDBValueTypeConversionCallBack ) {
        convert.append( MDBValueTypeConversion( target: target, convert: fn ) )
    }

    public init( _ v: Any?, isPartialString: Bool = false ) throws {
        storage = try MIOCoreAutoReleasePool {
            if v == nil || v is NSNull { return .null }
            else if v is [Any]         {
                                         var list: [MDBValue] = []
                                         for a in (v as! [Any]) {
                                             list.append( try MDBValue.fromValue( a ) )
                                         }
                                         return .array( list )
                                       }
            else if v is String        { return isPartialString ? .partialString( v as! String )
                                                                : .string( v as! String ) }
            else if "\(type( of: v! ))" == "__NSCFBoolean" { return .bool( v as! Bool ) }
            else if v is Int           { return .int( Int64( v as! Int ) )   }
            else if v is Float         { return .float( v as! Float )        }
            else if v is Double        { return .double( v as! Double )      }
            else if v is UUID          { return .uuid( v as! UUID )          }
            else if v is Int8          { return .int( Int64( v as! Int8 ) )  }
            else if v is Int16         { return .int( Int64( v as! Int16 ) ) }
            else if v is Int32         { return .int( Int64( v as! Int32 ) ) }
            else if v is Int64         { return .int( v as! Int64 )          }
            else if v is Decimal       { return .decimal( v as! Decimal )    }
            else if v is Bool          { return .bool( v as! Bool )          }
            else if v is Date          { return .date( v as! Date )          }
            else if v is [String:Any]  {
                guard let data = try? MIOCoreJsonValue( withJSONObject: v as! [String:Any] ) else {
                    throw MDBValueError.couldNotConvert( v! )
                }

                guard let json = String.init( data: data, encoding: .utf8 ) else {
                    throw MDBValueError.couldNotConvert( v! )
                }

                return .json( json )
            }
            else {
                for c in MDBValue.convert {
                    if c.canConvert( v! ) {
                        return .raw( c.convert( v! ) )
                    }
                }

                throw MDBValueError.couldNotConvert( v! )
            }
        }
    }

    static func render ( _ storage: MDBValueStorage ) -> String {
        switch storage {
        case .null:                    return "NULL"
        case .bool( let b ):           return b ? "TRUE" : "FALSE"
        case .int( let i ):            return String( i )
        case .float( let f ):          return String( f )
        case .double( let d ):         return String( d )
        case .decimal( let d ):        return NSDecimalNumber( decimal: d ).stringValue
        case .string( let s ):         return "'"  + escape_string( s ) + "'"
        case .partialString( let s ):  return "'%" + escape_string( s ) + "%'"
        case .uuid( let u ):           return "'"  + u.uuidString.uppercased() + "'"
        case .date( let d ):           return "'"  + MDBSQLTimestampString( d ) + "'"
        // JSON text is escaped like any other string literal. Callers must NOT
        // pre-escape — MDBValue is the single place quoting happens.
        case .json( let j ):           return "'"  + escape_string( j ) + "'"
        case .array( let items ):      return "(" + items.map{ $0.value }.joined( separator: "," ) + ")"
        case .raw( let s ):            return s
        }
    }

    public static func escape_string ( _ str: String ) -> String {
        // In Swift source "\'" == "'", so the old first pass replacing "\'" with "'"
        // was an identity transform that still paid a full scan per string.
        return str.contains( "'" ) ? str.replacingOccurrences(of: "'", with: "''" ) : str
    }

    public static func fromValue ( _ value: Any? ) throws -> MDBValue {
        return value is MDBValue ? value as! MDBValue : try MDBValue( value )
    }


    /// Builds a value from the storage you pick, instead of guessing it from
    /// the Swift type the way ``init(_:isPartialString:)`` does.
    public init( storage: MDBValueStorage ) {
        self.storage = storage
    }

    public init( fromTable: String ) {
        storage = .raw( fromTable.split( separator: "," )
                                 .map{ MDBValue.checkAS( $0 ) }
                                 .joined(separator: ",") as String )
    }

    public init( fromField: String ) {
        storage = .raw( "\"\(fromField)\"" )
    }

    public init( raw: String ) {
        storage = .raw( raw )
    }

    private static func checkAS ( _ field:String.SubSequence ) -> String {

        var parts:[String]?
        MIOCoreAutoReleasePool {
            parts = field.components(separatedBy: " AS ")
        }

        return parts!.count > 1 ?
               formatField( parts!.first! ) + " AS " + formatField( parts!.last! )
             : formatField( String( field ) )
    }

    private static func formatField ( _ field: String ) -> String {
      return field.split( separator: "." )
                  .map{ $0 == "*" ? "*" : "\"" + $0 + "\"" }
                  .joined(separator: "." )
    }
}


public func toValues ( _ dict: [String:Any?] ) throws -> [String:MDBValue] {
    var ret: [String:MDBValue] = [:]

    for (key,value) in dict {
        ret.updateValue( try MDBValue.fromValue( value ), forKey: key)
    }

    return ret
}


public typealias MDBValues = [String:MDBValue]
