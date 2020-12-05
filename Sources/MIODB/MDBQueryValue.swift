//
//  File.swift
//  
//
//  Created by David Trallero on 11/07/2020.
//

import Foundation
import MIOCore

public enum MDBValueError: Error {
    case couldNotConvert( _ value: Any )
}

extension MDBValueError: LocalizedError {
public var errorDescription: String? {
    switch self {
    case let .couldNotConvert(value):
        return "[MDBValue] Could not initialize with \(value) becasue its type is not supported."
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


public class MDBValue {
    public var value: String = "" ;

    static var convert: [ MDBValueTypeConversion ] = []
    public static func register_type_conversion ( _ target: AnyClass, _ fn: @escaping MDBValueTypeConversionCallBack ) {
        convert.append( MDBValueTypeConversion( target: target, convert: fn ) )
    }
    
    public init( _ v: Any?, isPartialString: Bool = false ) throws {

        try MIOCoreAutoReleasePool {
            if v == nil || v is NSNull { value = "NULL"               }
            else if v is [Any]         {
                                         let list = try (v as! [Any]).map{ try MDBValue.fromValue( $0 ).value }
                
                                         value = "(" + list.joined( separator: "," ) + ")"
                                       }
            else if v is String        { value = isPartialString ?
                                                    "'%" + MDBValue.escape_string( v as! String ) + "%'"
                                                 :  "'"  + MDBValue.escape_string( v as! String ) + "'"  }
            else if "\(type( of: v! ))" == "__NSCFBoolean" { value = (v as! Bool) ? "TRUE" : "FALSE" }
            else if v is Int           { value = String(v as! Int)    }
            else if v is Float         { value = String(v as! Float)  }
            else if v is Double        { value = String(v as! Double) }
            else if v is UUID          { value = "'" + (v as! UUID).uuidString + "'" }
            else if v is Int8          { value = String(v as! Int8)   }
            else if v is Int16         { value = String(v as! Int16)  }
            else if v is Int32         { value = String(v as! Int32)  }
            else if v is Int64         { value = String(v as! Int64)  }
            else if v is Decimal       { value = NSDecimalNumber(decimal: (v as! Decimal)).stringValue }
            else if v is Bool          { value = (v as! Bool) ? "TRUE" : "FALSE" }
            else if v is Date          { value = "'" + MIOCoreDateTDateTimeFormatter().string( from: (v as! Date) ) + "'" }
            else {
                var converted = false
                
                for c in MDBValue.convert {
                    if c.canConvert( v! ) {
                        value = c.convert( v! )
                        converted = true
                        break
                    }
                }
                
                if !converted {
                    throw MDBValueError.couldNotConvert( v! )
                }
            }
        }
    }
    
    public static func escape_string ( _ str: String ) -> String {
        return str.replacingOccurrences(of: "\'", with: "'" )
                  .replacingOccurrences(of: "'", with: "''" )
    }
    
    public static func fromValue ( _ value: Any? ) throws -> MDBValue {
        return value is MDBValue ? value as! MDBValue : try MDBValue( value )
    }

    
    public init( fromTable: String ) {
        value = fromTable.split( separator: "," )
                         .map{ checkAS( $0 ) }
                         .joined(separator: ",") as String
    }

    public init( raw: String ) {
        value = raw
    }

    private func checkAS ( _ field:String.SubSequence ) -> String {
        
        var parts:[String]?
        MIOCoreAutoReleasePool {
            parts = field.components(separatedBy: " AS ")
        }
                
        return parts!.count > 1 ?
               formatField( parts!.first! ) + " AS " + formatField( parts!.last! )
             : formatField( String( field ) )
    }
    
    private func formatField ( _ field: String ) -> String {
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


//public func toValues ( _ arr: [Any] ) -> MDBValue {
//    return MDBValue( raw: "(" + arr.map{ MDBValue.fromValue( $0 ).value }
//                                   .joined( separator: "," ) + ")" )
//}


public typealias MDBValues = [String:MDBValue]
