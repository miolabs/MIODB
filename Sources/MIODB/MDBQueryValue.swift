//
//  File.swift
//  
//
//  Created by David Trallero on 11/07/2020.
//

import Foundation

public class MDBValue {
    public var value: String = "" ;

    public init( _ v: Any? ) {
        if v == nil || v is NSNull { value = "NULL"               }
        else if v is [Any]         { value = "(" + (v as! [Any]).map{ MDBValue.fromValue( $0 ).value }
                                                                .joined( separator: "," ) + ")" }
        else if v is String        { value = "'\(v as! String)'"  }
        else if "\(type( of: v! ))" == "__NSCFBoolean" { value = (v as! Bool) ? "TRUE" : "FALSE" }
        else if v is Int           { value = String(v as! Int)    }
        else if v is Float         { value = String(v as! Float)  }
        else if v is Double        { value = String(v as! Double) }
        else if v is Int8          { value = String(v as! Int8)   }
        else if v is Int16         { value = String(v as! Int16)  }
        else if v is Int32         { value = String(v as! Int32)  }
        else if v is Int64         { value = String(v as! Int64)  }
        else if v is Bool          { value = (v as! Bool) ? "TRUE" : "FALSE" }
        // TODO: Exception!
        // TODO: Date?
    }
    
    public static func fromValue ( _ value: Any? ) -> MDBValue {
        return value is MDBValue ? value as! MDBValue : MDBValue( value )
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
        let parts = field.components(separatedBy: " AS ")
        
        return parts.count > 1 ?
               formatField( parts.first! ) + " AS " + formatField( parts.last! )
             : formatField( String( field ) )
    }
    
    private func formatField ( _ field: String ) -> String {
      return field.split( separator: "." )
                  .map{ $0 == "*" ? "*" : "\"" + $0 + "\"" }
                  .joined(separator: "." )
    }
}


public func toValues ( _ dict: [String:Any?] ) -> [String:MDBValue] {
    var ret: [String:MDBValue] = [:]
    
    for (key,value) in dict {
        ret.updateValue( MDBValue.fromValue( value ), forKey: key)
    }
    
    return ret
}


//public func toValues ( _ arr: [Any] ) -> MDBValue {
//    return MDBValue( raw: "(" + arr.map{ MDBValue.fromValue( $0 ).value }
//                                   .joined( separator: "," ) + ")" )
//}


public typealias MDBValues = [String:MDBValue]
