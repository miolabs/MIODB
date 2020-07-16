//
//  File.swift
//  
//
//  Created by David Trallero on 11/07/2020.
//

import Foundation


public enum WHERE_LINE_OPERATOR: String {
    case EQ = "="
    case LT = "<"
    case LE = "<="
    case GT = ">"
    case GE = ">="
    case NOT_IN = "NOT IN"
    case IN = "IN"
    case IS = "IS"
    case IS_NOT = "IS NOT"
    case LIKE = "LIKE"
}

public enum WHERE_OPERATOR: String {
    case AND = "AND"
    case OR  = "OR"
}

public protocol MDBWhereString {
    func raw ( firstLine: Bool ) -> String
}


public struct MDBWhereLine : MDBWhereString {
    public var where_op:WHERE_OPERATOR = .AND
    public var field:String
    public var op: WHERE_LINE_OPERATOR
    public var value: String
    
    public func raw ( firstLine: Bool ) -> String {
        return (firstLine ? "" : "\(where_op) ") + field + " " + ( op.rawValue ) + " " + value
    }
}

public class MDBWhere {
    public var lines: [ MDBWhereString ] = []
    
    public func raw ( ) -> String {
        return lines.enumerated().map{ (index,line) in line.raw( firstLine: index == 0 ) }.joined( separator: " " )
    }
    
    func push ( _ cond: MDBWhereString ) {
        lines.append( cond )
    }
}


public class MDBWhereGroup : MDBWhereString {
    public var where_op: WHERE_OPERATOR = .AND
    public var where_fields: MDBWhere = MDBWhere( )
    
    public func raw ( firstLine: Bool ) -> String {
        return (firstLine ? "" : "\(where_op) ") + "(" + where_fields.raw( ) + ")"
    }
    
    public func raw ( ) -> String {
        return where_fields.raw()
    }

}
