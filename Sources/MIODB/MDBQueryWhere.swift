//
//  File.swift
//  
//
//  Created by David Trallero on 11/07/2020.
//

import Foundation


public enum WHERE_LINE_OPERATOR: String {
    case EQ = "="
    case NEQ = "!="
    case LT = "<"
    case LE = "<="
    case GT = ">"
    case GE = ">="
    case NOT_IN = "NOT IN"
    case IN = "IN"
    case IS = "IS"
    case IS_NOT = "IS NOT"
    case LIKE = "LIKE"
    case ILIKE = "ILIKE"
    case JSON_EXISTS_IN = "?|"
    case RAW = ""
    case BITWISE_AND = "&"
    case BITWISE_OR = "|"
    case BITWISE_XOR = "#"
    case BITWISE_NOT = "~"
}

public enum WHERE_OPERATOR: String {
    case AND = "AND"
    case OR  = "OR"
}

public protocol MDBWhereString {
    var where_op:WHERE_OPERATOR { get set }

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
    
    public func raw ( first_line_hides_operator: Bool = true ) -> String {
        return lines.enumerated().map{ (index,line) in line.raw( firstLine: first_line_hides_operator && index == 0 ) }.joined( separator: " " )
    }
    
    func push ( _ cond: MDBWhereString ) {
        lines.append( cond )
    }
}


public class MDBWhereGroup : MDBWhereString {
    public var where_fields: MDBWhere = MDBWhere( )
    
    public var where_op: WHERE_OPERATOR {
        get { return where_fields.lines.first?.where_op ?? .AND }
        set { }
    }
    
    public func raw ( firstLine: Bool ) -> String {
        return (firstLine ? "" : "\(where_op) ") + "(" + where_fields.raw( ) + ")"
    }
    
    public func raw ( first_line_hides_operator: Bool = true ) -> String {
        return where_fields.raw( first_line_hides_operator: first_line_hides_operator )
    }

}


public class MDBQueryWhere {
    public var _whereCond: MDBWhereGroup? = nil
    var whereStack: [ MDBWhereGroup ] = []
    
    //
    // WHERE
    //
    
    private func whereCond ( ) -> MDBWhere {
        if whereStack.isEmpty {
            _whereCond = MDBWhereGroup( )
            whereStack.append( _whereCond! )
        }
        
        return whereStack.last!.where_fields
    }
    
    @discardableResult
    public func begin_group ( ) -> MDBQueryWhere {
        let grp = MDBWhereGroup( )
        whereCond( ).push( grp )
        whereStack.append( grp )
        
        return self ;
    }

    @discardableResult
    public func end_group ( ) -> MDBQueryWhere {
        whereStack.removeLast()
        return self ;
    }

    public func add_where_line( _ where_op: WHERE_OPERATOR, _ field: Any, _ op: WHERE_LINE_OPERATOR, _ value: Any? ) throws {
        whereCond( ).push( MDBWhereLine( where_op: where_op
                                    , field: field is String ? MDBValue(fromTable: field as! String).value : (field as! MDBValue).value
                                    , op: op
                                    , value: try MDBValue.fromValue( value ).value ) )
    }
}
