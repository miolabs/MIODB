//
//  File.swift
//  
//
//  Created by David Trallero on 11/07/2020.
//

import Foundation

/*
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
*/

public protocol MDBWhereStringV1 {
    var where_op:WHERE_OPERATOR { get set }

    func raw ( firstLine: Bool ) -> String
}


public struct MDBWhereLineV1 : MDBWhereStringV1 {
    public var where_op:WHERE_OPERATOR = .AND
    public var field:String
    public var op: WHERE_LINE_OPERATOR
    public var value: String
    
    public func raw ( firstLine: Bool ) -> String {
        return (firstLine ? "" : "\(where_op) ") + field + " " + ( op.value ) + " " + value
    }
}

public class MDBWhereV1 {
    public var lines: [ MDBWhereStringV1 ] = []
    
    public func raw ( first_line_hides_operator: Bool = true ) -> String {
        return lines.enumerated().map{ (index,line) in line.raw( firstLine: first_line_hides_operator && index == 0 ) }.joined( separator: " " )
    }
    
    func push ( _ cond: MDBWhereStringV1 ) {
        lines.append( cond )
    }
}


public class MDBWhereGroupV1 : MDBWhereStringV1 {
    public var where_fields: MDBWhereV1 = MDBWhereV1( )
    
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


public class MDBQueryWhereV1 {
    public var _whereCond: MDBWhereGroupV1? = nil
    var whereStack: [ MDBWhereGroupV1 ] = []
    
    //
    // WHERE
    //
    
    private func whereCond ( ) -> MDBWhereV1 {
        if whereStack.isEmpty {
            _whereCond = MDBWhereGroupV1( )
            whereStack.append( _whereCond! )
        }
        
        return whereStack.last!.where_fields
    }
    
    @discardableResult
    public func begin_group ( ) -> MDBQueryWhereV1 {
        let grp = MDBWhereGroupV1( )
        whereCond( ).push( grp )
        whereStack.append( grp )
        
        return self ;
    }

    @discardableResult
    public func end_group ( ) -> MDBQueryWhereV1 {
        whereStack.removeLast()
        return self ;
    }

    public func add_where_line( _ where_op: WHERE_OPERATOR, _ field: Any, _ op: WHERE_LINE_OPERATOR, _ value: Any? ) throws {
        whereCond( ).push( MDBWhereLineV1( where_op: where_op
                                    , field: field is String ? MDBValue(fromTable: field as! String).value : (field as! MDBValue).value
                                    , op: op
                                    , value: try MDBValue.fromValue( value ).value ) )
    }
}
