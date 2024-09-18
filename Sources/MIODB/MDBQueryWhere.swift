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
    case UNDEF = "UNDEF"
}

public protocol MDBWhereString {
    var where_op:WHERE_OPERATOR { get set }

    func raw ( firstLine: Bool) -> String
}


public struct MDBWhereLine : MDBWhereString {
    public var where_op:WHERE_OPERATOR = .AND
    public var field:String
    public var op: WHERE_LINE_OPERATOR
    public var mdbValue: MDBValue
    public var value: String = ""

    init( where_op: WHERE_OPERATOR, field: String, op: WHERE_LINE_OPERATOR, mdbValue: MDBValue ) {
        self.where_op = where_op
        self.field = field
        self.op = op
        self.mdbValue = mdbValue
        self.value = mdbValue.value ?? ""
    }
    
    public func raw ( firstLine: Bool) -> String {
        if (where_op == .UNDEF) {  // v2
            return field + " " + ( op.rawValue ) + " " + value
        }
        else {  // v1
            return (firstLine ? "" : "\(where_op) ") + field + " " + ( op.rawValue ) + " " + value
        }
    }
}

/*
public class MDBWhere {
    public var lines: [ MDBWhereString ] = []
    
    public func raw ( first_line_hides_operator: Bool = true ) -> String {
        return lines.enumerated().map{ (index,line) in line.raw( firstLine: first_line_hides_operator && index == 0 ) }.joined( separator: " " )
    }
    
    func push ( _ cond: MDBWhereString ) {
        lines.append( cond )
    }
}
*/


public class MDBWhereGroup : MDBWhereString {
    //public var where_fields: MDBWhere = MDBWhere( )
    public var lines: [ MDBWhereString ] = []

    private var _group_where_op : WHERE_OPERATOR = .UNDEF
    
    init () {
        _group_where_op = .UNDEF  // v1 groups (a or b) will have the operator in the first line
    }

    init (_ op:WHERE_OPERATOR) {  // v2 groups or (a b) will have the operator in the own group
        _group_where_op = op
    }
    
    public var where_op: WHERE_OPERATOR {
        //get { return where_fields.lines.first?.where_op ?? .AND }
        get { 
            if (_group_where_op == .UNDEF) {
                return lines.first?.where_op ?? .AND
            }
            else {
                return _group_where_op
            }
        }
        set { }
    }
    
    public func raw ( firstLine: Bool) -> String {
        if (_group_where_op != .UNDEF) {  // v2
            //return raw( ) // calls to raw(first_line_hides_operator) 
            let dontCareIguess = true
            var ret = ""
            if (lines.count > 0) {
                ret = "(" + lines[0].raw(firstLine: dontCareIguess)
                for i in 1..<lines.count {
                    ret += " " + ( _group_where_op.rawValue ) + " " 
                    ret += lines[i].raw(firstLine: dontCareIguess)
                }
                ret += ")"
            }
            return ret;
        }
        else {
            return (firstLine ? "" : "\(where_op) ") + "(" + raw( ) + ")"
        }
    }
    
    // public func raw ( first_line_hides_operator: Bool = true ) -> String {
    //     return where_fields.raw( first_line_hides_operator: first_line_hides_operator )
    // }
    public func raw ( first_line_hides_operator: Bool = true ) -> String {
        if (_group_where_op != .UNDEF) {  // v2
            return ""
        }
        else {  // v1 or root group
            return lines.enumerated().map{ (index,line) in 
                        line.raw( firstLine: first_line_hides_operator && index == 0) 
                        }.joined( separator: " " )
        }
    }

    func push ( _ cond: MDBWhereString ) {
        lines.append( cond )
    }

    func contains_MDBWhereLine ( ) -> Bool {
        for item in lines {
            if item is MDBWhereLine {
                return true
            }
        }
        return false
    }

}


public class MDBQueryWhere {
    public var _whereCond: MDBWhereGroup? = nil
    var whereStack: [ MDBWhereGroup ] = []
    
    init () {
        _whereCond = MDBWhereGroup( )
        whereStack.append( _whereCond! )
    }
    
    private func whereCond ( ) -> MDBWhereGroup { //MDBWhere {
        // if whereStack.isEmpty {
        //     _whereCond = MDBWhereGroup( )
        //     whereStack.append( _whereCond! )
        // }
        
        return whereStack.last!
    }
    
    @discardableResult
    public func begin_group (_ where_op: WHERE_OPERATOR = .UNDEF ) throws -> MDBQueryWhere {
        if (where_op != .UNDEF && whereStack.count == 1 && _whereCond!.contains_MDBWhereLine( )) { // version 2 behaviour
                throw MDBError.rootCanOnlyHaveGroupsOrOneLine
            }
        let grp = MDBWhereGroup( where_op )
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
        if (where_op != .UNDEF) {  // v.1 behaviour
            whereCond( ).push( MDBWhereLine( where_op: where_op
                                        , field: field is String ? MDBValue(fromTable: field as! String).value : (field as! MDBValue).value
                                        , op: op
                                        , mdbValue: try MDBValue.fromValue( value ) ) )
        }
        else {
            // v.2: condition lines can only be inside groups or in the first level if nothing else is there
            if (whereStack.count == 1 && _whereCond!.lines.count > 0) {
                throw MDBError.rootCanOnlyHaveGroupsOrOneLine
            }
            else {
                whereCond( ).push( MDBWhereLine( where_op: .UNDEF
                                        , field: field is String ? MDBValue(fromTable: field as! String).value : (field as! MDBValue).value
                                        , op: op
                                        , mdbValue: try MDBValue.fromValue( value ) ) )
            }
        }
    }
}


/*

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

*/
