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
    /// Case- and diacritic-insensitive ILIKE. The raw value is never valid
    /// SQL — dialects render it (`MDBDialect.whereLine` folds both sides).
    case ILIKE_DI = "ILIKE_DI"
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

    func raw ( firstLine: Bool, dialect: MDBDialect ) throws -> String
}

public extension MDBWhereString {
    /// Legacy entry point, renders with the default dialect (which never throws).
    func raw ( firstLine: Bool ) -> String {
        return ( try? raw( firstLine: firstLine, dialect: .ansi ) ) ?? ""
    }
}


public struct MDBWhereLine : MDBWhereString {
    public var where_op:WHERE_OPERATOR = .AND
    public var field:String
    public var op: WHERE_LINE_OPERATOR
    /// The typed value. Rendering to an SQL literal is deferred to the
    /// dialect, so backends can render booleans, dates, ... their own way.
    public var value: MDBValue

    public func raw ( firstLine: Bool, dialect: MDBDialect ) throws -> String {
        return try dialect.whereLine( self, firstLine: firstLine )
    }
}

public class MDBWhere {
    public var lines: [ MDBWhereString ] = []

    public func raw ( first_line_hides_operator: Bool = true, dialect: MDBDialect ) throws -> String {
        return try lines.enumerated().map{ (index,line) in try line.raw( firstLine: first_line_hides_operator && index == 0, dialect: dialect ) }.joined( separator: " " )
    }

    /// Legacy entry point, renders with the default dialect (which never throws).
    public func raw ( first_line_hides_operator: Bool = true ) -> String {
        return ( try? raw( first_line_hides_operator: first_line_hides_operator, dialect: .ansi ) ) ?? ""
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

    public func raw ( firstLine: Bool, dialect: MDBDialect ) throws -> String {
        return (firstLine ? "" : "\(where_op) ") + "(" + ( try where_fields.raw( dialect: dialect ) ) + ")"
    }

    public func raw ( first_line_hides_operator: Bool = true, dialect: MDBDialect ) throws -> String {
        return try where_fields.raw( first_line_hides_operator: first_line_hides_operator, dialect: dialect )
    }

    /// Legacy entry point, renders with the default dialect (which never throws).
    public func raw ( first_line_hides_operator: Bool = true ) -> String {
        return ( try? raw( first_line_hides_operator: first_line_hides_operator, dialect: .ansi ) ) ?? ""
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
                                    , value: try MDBValue.fromValue( value ) ) )
    }
}
