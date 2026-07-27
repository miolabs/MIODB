//
//  File.swift
//
//
//  Created by David Trallero on 26/7/21.
//

import Foundation

public enum JOIN_TYPE: String {
    case INNER = "INNER"
    case LEFT  = "LEFT"
    case RIGHT = "RIGHT"
    case FULL  = "FULL"
}

public class Join: MDBQueryWhere {
    var joinType: JOIN_TYPE
    public var table: String
    var fromTable: String
    var toTable: String
    var asWhat: String? = nil

    init ( joinType: JOIN_TYPE, table: String, fromTable: String, toTable: String, asWhat: String? = nil ) {
        self.joinType  = joinType
        self.table     = table
        self.fromTable = fromTable
        self.toTable   = toTable
        self.asWhat    = asWhat
    }

    public func raw ( dialect: MDBDialect ) throws -> String {
        return "\(joinType) JOIN \"\(table)\"" + (asWhat != nil ? " AS \"\(asWhat!)\"" : "") + " ON \(fromTable) = \(toTable)" + ( try rawWhere( dialect: dialect ) )
    }

    /// Legacy entry point, renders with the default dialect (which never throws).
    func raw ( ) -> String {
        return ( try? raw( dialect: .ansi ) ) ?? ""
    }

    @discardableResult
    public func addWhereLine( _ where_op: WHERE_OPERATOR, _ field: Any, _ op: WHERE_LINE_OPERATOR, _ value: Any? ) throws -> Join {
        try super.add_where_line( where_op, field, op, value )

        return self
    }

    func rawWhere ( dialect: MDBDialect ) throws -> String {
        return _whereCond != nil ? " " + ( try _whereCond!.raw( first_line_hides_operator: false, dialect: dialect ) ) : ""
    }
}


public class JoinJSON: Join {
    init ( joinType: JOIN_TYPE, table: String, json: String, toTable: String, asWhat: String? = nil ) {
        super.init( joinType: joinType, table: table, fromTable: json, toTable: toTable, asWhat: asWhat )
    }

    public override func raw ( dialect: MDBDialect ) throws -> String {
        // join product on _relation__products::jsonb ? UPPER(product.id::text) as product_productmodifier;

        return "\(joinType) JOIN \"\(table)\"" + (asWhat != nil ? " AS \"\(asWhat!)\"" : "") + " ON \(fromTable)::jsonb ? UPPER(\(toTable)::text) \( try rawWhere( dialect: dialect ) )"
    }
}
