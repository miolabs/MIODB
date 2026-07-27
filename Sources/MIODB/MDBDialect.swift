//
//  MDBDialect.swift
//  MIODB
//
//  Created by Javier Segura Perez on 27/07/2026.
//  Copyright © 2026 Javier Segura Perez. All rights reserved.
//
//  Renders an MDBQuery into backend-specific SQL. The base class produces the
//  ANSI + PostgreSQL flavor MIODB has always generated — byte-identical to the
//  old MDBQuery.defaultRawQuery() — and backends subclass it to override the
//  constructs their database expresses differently (upserts, multi-row
//  updates, LIMIT, ILIKE, ...). See docs/SQL_DIALECTS.md.
//
//  A dialect belongs to the connection (`MIODB.dialect`), never to the query:
//  the same MDBQuery renders differently on different backends. Raw SQL
//  fragments (`andWhereRaw`, `MDBValue(raw:)`) bypass the dialect by contract —
//  callers that use them own their portability.
//

import Foundation

open class MDBDialect
{
    /// The default dialect: ANSI + PostgreSQL extensions, exactly what
    /// `MDBQuery.rawQuery()` has always produced. Guaranteed not to throw.
    public static let ansi = MDBDialect()

    public init ( ) { }

    // MARK: - Entry point

    open func render ( _ q: MDBQuery ) throws -> String {
        switch q.queryType {
            case .UNKOWN:            return ""
            case .SELECT,
                 .SELECT_FOR_UPDATE: return try select( q )
            case .INSERT:            return try insert( q )
            case .MULTI_INSERT:      return try multiInsert( q )
            case .UPDATE:            return try update( q )
            case .MULTI_UPDATE:      return try multiUpdate( q )
            case .UPSERT:            return try upsert( q )
            case .MULTI_UPSERT:      return try multiUpsert( q )
            case .DELETE:            return try delete( q )
        }
    }

    // MARK: - Statements

    open func select ( _ q: MDBQuery ) throws -> String {
        let for_update = q.queryType == .SELECT_FOR_UPDATE ? try forUpdateClause() : ""
        return q.composeQuery( [ "SELECT " + ( try distinctOnClause( q ) ) + q.selectFieldsRaw() + " FROM " + tableRef( q )
                               , q.aliasRaw( )
                               , try joinsClause( q )
                               , try whereClause( q )
                               , groupClause( q )
                               , orderClause( q )
                               , limitClause( q )
                               , offsetClause( q )
                               , for_update
                               ] )
    }

    open func insert ( _ q: MDBQuery ) throws -> String {
        let sorted_values = q.sortedValues()
        return sorted_values.isEmpty ? ""
             : q.composeQuery( [ "INSERT INTO " + tableRef( q )
                               , q.valuesFieldsRaw( sorted_values )
                               , "VALUES"
                               , valuesList( sorted_values )
                               , try whereClause( q )
                               , try returningClause( q )
                               ] )
    }

    open func multiInsert ( _ q: MDBQuery ) throws -> String {
        let sorted_values = q.sortedValues( q.multiValues.count > 0 ? q.multiValues[ 0 ] : [:] )
        return sorted_values.isEmpty ? ""
             : q.composeQuery( [ "INSERT INTO " + tableRef( q )
                               , q.valuesFieldsRaw( sorted_values )
                               , "VALUES"
                               , multiValuesList( q, sorted_values )
                               , try whereClause( q )
                               , try returningClause( q )
                               ] )
    }

    open func update ( _ q: MDBQuery ) throws -> String {
        return q.values.isEmpty ? ""
             : q.composeQuery( [ "UPDATE " + tableRef( q ) + " SET"
                               , setClause( q.sortedValues() )
                               , try whereClause( q )
                               , try returningClause( q )
                               ] )
    }

    open func multiUpdate ( _ q: MDBQuery ) throws -> String {
        let sorted_values = q.sortedValues( q.multiValues.count > 0 ? q.multiValues[ 0 ] : [:] )
        return sorted_values.isEmpty ? ""
             : q.composeQuery( [ "UPDATE " + tableRef( q ) + " SET"
                               , q.multiUpdateValuesRaw( )
                               , "FROM (SELECT * FROM (VALUES "
                               , multiValuesList( q, sorted_values )
                               , ") AS t(\( sorted_values.map{ "\"\($0.key)\"" }.joined(separator: ", ") ))) AS s"
                               , try whereClause( q )
                               , try returningClause( q )
                               ] )
    }

    open func upsert ( _ q: MDBQuery ) throws -> String {
        let sorted_values = q.sortedValues()
        return sorted_values.isEmpty ? ""
             : q.composeQuery( [ "INSERT INTO " + tableRef( q )
                               , q.valuesFieldsRaw( sorted_values )
                               , "VALUES"
                               , valuesList( sorted_values )
                               , "ON CONFLICT (" + q.on_conflict + ") DO UPDATE SET"
                               , setClause( sorted_values )
                               , try returningClause( q )
                               ] )
    }

    open func multiUpsert ( _ q: MDBQuery ) throws -> String {
        let sorted_values = q.sortedValues( q.multiValues.count > 0 ? q.multiValues[ 0 ] : [:] )
        let classnames = Set( q.multiValues.compactMap { $0["classname"]?.value } ).sorted()
        let conflict_filter = classnames.isEmpty ? "" : " WHERE classname in (\(classnames.joined( separator: ",")))"
        return sorted_values.isEmpty ? ""
             : q.composeQuery( [ "INSERT INTO " + tableRef( q )
                               , q.valuesFieldsRaw( sorted_values )
                               , "VALUES"
                               , multiValuesList( q, sorted_values )
                               , "ON CONFLICT (" + q.on_conflict + ")" + conflict_filter + " DO UPDATE SET"
                               , q.multiExcludedRaw( sorted_values )
                               , try returningClause( q )
                               ] )
    }

    open func delete ( _ q: MDBQuery ) throws -> String {
        return q.composeQuery( [ "DELETE FROM " + tableRef( q )
                               , try whereClause( q )
                               , try returningClause( q )
                               ] )
    }

    // MARK: - Clauses

    open func tableRef ( _ q: MDBQuery ) -> String {
        return MDBValue( fromTable: q.table ).value
    }

    open func joinsClause ( _ q: MDBQuery ) throws -> String {
        return try q.joins.map{ try joinClause( $0 ) }.joined( separator: " " )
    }

    open func joinClause ( _ join: Join ) throws -> String {
        return try join.raw( dialect: self )
    }

    open func whereClause ( _ q: MDBQuery ) throws -> String {
        return q._whereCond != nil ? "WHERE " + ( try q._whereCond!.raw( dialect: self ) ) : ""
    }

    open func groupClause  ( _ q: MDBQuery ) -> String { return q.groupRaw() }
    open func orderClause  ( _ q: MDBQuery ) -> String { return q.orderRaw() }
    open func limitClause  ( _ q: MDBQuery ) -> String { return q.limitRaw() }
    open func offsetClause ( _ q: MDBQuery ) -> String { return q.offsetRaw() }

    /// Note the leading space: `composeQuery` joins parts with a space, so the
    /// historical output carries two before FOR UPDATE. Kept byte-identical.
    open func forUpdateClause ( ) throws -> String { return " FOR UPDATE" }

    open func returningClause ( _ q: MDBQuery ) throws -> String { return q.returningRaw() }

    open func distinctOnClause ( _ q: MDBQuery ) throws -> String { return q.distinctOnRaw() }

    // MARK: - WHERE lines

    open func whereLine ( _ line: MDBWhereLine, firstLine: Bool ) throws -> String {
        return ( firstLine ? "" : "\(line.where_op) " ) + line.field + " " + ( try whereOperator( line.op ) ) + " " + renderValue( line.value )
    }

    open func whereOperator ( _ op: WHERE_LINE_OPERATOR ) throws -> String {
        return op.rawValue
    }

    // MARK: - Values

    /// A single value as an SQL literal. Backends whose literals differ
    /// (Oracle booleans, ...) override this and switch on `v.storage`.
    open func renderValue ( _ v: MDBValue ) -> String {
        return v.value
    }

    /// `"key"=value,...` — the SET clause of UPDATE/UPSERT.
    open func setClause ( _ sorted_values: [(key:String,value:MDBValue)] ) -> String {
        var key_eq_values: [String] = []
        for (key,value) in sorted_values {
            key_eq_values.append( "\"" + key + "\"=" + renderValue( value ) )
        }
        return key_eq_values.joined(separator: ",")
    }

    /// `(a,b,c)` — one VALUES tuple.
    open func valuesList ( _ sorted_values: [(key:String,value:MDBValue)] ) -> String {
        return "(" + ( sorted_values.map{ renderValue( $0.value ) } ).joined(separator: ",") + ")"
    }

    /// `(a,b),(c,d),...` — the VALUES tuples of a multi-row statement, with
    /// the historical gap-fill for rows that miss a column.
    open func multiValuesList ( _ q: MDBQuery, _ sorted_values: [(key:String,value:MDBValue)] ) -> String {
        // THIS IS UGLY: During the migration it happens that some entities do have relations and other do not.
        // When using a multi-insert, the ones that has relations makes "spaces to fill-in" for the other entities
        func def_value ( col: String ) -> String {
            return col.starts(with: "_relation") ? "''" : "null"
        }

        return q.multiValues.map{ row in
             "(" + sorted_values.map{ col in ( row[ col.key ].map{ renderValue( $0 ) } ?? def_value( col: col.key ) ) }.joined(separator: "," ) + ")"
           }.joined(separator: ",")
    }
}
