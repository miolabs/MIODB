//
//  MDBQuery.swift
//  MDBPostgresSQL
//
//  Created by Javier Segura Perez on 28/12/2019.
//  Copyright © 2019 Javier Segura Perez. All rights reserved.
//

import Foundation
import MIOCoreLogger

public enum QUERY_TYPE
{
    case UNKOWN
    case SELECT
    case SELECT_FOR_UPDATE
    case INSERT
    case MULTI_INSERT
    case UPDATE
    case MULTI_UPDATE
    case DELETE
    case UPSERT
    case MULTI_UPSERT
}


public enum ORDER_BY_DIRECTION: String {
    case ASC  = "ASC"
    case DESC = "DESC"
}


struct OrderBy {
//    var field: String
//    var dir: ORDER_BY_DIRECTION = .ASC
    let rawValue: String
    
    init(field: String, dir: ORDER_BY_DIRECTION) {
        self.rawValue = field + " \(dir)"
    }
    
    init(raw: String) {
        self.rawValue = raw
    }
    
    func raw ( ) -> String {
        return rawValue
    }
}


public class MDBQuery: MDBQueryWhere {

    // In some cases like "WHERE IN ()" we can predict the query will not return values
    public var willBeEmpty: Bool = false
    public var table: String = ""
    public var queryType: QUERY_TYPE = .UNKOWN
    var _selectFields: [ String ] = []
    public var values: MDBValues = [:]
    public var multiValues: [MDBValues] = []
    var _returning: [String] = []
    var _limit: Int32 = 0
    var _offset: Int32 = 0
    var order: [ OrderBy ] = []
    var joins: [ Join ] = []
    var _group_by: [ String ] = []
    public var on_conflict: String = ""
    var distinct_on: [String] = []
    public var _alias: String? = nil

    public init( _ table: String ) {
        self.table = table
    }
    
    public func alias ( _ alias_name: String ) { _alias = alias_name }
    func aliasRaw ( ) -> String { return _alias == nil ? "" : "AS \(_alias!)" }
    
    public static func beginTransactionStament() -> String { return( "BEGIN TRANSACTION" ) }
    public static func commitTransactionStament() -> String { return( "COMMIT TRANSACTION" ) }
    
    public func returning ( _ args: String... ) -> MDBQuery {
        for field in args {
            _returning.append( MDBValue( fromTable: field ).value )
        }
        
        return self
    }
    
    public func returningRaw ( ) -> String
    {
        return _returning.isEmpty ? "" : "RETURNING " + _returning.joined( separator: "," )
    }
    
    
    @discardableResult
    public func select ( _ args: Any... ) -> MDBQuery {
        return select( args )
    }

    /// The array form, and the one that does the work.
    ///
    /// It exists because Swift does not splat an array into a variadic parameter: without
    /// it, `select_for_update`'s `select( args )` handed the whole `[Any]` to the variadic
    /// as a *single* element, and `field as! String` then trapped on an array. Every call
    /// to `select_for_update` with at least one argument crashed the process.
    @discardableResult
    public func select ( _ args: [Any] ) -> MDBQuery {
        for field in args { append_select_field( field ) }

        queryType = .SELECT

        return self
    }

    /// Appends one field, or several if what arrived was itself an array.
    ///
    /// The array case is not defensive padding, it is load-bearing. Overload resolution
    /// does **not** send every array to `select( _: [Any] )`: an `[Any]` built explicitly
    /// binds there, but a `[String]` — much the commoner thing to have in hand — binds to
    /// the *variadic* overload as a single element, because Swift ranks that above an
    /// array covariance conversion. Splatting here means an array says "these fields"
    /// whichever overload won, instead of the meaning depending on the caller's static
    /// type.
    ///
    /// Anything that is neither a column name, an MDBValue, nor an array is a caller bug.
    /// It used to be met with `field as! String`, which **trapped** — and a trap takes the
    /// whole process down, where a nonsense column name is merely a query the database
    /// rejects. So it is logged and stringified: still wrong, but survivably wrong.
    private func append_select_field ( _ field: Any ) {
        if let value = field as? MDBValue {
            _selectFields.append( value.value )
            return
        }

        if let name = field as? String {
            _selectFields.append( MDBValue( fromTable: name ).value )
            return
        }

        if let nested = field as? [Any] {
            for inner in nested { append_select_field( inner ) }
            return
        }

        Log.warning( "MDBQuery.select: expected a column name or an MDBValue, got \(type( of: field )) (\(field)). This is a bug in the caller; the query will almost certainly fail." )

        _selectFields.append( MDBValue( fromTable: String( describing: field ) ).value )
    }

    @discardableResult
    public func select_for_update ( _ args: Any... ) -> MDBQuery {
        select( args )          // resolves to select( _: [Any] ), not back to this one
        queryType = .SELECT_FOR_UPDATE

        return self
    }

    @discardableResult
    public func select_for_update ( _ args: [Any] ) -> MDBQuery {
        select( args )
        queryType = .SELECT_FOR_UPDATE

        return self
    }

    public func selectFieldsRaw ( ) -> String {
        return _selectFields.isEmpty ? "*" : _selectFields.joined( separator: "," )
    }
    

    
    public func update ( _ val: [String:Any?] ) throws -> MDBQuery {
        queryType = .UPDATE
        self.values = try toValues( val )
        return self
    }
    
    public func insert ( _ val: [String:Any?] ) throws -> MDBQuery {
        queryType = .INSERT
        self.values = try toValues( val )
        return self
    }
    
    public func upsert ( _ val: [String:Any?], _ conflict: String ) throws -> MDBQuery {
        queryType = .UPSERT
        self.values = try toValues( val )
        self.on_conflict = conflict
        return self
    }

    public func upsert ( _ val: [[String:Any?]], _ conflict: String ) throws -> MDBQuery {
        queryType = .MULTI_UPSERT
        self.multiValues = try val.map{ try toValues( $0 ) }
        self.on_conflict = conflict
        return self
    }

    
    public func insert ( _ val: [[String:Any?]] ) throws -> MDBQuery {
        queryType = .MULTI_INSERT
        self.multiValues = try val.map{ try toValues( $0 ) }
        try check_all_rows_has_same_keys( )
        return self
    }
    
    func check_all_rows_has_same_keys ( ) throws
    {
        if multiValues.isEmpty {
            return
        }
        
        let keys = Set( multiValues[ 0 ].keys )
        
        for i in 1..<multiValues.count {
            let row_keys = Set( multiValues[ i ].keys )
            let cnt = keys.intersection( keys ).count
            
            if cnt != keys.count || row_keys.count != cnt {
                throw MDBError.general( "The inserted dictionary does not have the same keys: \(row_keys) vs first row keys: \(keys). Missing \(keys.symmetricDifference( row_keys ))." )
            }
        }
    }

    
    public func update ( _ val: [[String:Any?]], _ fields: [String] ) throws -> MDBQuery {
        queryType = .MULTI_UPDATE
        self.multiValues = try val.map{ try toValues( $0 ) }
        try check_all_rows_has_same_keys( )

        for f in fields {
            let without_casting = f.components(separatedBy: "::")
            let casting = without_casting.count > 1 ? "::\(without_casting[ 1 ])" : ""
            try andWhere( "\(table).\(without_casting[0])", MDBValue( raw: "s.\"\(without_casting[0])\"\(casting)" ) )
        }
        
        return self
    }

    
    public func delete ( ) -> MDBQuery {
        queryType = .DELETE
        return self
    }
    
    
    
    //
    // DISTINCT ON
    //
    
    @discardableResult
    public func distinctOn ( _ cols: [String] ) -> MDBQuery {
        distinct_on = cols
        return self
    }
    
    public func distinctOnRaw ( ) -> String {
        return distinct_on.count == 0 ?
                 ""
               : "distinct on (" + MDBValue( fromTable:distinct_on.joined( separator: "," ) ).value + ") "
    }
    
    //
    // WHERE
    //
    
    @discardableResult
    public func beginGroup ( ) -> MDBQuery {
        super.begin_group()
        return self
    }

    @discardableResult
    public func endGroup ( ) -> MDBQuery {
        super.end_group()

        return self ;
    }
    

    @discardableResult
    public func addWhereLine( _ where_op: WHERE_OPERATOR, _ field: Any, _ op: WHERE_LINE_OPERATOR, _ value: Any? ) throws -> MDBQuery {
        try super.add_where_line( where_op, field, op, value )

        return self
    }

    @discardableResult
    public func andWhereRaw ( _ raw: String ) -> MDBQuery {
        return try! addWhereLine( .AND, "", WHERE_LINE_OPERATOR.RAW, MDBValue.init(raw: raw) )
    }

    @discardableResult
    public func orWhereRaw ( _ raw: String ) -> MDBQuery {
        return try! addWhereLine( .OR, "", WHERE_LINE_OPERATOR.RAW, MDBValue.init(raw: raw) )
    }

    @discardableResult
    public func andWhereNULL ( _ field: String ) -> MDBQuery {
        return try! addWhereLine( .AND, field, WHERE_LINE_OPERATOR.IS, try! MDBValue( nil ) )
    }

    @discardableResult
    public func orWhereNULL ( _ field: String ) throws -> MDBQuery {
        return try! addWhereLine( .OR, field, WHERE_LINE_OPERATOR.IS, try! MDBValue( nil ) )
    }

    @discardableResult
    public func andWhereNotNULL ( _ field: String ) -> MDBQuery {
        return try! addWhereLine( .AND, field, WHERE_LINE_OPERATOR.IS_NOT, try! MDBValue( nil ) )
    }

    @discardableResult
    public func orWhereNotNULL ( _ field: String ) -> MDBQuery {
        return try! addWhereLine( .OR, field, WHERE_LINE_OPERATOR.IS_NOT, try! MDBValue( nil ) )
    }

    @discardableResult
    public func andWhereIN ( _ field: String, _ vals: [Any] ) throws -> MDBQuery {
        return try! addWhereLine( .AND, field, WHERE_LINE_OPERATOR.IN, try MDBValue.fromValue( vals ) )
    }

    @discardableResult
    public func andWhereNotIN ( _ field: String, _ vals: [Any] ) throws -> MDBQuery {
        return try! addWhereLine( .AND, field, WHERE_LINE_OPERATOR.NOT_IN, try MDBValue.fromValue( vals ) )
    }

    @discardableResult
    public func orWhereIN ( _ field: String, _ vals: [Any] ) throws -> MDBQuery {
        return try addWhereLine( .OR, field, WHERE_LINE_OPERATOR.IN, try MDBValue.fromValue( vals ) )
    }

    @discardableResult
    public func orWhereNotIN ( _ field: String, _ vals: [Any] ) throws -> MDBQuery {
        return try addWhereLine( .OR, field, WHERE_LINE_OPERATOR.NOT_IN, try MDBValue.fromValue( vals ) )
    }

    @discardableResult
    public func andWhere ( _ field: String, _ value: Any ) throws -> MDBQuery {
        return try addWhereLine( .AND, field, .EQ, value )
    }

    @discardableResult
    public func andWhere ( _ field: String, _ op: WHERE_LINE_OPERATOR, _ value: Any ) throws -> MDBQuery {
        return try addWhereLine( .AND, field, op, value )
    }

    @discardableResult
    public func orWhere ( _ field: String, _ value: Any ) throws -> MDBQuery {
        return try addWhereLine( .OR, field, .EQ, value )
    }
    
    @discardableResult
    public func orWhere ( _ field: String, _ op: WHERE_LINE_OPERATOR, _ value: Any ) throws -> MDBQuery {
        return try addWhereLine( .OR, field, op, value )
    }

    //
    // ORDER BY
    //
    
    @discardableResult
    public func groupBy ( _ field: String, _ dir: ORDER_BY_DIRECTION = .ASC ) -> MDBQuery {
        _group_by.append( field )
        return self
    }
    
    
    func groupRaw ( ) -> String {
        return _group_by.count > 0 ? "GROUP BY " + _group_by.joined(separator: ",") : ""
    }
    

    //
    // ORDER BY
    //
    
    @discardableResult
    public func orderBy ( _ field: String, _ dir: ORDER_BY_DIRECTION = .ASC ) -> MDBQuery {
        order.append( OrderBy( field: MDBValue( fromTable: field ).value, dir: dir ) )
        return self
    }

    @discardableResult
    public func orderBy ( raw:String ) -> MDBQuery {
        order.append( OrderBy( raw: raw ) )
        return self
    }
    
    func orderRaw ( ) -> String {
        return order.isEmpty ? "" : "ORDER BY " + order.map{ $0.raw( ) }.joined( separator: "," )
    }

    
    //
    // LIMIT
    //

    public func limit ( _ value: Int32 ) -> MDBQuery
    {
        _limit = value ;
        return self
    }
    
    func limitRaw ( ) -> String { return _limit > 0 ? "LIMIT " + String( _limit ) : "" }
    
    
    //
    // OFFSET
    //

    public func offset ( _ value: Int32 ) -> MDBQuery { _offset = value ; return self }
    func offsetRaw ( ) -> String { return _offset > 0 ? "OFFSET " + String( _offset ) : "" }

    /// Read accessors for dialects living outside the module (Oracle needs
    /// the raw numbers to render OFFSET..FETCH instead of LIMIT/OFFSET).
    public var limitValue:  Int32 { return _limit  }
    public var offsetValue: Int32 { return _offset }
    

    
    @discardableResult
    public func join ( table: String, from: String? = nil, to: String, joinType: JOIN_TYPE = .INNER, as as_what: String? = nil, _ cb: @escaping ( Join ) throws -> Void = { _ in } ) throws -> MDBQuery {
        // Unqualified columns are qualified to keep the ON clause unambiguous:
        // `from` is a column of the joined table (or its alias), `to` a column of the query's base table
        let from_col   = from ?? "id"
        let from_table = MDBValue( fromTable: from_col.contains( "." ) ? from_col : ( as_what ?? table ) + "." + from_col ).value
        let to_table   = MDBValue( fromTable: to.contains( "." ) ? to : ( _alias ?? self.table ) + "." + to ).value
        let new_join   = Join( joinType: joinType, table: table, fromTable: from_table, toTable: to_table, asWhat: as_what )
                                
        try cb( new_join )
        
        let join_already_done = joins.filter { j in j.raw( ) == new_join.raw( ) }.count > 0
        
        if !join_already_done {
            joins.append( new_join )
        }
        
        return self
    }

    
    @discardableResult
    public func join ( table: String, json: String, to: String, joinType: JOIN_TYPE = .INNER, as as_what: String? = nil, _ cb: @escaping ( JoinJSON ) throws -> Void = { _ in }  ) throws -> MDBQuery {
        let to_table   = MDBValue( fromTable: to ).value
        let new_join   = JoinJSON( joinType: joinType, table: table, json: json, toTable: to_table, asWhat: as_what )
        let join_already_done = joins.filter{ j in j.raw( ) == new_join.raw( ) }.count > 0
        
        if !join_already_done {
          joins.append( new_join )
        }
        
        try cb( new_join )
        
        return self
    }

    

    public func property_alias ( _ relation_name: String ) -> String {
        let join_already_done = joins.filter{ j in j.table == relation_name }
        
        return join_already_done.first?.asWhat! ?? relation_name
    }
    
    
    public func mergeValues ( _ moreValues: [ String: Any? ] ) throws -> MDBQuery {
        values.merge( try toValues( moreValues ) ) { (_, new) in new }
        return self
    }
    
    /// Renders the query with the given backend dialect. Throws when the
    /// query uses a construct the dialect can't express (MDBError.unsupported).
    public func rawQuery ( dialect: MDBDialect ) throws -> String {
        return try dialect.render( self )
    }

    /// Legacy entry point: renders with the default ANSI/PostgreSQL dialect,
    /// which is guaranteed not to throw.
    public func rawQuery () -> String {
        return defaultRawQuery()
    }

    public func defaultRawQuery () -> String {
        return ( try? MDBDialect.ansi.render( self ) ) ?? ""
    }

    public func composeQuery ( _ parts: [String?] ) -> String {
        return (parts.filter{ $0 != nil && $0 != "" } as! [String]).joined(separator: " " )
    }
    
    public func valuesRaw ( ) -> String {
        var key_eq_values: [String] = []

        for (key,value) in sortedValues() {
            key_eq_values.append( "\"" + key + "\"=" + value.value )
        }
        
        return key_eq_values.joined(separator: ",")
    }

    public func multiUpdateValuesRaw ( ) -> String {
        var key_eq_values: [String] = []

        if !multiValues.isEmpty {
            for (key,_) in sortedValues( multiValues[ 0 ] ) {
                key_eq_values.append( "\"\(key)\"= s.\"\(key)\"")
            }
        }
        
        return key_eq_values.joined(separator: ",")
    }
    
    // Dictionaries do not respect the declaration order, so we sort the keys to make the generated SQL deterministic
    public func sortedValues ( _ v: MDBValues? = nil ) -> [(key:String,value:MDBValue)] {
        return ( v ?? values ).sorted { (v1,v2) in v1.key < v2.key }
    }
    
    public func valuesFieldsRaw( _ sorted_values: [(key:String,value:MDBValue)] ) -> String {
        return  "(" + (sorted_values.map{ "\"\($0.key)\"" }).joined(separator: ",") + ")"
    }

    public func valuesValuesRaw( _ sorted_values: [(key:String,value:MDBValue)] ) -> String {
        return "(" + (sorted_values.map{ $0.value.value }).joined(separator: ",") + ")"
    }

//
//    func multiValuesKeyValue ( _ sorted_values: [(key:String,value:MDBValue)] ) -> [[(key:String,value:Any)]] {
//        // THIS IS UGLY: During the migration it happens that some entities do have relations and other do not.
//        // When using a multi-insert, the ones that has relations makes "spaces to fill-in" for the other entities
//        func def_value ( col: String ) -> String {
//            return col.starts(with: "_relation") ? "''" : "null"
//        }
//        
//        return  multiValues.map{ row in
//            sorted_values.map{ col in (key: col.key, value:(row[ col.key ]?.value ?? def_value(col: col.key) )) }
//           }
//    }
//
    
    public func multiValuesRaw ( _ sorted_values: [(key:String,value:MDBValue)] ) -> String {
        // THIS IS UGLY: During the migration it happens that some entities do have relations and other do not.
        // When using a multi-insert, the ones that has relations makes "spaces to fill-in" for the other entities
        func def_value ( col: String ) -> String {
            return col.starts(with: "_relation") ? "''" : "null"
        }
        
        return  multiValues.map{ row in
             "(" + sorted_values.map{ col in (row[ col.key ]?.value ?? def_value(col: col.key) ) }.joined(separator: "," ) + ")"
           }.joined(separator: ",")
    }
    
    public func multiExcludedRaw ( _ sorted_values: [(key:String,value:MDBValue)] ) -> String {
        let conflict_keys = Set( on_conflict.components(separatedBy: ",").map{ $0.trimmingCharacters(in: .whitespacesAndNewlines) } )
        
        return sorted_values.filter{ col in !conflict_keys.contains( col.key ) }
                            .map{ col in ("\"\(col.key)\" = excluded.\"\(col.key)\"" ) }.joined(separator: ",")
    }
    
    func multi_insert_cursor ( ) {
        
    }
}
