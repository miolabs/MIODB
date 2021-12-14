//
//  MDBQuery.swift
//  MDBPostgresSQL
//
//  Created by Javier Segura Perez on 28/12/2019.
//  Copyright © 2019 Javier Segura Perez. All rights reserved.
//

import Foundation

public enum QUERY_TYPE
{
    case UNKOWN
    case SELECT
    case INSERT
    case MULTI_INSERT
    case UPDATE
    case DELETE
    case UPSERT
    case MULTI_UPSERT
}


public enum ORDER_BY_DIRECTION: String {
    case ASC  = "ASC"
    case DESC = "DESC"
}


struct OrderBy {
    var field: String
    var dir: ORDER_BY_DIRECTION = .ASC
    
    func raw ( ) -> String {
        return field + " \(dir)"
    }
}

public protocol MDBQueryProtocol
{
    func upsert( table: String, values: [(key:String,value:MDBValue)], conflict: String, returning: [String] ) -> String?
    // func multi_upsert( table: String, keys: [(key:String,value:MDBValue)], values: [[(key:String,value:MDBValue)]], conflict: String, returning: [String] ) -> String?
}


public class MDBQuery: MDBQueryWhere {

    // In some cases like "WHERE IN ()" we can predict the query will not return values
    public var willBeEmpty: Bool = false
    var table: String = ""
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
    var on_conflict: String = ""
    public var _alias: String? = nil
    
    public var delegate: MDBQueryProtocol? = nil
    
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
    
    func returningRaw ( ) -> String
    {
        return _returning.isEmpty ? "" : "RETURNING " + _returning.joined( separator: "," )
    }
    
    
    @discardableResult
    public func select ( _ args: Any... ) -> MDBQuery {
        for field in args {
            let select_field: MDBValue = field is MDBValue ? field as! MDBValue : MDBValue( fromTable: field as! String )
            
            _selectFields.append( select_field.value )
        }
                
        queryType = .SELECT

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
        return self
    }

    public func delete ( ) -> MDBQuery {
        queryType = .DELETE
        return self
    }
    
    
    //
    // WHERE
    //
    
    public func beginGroup ( ) -> MDBQuery {
        super.begin_group()
        return self
    }

    public func endGroup ( ) -> MDBQuery {
        super.end_group()

        return self ;
    }
    
    func whereRaw ( ) -> String {
        return _whereCond != nil ? "WHERE " + _whereCond!.raw( ) : ""
    }

    public func addWhereLine( _ where_op: WHERE_OPERATOR, _ field: Any, _ op: WHERE_LINE_OPERATOR, _ value: Any? ) throws -> MDBQuery {
        try super.add_where_line( where_op, field, op, value )

        return self
    }

    public func andWhereRaw ( _ raw: String ) -> MDBQuery {
        return try! addWhereLine( .AND, "", WHERE_LINE_OPERATOR.RAW, MDBValue.init(raw: raw) )
    }

    public func orWhereRaw ( _ raw: String ) -> MDBQuery {
        return try! addWhereLine( .OR, "", WHERE_LINE_OPERATOR.RAW, MDBValue.init(raw: raw) )
    }

    
    public func andWhereNULL ( _ field: String ) -> MDBQuery {
        return try! addWhereLine( .AND, field, WHERE_LINE_OPERATOR.IS, try! MDBValue( nil ) )
    }

    public func orWhereNULL ( _ field: String ) throws -> MDBQuery {
        return try! addWhereLine( .OR, field, WHERE_LINE_OPERATOR.IS, try! MDBValue( nil ) )
    }

    public func andWhereNotNULL ( _ field: String ) -> MDBQuery {
        return try! addWhereLine( .AND, field, WHERE_LINE_OPERATOR.IS_NOT, try! MDBValue( nil ) )
    }

    public func orWhereNotNULL ( _ field: String ) -> MDBQuery {
        return try! addWhereLine( .OR, field, WHERE_LINE_OPERATOR.IS_NOT, try! MDBValue( nil ) )
    }

    public func andWhereIN ( _ field: String, _ vals: [Any] ) throws -> MDBQuery {
        return try! addWhereLine( .AND, field, WHERE_LINE_OPERATOR.IN, try MDBValue.fromValue( vals ) )
    }

    public func andWhereNotIN ( _ field: String, _ vals: [Any] ) throws -> MDBQuery {
        return try! addWhereLine( .AND, field, WHERE_LINE_OPERATOR.NOT_IN, try MDBValue.fromValue( vals ) )
    }

    public func orWhereIN ( _ field: String, _ vals: [Any] ) throws -> MDBQuery {
        return try addWhereLine( .OR, field, WHERE_LINE_OPERATOR.IN, try MDBValue.fromValue( vals ) )
    }

    public func orWhereNotIN ( _ field: String, _ vals: [Any] ) throws -> MDBQuery {
        return try addWhereLine( .OR, field, WHERE_LINE_OPERATOR.NOT_IN, try MDBValue.fromValue( vals ) )
    }

    public func andWhere ( _ field: String, _ value: Any ) throws -> MDBQuery {
        return try addWhereLine( .AND, field, .EQ, value )
    }

    public func andWhere ( _ field: String, _ op: WHERE_LINE_OPERATOR, _ value: Any ) throws -> MDBQuery {
        return try addWhereLine( .AND, field, op, value )
    }

    public func orWhere ( _ field: String, _ value: Any ) throws -> MDBQuery {
        return try addWhereLine( .OR, field, .EQ, value )
    }
    
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
    

    
    @discardableResult
    public func join ( table: String, from: String? = nil, to: String, joinType: JOIN_TYPE = .INNER, as as_what: String? = nil, _ cb: @escaping ( Join ) throws -> Void = { _ in } ) throws -> MDBQuery {
        let from_table = MDBValue( fromTable: from != nil ? from! : table + ".id" ).value
        let to_table   = MDBValue( fromTable: to ).value
        let new_join   = Join( joinType: joinType, table: table, fromTable: from_table, toTable: to_table, asWhat: as_what )
        let join_already_done = joins.filter{ j in j.raw( ) == new_join.raw( ) }.count > 0
        
        if !join_already_done {
          joins.append( new_join )
        }
        
        try cb( new_join )
        
        return self
    }

    
    @discardableResult
    public func join ( table: String, json: String, to: String, joinType: JOIN_TYPE = .INNER, as as_what: String? = nil ) -> MDBQuery {
        let to_table   = MDBValue( fromTable: to ).value
        let new_join   = JoinJSON( joinType: joinType, table: table, json: json, toTable: to_table, asWhat: as_what )
        let join_already_done = joins.filter{ j in j.raw( ) == new_join.raw( ) }.count > 0
        
        if !join_already_done {
          joins.append( new_join )
        }
        return self
    }

    
    func joinRaw ( ) -> String {
        return joins.map{ $0.raw( ) }.joined( separator: " " )
    }

    public func property_alias ( _ relation_name: String ) -> String {
        let join_already_done = joins.filter{ j in j.table == relation_name }
        
        return join_already_done.first?.asWhat! ?? relation_name
    }
    
    
    public func mergeValues ( _ moreValues: [ String: Any? ] ) throws -> MDBQuery {
        values.merge( try toValues( moreValues ) ) { (_, new) in new }
        return self
    }
    
    public func rawQuery() -> String {
        switch queryType {
            case .UNKOWN:
                 return ""
            case .SELECT:
                 return composeQuery( [ "SELECT " + selectFieldsRaw( ) + " FROM " + MDBValue( fromTable: table ).value
                                      , aliasRaw( )
                                      , joinRaw( )
                                      , whereRaw( )
                                      , groupRaw( )
                                      , orderRaw( )
                                      , limitRaw( )
                                      , offsetRaw( )
                                      ] )

            case .UPSERT:
                let sorted_values = sortedValues()
                let table = MDBValue( fromTable: table ).value

                return sorted_values.isEmpty ? ""
                     : delegate?.upsert( table: table, values: sorted_values, conflict: on_conflict, returning: _returning ) ??
                       composeQuery( [ "INSERT INTO " + table
                                     , valuesFieldsRaw( sorted_values )
                                     , "VALUES"
                                     , valuesValuesRaw( sorted_values )
                                     , "ON CONFLICT (" + on_conflict + ") DO UPDATE SET"
                                     , valuesRaw()
                                     , returningRaw()
                                     ] )
            case .MULTI_UPSERT:
                 let sorted_values = sortedValues( multiValues.count > 0 ? multiValues[ 0 ] : [:] )
                 return sorted_values.isEmpty ? ""
//                      : delegate?.multi_upsert( table: table, keys: sorted_values, values: multiValuesKeyValue( sorted_values ), conflict: on_conflict, returning: _returning ) ??
                      : composeQuery( [ "INSERT INTO " + MDBValue( fromTable: table ).value
                                      , valuesFieldsRaw( sorted_values )
                                      , "VALUES"
                                      , multiValuesRaw( sorted_values )
                                      , "ON CONFLICT (" + on_conflict + ") DO UPDATE SET"
                                      , multiExcludedRaw( sorted_values )
                                      , returningRaw()
                                      ] )

            case .INSERT:
                 let sorted_values = sortedValues()
                 
                 return sorted_values.isEmpty ? ""
                      : composeQuery( [ "INSERT INTO " + MDBValue( fromTable: table ).value
                                      , valuesFieldsRaw( sorted_values )
                                      , "VALUES"
                                      , valuesValuesRaw( sorted_values )
                                      , whereRaw( )
                                      , returningRaw()
                                      ] )
            case .MULTI_INSERT:
                 let sorted_values = sortedValues( multiValues.count > 0 ? multiValues[ 0 ] : [:] )
                 return sorted_values.isEmpty ? ""
                      : composeQuery( [ "INSERT INTO " + MDBValue( fromTable: table ).value
                                      , valuesFieldsRaw( sorted_values )
                                      , "VALUES"
                                      , multiValuesRaw( sorted_values )
                                      , whereRaw( )
                                      , returningRaw()
                                      ] )
            case .UPDATE:
                 return values.isEmpty ? ""
                      : composeQuery( [ "UPDATE " + MDBValue( fromTable: table ).value + " SET"
                                      , valuesRaw( )
                                      , whereRaw( )
                                      , returningRaw()
                                      ] )
            case .DELETE:
                 return composeQuery( [ "DELETE FROM " + MDBValue( fromTable: table ).value
                                      , whereRaw( )
                                      ] )
        }
    }

    func composeQuery ( _ parts: [String?] ) -> String {
        return (parts.filter{ $0 != nil && $0 != "" } as! [String]).joined(separator: " " )
    }
    
    public func valuesRaw( ) -> String {
        var key_eq_values: [String] = []

        for (key,value) in sortedValues() {
            key_eq_values.append( "\"" + key + "\"=" + value.value )
        }
        
        return key_eq_values.joined(separator: ",")
    }

    // Unfortunatelly dictionaries do not respect the declaration order, so we need to sorted them for testing
    func sortedValues ( _ v: MDBValues? = nil ) -> [(key:String,value:MDBValue)] {
        #if testing
        return ( v == nil ? values : v! ).sorted { (v1,v2) in v1.key < v2.key }
        #else
        return (v ?? values).map { (key, value) in (key, value) }
        #endif
    }
    
    func valuesFieldsRaw( _ sorted_values: [(key:String,value:MDBValue)] ) -> String {
        return  "(" + (sorted_values.map{ "\"\($0.key)\"" }).joined(separator: ",") + ")"
    }

    func valuesValuesRaw( _ sorted_values: [(key:String,value:MDBValue)] ) -> String {
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
    
    func multiValuesRaw ( _ sorted_values: [(key:String,value:MDBValue)] ) -> String {
        // THIS IS UGLY: During the migration it happens that some entities do have relations and other do not.
        // When using a multi-insert, the ones that has relations makes "spaces to fill-in" for the other entities
        func def_value ( col: String ) -> String {
            return col.starts(with: "_relation") ? "''" : "null"
        }
        
        return  multiValues.map{ row in
             "(" + sorted_values.map{ col in (row[ col.key ]?.value ?? def_value(col: col.key) ) }.joined(separator: "," ) + ")"
           }.joined(separator: ",")
    }
    
    func multiExcludedRaw ( _ sorted_values: [(key:String,value:MDBValue)] ) -> String {
        let conflict_keys = Set( on_conflict.components(separatedBy: ",").map{ $0.trimmingCharacters(in: .whitespacesAndNewlines) } )
        
        return sorted_values.filter{ col in !conflict_keys.contains( col.key ) }
                            .map{ col in ("\"\(col.key)\" = excluded.\"\(col.key)\"" ) }.joined(separator: ",")
    }
}
