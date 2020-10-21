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

public enum JOIN_TYPE: String {
    case INNER = "INNER"
    case LEFT  = "LEFT"
    case RIGHT = "RIGHT"
    case FULL  = "FULL"
}

struct Join {
    var joinType: JOIN_TYPE
    var table: String
    var fromTable: String
    var toTable: String
    var asWhat: String? = nil
    
    func raw ( ) -> String {
        return "\(joinType) JOIN \"\(table)\"" + (asWhat != nil ? " AS \"\(asWhat!)\"" : "") + " ON \(fromTable) = \(toTable)"
    }
}


public class MDBQuery {

    // In some cases like "WHERE IN ()" we can predict the query will not return values
    public var willBeEmpty: Bool = false
    var table: String = ""
    public var queryType: QUERY_TYPE = .UNKOWN
    var _selectFields: [ String ] = []
    public var values: MDBValues = [:]
    public var multiValues: [MDBValues] = []
    var _whereCond: MDBWhereGroup? = nil
    var whereStack: [ MDBWhereGroup ] = []
    var _returning: [String] = []
    var _limit: Int = 0
    var _offset: Int = 0
    var order: [ OrderBy ] = []
    var joins: [ Join ] = []

    // DEPRECATED

    public enum OrderType {
        case Asc
        case Desc
    }
    
    public enum StamentType {
        case select
        case insert
        case update
        case delete
    }

    var db:MIODB!
    var currentStatment = StamentType.select
    
    var insertTable:String = ""
    var insertFields = [String]()
    var insertValues = [String]()
    
    var updateTable:String = ""
    var updateValues = [String]()
    
    public var items = [String]()
    var useOrderBy = false
    var orderBy = [String]()
    var extras = [String]()
        
    public convenience init(db:MIODB){
        self.init( "" )
        self.db = db
    }
    
    // END DEPRECATED
    
    public init( _ table: String ) {
        self.table = table
    }
    
//    public func select ( _ fields: String = "*" ) -> MDBQuery {
//        queryType = .SELECT
//        selectFields = MDBValue( fromTable: fields ).value
//        return self
//    }

    
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
    
    private func whereCond ( ) -> MDBWhere {
        if whereStack.isEmpty {
            _whereCond = MDBWhereGroup( )
            whereStack.append( _whereCond! )
        }
        
        return whereStack.last!.where_fields
    }
    
    public func beginGroup ( ) -> MDBQuery {
        let grp = MDBWhereGroup( )
        whereCond( ).push( grp )
        whereStack.append( grp )
        
        return self ;
    }

    public func endGroup ( ) -> MDBQuery {
        whereStack.removeLast()
        return self ;
    }

    public func addWhereLine( _ where_op: WHERE_OPERATOR, _ field: String, _ op: WHERE_LINE_OPERATOR, _ value: Any? ) throws -> MDBQuery {
        whereCond( ).push( MDBWhereLine( where_op: where_op
                                    , field: MDBValue(fromTable: field).value
                                    , op: op
                                    , value: try MDBValue.fromValue( value ).value ) )
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

    public func orWhere ( _ field: String, _ op: WHERE_LINE_OPERATOR, _ value: Any ) throws -> MDBQuery {
        return try addWhereLine( .OR, field, op, value )
    }

    
    func whereRaw ( ) -> String {
        return _whereCond != nil ? "WHERE " + _whereCond!.raw( ) : ""
    }

    //
    // ORDER BY
    //
    
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

    public func limit ( _ value: Int ) -> MDBQuery
    {
        // DEPRECATED
        let rowsString = String(value)
        extras.append("LIMIT \(rowsString)")
        // END DEPRECATED
        _limit = value ;
        return self
    }
    
    func limitRaw ( ) -> String { return _limit > 0 ? "LIMIT " + String( _limit ) : "" }
    
    
    //
    // OFFSET
    //

    public func offset ( _ value: Int ) -> MDBQuery { _offset = value ; return self }
    func offsetRaw ( ) -> String { return _offset > 0 ? "OFFSET " + String( _offset ) : "" }
    

    
    public func join( table: String, from: String? = nil, to: String, joinType: JOIN_TYPE = .INNER, as as_what: String? = nil ) -> MDBQuery {
        let from_table = MDBValue( fromTable: from != nil ? from! : table + ".id" ).value
        let to_table   = MDBValue( fromTable: to ).value
        let new_join   = Join( joinType: joinType, table: table, fromTable: from_table, toTable: to_table, asWhat: as_what )
        let join_already_done = joins.filter{ j in j.raw( ) == new_join.raw( ) }.count > 0
        
        if !join_already_done {
          joins.append( new_join )
        }
        return self
    }
    
    func joinRaw ( ) -> String {
        return joins.map{ $0.raw( ) }.joined( separator: " " )
    }

    
    
    
    public func mergeValues ( _ moreValues: [ String: Any? ] ) throws -> MDBQuery {
        values.merge( try toValues( moreValues ) ) { (_, new) in new }
        return self
    }
    
    public func rawQuery() -> String {
        switch queryType {
            case .UNKOWN:
                 var queryString = items.joined(separator: " ")
            
                 if useOrderBy {
                    queryString += " ORDER BY " + orderBy.joined(separator: ",")
                 }
            
                 return queryString
            case .SELECT:
                 return composeQuery( [ "SELECT " + selectFieldsRaw( ) + " FROM " + MDBValue( fromTable: table ).value
                                      , joinRaw( )
                                      , whereRaw( )
                                      , orderRaw( )
                                      , limitRaw( )
                                      , offsetRaw( )
                                      ] )
            case .INSERT:
                 let sorted_values = sortedValues()
                 
                 return composeQuery( [ "INSERT INTO " + MDBValue( fromTable: table ).value
                                      , valuesFieldsRaw( sorted_values )
                                      , "VALUES"
                                      , valuesValuesRaw( sorted_values )
                                      , whereRaw( )
                                      , returningRaw()
                                      ] )
            case .MULTI_INSERT:
                 let sorted_values = sortedValues( multiValues.count > 0 ? multiValues[ 0 ] : [:] )
                 return composeQuery( [ "INSERT INTO " + MDBValue( fromTable: table ).value
                                      , valuesFieldsRaw( sorted_values )
                                      , "VALUES"
                                      , multiValuesRaw( sorted_values )
                                      , whereRaw( )
                                      , returningRaw()
                                      ] )
            case .UPDATE:
                 return composeQuery( [ "UPDATE " + MDBValue( fromTable: table ).value + " SET"
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
    
    func valuesRaw( ) -> String {
        var key_eq_values: [String] = []

        for (key,value) in sortedValues() {
            key_eq_values.append( "\"" + key + "\"=" + value.value )
        }
        
        return key_eq_values.joined(separator: ",")
    }

    // Unfortunatelly dictionaries do not respect the declaration order, so we need to sorted them for testing
    func sortedValues ( _ v: MDBValues? = nil ) -> [(key:String,value:MDBValue)] {
        return ( v == nil ? values : v! ).sorted { (v1,v2) in v1.key < v2.key }
    }
    
    func valuesFieldsRaw( _ sorted_values: [(key:String,value:MDBValue)] ) -> String {
        return  "(" + (sorted_values.map{ "\"\($0.key)\"" }).joined(separator: ",") + ")"
    }

    func valuesValuesRaw( _ sorted_values: [(key:String,value:MDBValue)] ) -> String {
        return "(" + (sorted_values.map{ $0.value.value }).joined(separator: ",") + ")"
    }

    
    func multiValuesRaw( _ sorted_values: [(key:String,value:MDBValue)] ) -> String {
        return  multiValues.map{ row in
                                  "(" + sorted_values.map{ col in row[ col.key ]!.value }.joined(separator: "," ) + ")"
                               }.joined(separator: ",")
    }

    // DEPRECATED
    public func join( table: String, fromTable: String, column: String, joinType: String = "INNER" ) -> MDBQuery {
        items.append( "\(joinType) JOIN \"\(table)\" ON \"\(table)\".\"id\" = \"\(fromTable)\".\"\(column)\"" )
        return self
    }

    
    //
    // DEPRECATED
    //

    public func append(_ item:String){
        items.append(item)
    }
    
    public func execute() throws -> [[String : Any?]]? {
        return try db.executeQueryString(rawQuery())
    }
    

    public func selectFields(_ fields:String) -> MDBQuery {
        items.append("SELECT \(fields)")
        return self
    }
    
    public func fromTable(_ table:String) -> MDBQuery {
        self.table = table
        items.append("FROM \"\(table)\"")
        return self
    }
    
    /// If you use this function, be responsible about quoting the table names
    public func fromTables(_ tables:String) -> MDBQuery {
        items.append("FROM \(tables)")
        return self
    }
    
    public func insertInto(_ table:String) -> MDBQuery {
        insertTable = table
        currentStatment = .insert
        return self
    }
        
    public func field(_ field:String, value:Any?) -> MDBQuery {
        
        var safeValue = value
        if let stringValue = safeValue as? String {
            safeValue = "'\(stringValue.replacingOccurrences(of: "'", with: "''"))'"
        } else if value is NSNull {
            safeValue = nil
        }
        
        switch currentStatment {
        case .insert:
            if safeValue == nil {
                return self
            }
            insertFields.append("\"\(field)\"")
            insertValues.append("\(safeValue!)")

        case .update:
            if value == nil {
                updateValues.append("\"\(field)\"=NULL")
            } else {
                updateValues.append("\"\(field)\"=\(safeValue!)")
            }
            
        default:
            print("Not implemented")
        }
                
        return self
    }
    
    public func insert() throws {
        var queryString = "INSERT INTO \"\(insertTable)\""
        queryString += " (" + insertFields.joined(separator: ",") + ")"
        queryString += " VALUES (" + insertValues.joined(separator: ",") + ")"
        _ = try db.executeQueryString(queryString)
    }
    
    public func insertAndReturnFieldValue(_ field:String) throws -> Any {
        var queryString = "INSERT INTO \"\(insertTable)\""
        queryString += " (" + insertFields.joined(separator: ",") + ")"
        queryString += " VALUES (" + insertValues.joined(separator: ",") + ")"
        let items = try db.executeQueryString(queryString + " RETURNING \(field)")! // as [[String:Any]]
                                
        return items[0][field]!
    }

    
    public func updateTo(_ table:String) -> MDBQuery {
        updateTable = table
        currentStatment = .update
        return self
    }
    
    public func update() throws {
        var queryString = "UPDATE \"\(updateTable)\" SET"
        queryString += " " + updateValues.joined(separator: ",")
        queryString += " " + items.joined(separator: " ")
        _ = try db.executeQueryString(queryString)
    }
    
    public func deleteFrom(_ table:String) -> MDBQuery {
        items.append("DELETE FROM \"\(table)\"")
        return self
    }
    
    public func execDelete() throws {
        var queryString = items.joined(separator: " ")
        queryString += " " + orderBy.joined(separator: ",")
        _ = try db.executeQueryString(queryString)
    }
    
    public func whereValues() -> MDBQuery {
        items.append("WHERE")
        return self
    }
    
    public func rawItem(_ item:String) -> MDBQuery {
        items.append(item)
        return self
    }

    public func isNull(field:String) -> MDBQuery {
        items.append("\(field) IS NULL")
        return self
    }
    
    public func isNotNull(field:String) -> MDBQuery {
        items.append("\(field) IS NOT NULL")
        return self
    }
            
    public func equal(field:String, value:Any?) -> MDBQuery {
        
        var valueString = ""
        
        if value == nil {
            items.append("\(field) IS NULL")
            return self
        }
        else if value is String {
            valueString = "'\(value as! String)'"
        }
        else if value is Bool {
            valueString = (value as! Bool) ? "TRUE" : "FALSE"
        }
        else if value is Int8 {
                valueString = String(value as! Int8)
        }
        else if value is Int16 {
            valueString = String(value as! Int16)
        }
        else if value is Int {
            valueString = String(value as! Int)
        }
        else if value is Int32 {
            valueString = String(value as! Int32)
        }
        else if value is Int64 {
            valueString = String(value as! Int64)
        }
        else if value is Float {
            valueString = String(value as! Float)
        }
        
        items.append("\(field) = \(valueString)")
        return self
    }
    
    public func like(field:String, value:String?) -> MDBQuery {
        if value == nil {
            return self
        }
        
        items.append(db.like(key: field, value: value!))
        return self
    }
    
    public func andOperator() -> MDBQuery {
        items.append("AND")
        return self
    }

    public func orOperator() -> MDBQuery {
        items.append("OR")
        return self
    }
    
    public func openGroup() -> MDBQuery {
        items.append("(")
        return self
    }

    public func closeGroup() -> MDBQuery {
        items.append(")")
        return self
    }

    public func orderByValues() -> MDBQuery {
        //items.append("ORDER BY")
        useOrderBy = true
        return self
    }
    
    public func asc(_ field: String) -> MDBQuery {
        orderBy.append("\(field) ASC")
        return self
    }

    public func desc(_ field: String) -> MDBQuery {
        orderBy.append("\(field) DESC")
        return self
    }
    
    public func order(field:String, type:OrderType) -> MDBQuery{
        switch type {
        case .Asc:
            return asc(field)
        
        case .Desc:
            return desc(field)
        }
    }
    
//    public func limit(_ rows:Int) -> MDBQuery {
//        let rowsString = String(rows)
//        extras.append("LIMIT \(rowsString)")
//        return self
//    }
}
