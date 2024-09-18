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


public struct OrderBy {
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


open class MDBQuery {

    // In some cases like "WHERE IN ()" we can predict the query will not return values
    public var willBeEmpty: Bool = false
    public var table: String = ""
    public var queryType: QUERY_TYPE = .UNKOWN
    public var _where : MDBQueryWhere? //= MDBQueryWhere( )
    public var _selectFields: [ String ] = []
    public var values: MDBValues = [:]
    public var multiValues: [MDBValues] = []
    public var _returning: [String] = []
    public var _limit: Int32 = 0
    public var _offset: Int32 = 0
    public var order: [ OrderBy ] = []
    public var joins: [ Join ] = []
    public var _group_by: [ String ] = []
    public var on_conflict: String = ""
    public var distinct_on: [String] = []
    public var _alias: String? = nil
    public var unitTest: Bool = false
    public var usingQueryBuilderV2: Bool = false
    
    public var delegate: MDBQueryProtocol? = nil  // used by Oracle specialization only. Try to move to the QueryEncoder? 
    
    public init( _ table: String ) {
        self.table = table
    }
    
    // xxx que hacemos con los join? No pasan los tests

    // xxx y esto ??? 
    public static func beginTransactionStament() -> String { return( "BEGIN TRANSACTION" ) }
    public static func commitTransactionStament() -> String { return( "COMMIT TRANSACTION" ) }
    public static func rollbackTransactionStament() -> String { return( "ROLLBACK TRANSACTION" ) }
    
   
    // xxx y esto? 
    public func property_alias ( _ relation_name: String ) -> String {
        let join_already_done = joins.filter{ j in j.table == relation_name }
        
        return join_already_done.first?.asWhat! ?? relation_name
    }
    
    // xxx y esto?
    public func mergeValues ( _ moreValues: [ String: Any? ] ) throws -> MDBQuery {
        values.merge( try toValues( moreValues ) ) { (_, new) in new }  // in case of collision, the value in the parameter dictionary replaces the one in values
        return self
    }
    

}
