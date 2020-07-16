//
//  MDB.swift
//  MIODB
//
//  Created by Javier Segura Perez on 24/12/2019.
//  Copyright © 2019 Javier Segura Perez. All rights reserved.
//

import Foundation

enum MIODBError: Error {
    case cantBeginTransactionWhileInsideTransaction
    case cantEndTransactionWhileNotInsideTransaction
}

extension MIODBError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case .cantBeginTransactionWhileInsideTransaction:
            return "[MIODBError] Can't begin transaction, another transaction is already in progress."
        case .cantEndTransactionWhileNotInsideTransaction:
            return "[MIODBError] Can't end transaction if a transaction is not in progress."
        }
    }
}

open class MIODB {

    public var host:String?
    public var port:Int32?
    public var user:String?
    public var password:String?
    public var database:String?
    public var connectionString:String?
    public var isInsideTransaction : Bool = false
    
    var transactionQueryStrings : [String] = []
    
    public convenience init(connection:MDBConnection){
         self.init( )
         self.host = connection.host
         self.port = connection.port
         self.user = connection.user
         self.password = connection.password
         self.database = connection.database
     }
    
    public init ( ) {
    }
    
    open func connect() throws {
    }

    open func disconnect(){
        
    }

    @discardableResult open func fetch ( _ table: String, _ id: String ) throws -> [String : Any?]? {
        let enttty = try execute( MDBQuery( table ).select().andWhere( "id", .EQ, id ) )!

        return enttty.count > 0 ? enttty.first! : nil
    }
    
    @discardableResult open func execute(_ query: MDBQuery ) throws -> [[String : Any?]]? {
        return try executeQueryString( query.rawQuery() )
    }

    @discardableResult open func executeQueryString(_ query:String) throws -> [[String : Any?]]? {
        return []
    }
    
    // Build query methods
    open func query() -> MDBQuery {
        let query = MDBQuery(db: self)
        return query
    }
    
    open func like(key:String, value:String) -> String {
        return "\(key) LIKE '%\(value)%'"
    }
    
    open func beginTransaction() throws {
        if isInsideTransaction {
            throw MIODBError.cantBeginTransactionWhileInsideTransaction
        }
        isInsideTransaction = true
    }

    open func commitTransaction() throws {
        if isInsideTransaction == false {
            throw MIODBError.cantEndTransactionWhileNotInsideTransaction
        }
        
        isInsideTransaction = false
        
        _ = try executeQueryString(transactionQueryStrings.joined(separator: "; "))
        transactionQueryStrings.removeAll()
    }
    
    open func pushQueryString(_ query : String) {
        transactionQueryStrings.append(query)
    }
}
