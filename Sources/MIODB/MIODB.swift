//
//  MDB.swift
//  MIODB
//
//  Created by Javier Segura Perez on 24/12/2019.
//  Copyright © 2019 Javier Segura Perez. All rights reserved.
//

import Foundation

open class MIODB: MDBConnection {
    public var connectionString:String?
    
//    public var isInsideTransaction : Bool = false
//    var transactionQueryStrings : [String] = []
        
    open func connect() throws { }
    open func disconnect() { }
    open func changeScheme( _ schema: String? ) throws { }

    @discardableResult open func fetch ( _ table: String, _ id: String ) throws -> [String : Any?]? {
        let entity = try execute( MDBQuery( table ).select().andWhere( "id", .EQ, id ) )!

        return entity.first
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
    
//    open func transactionBegin ( ) throws {
//        if isInsideTransaction {
//           throw MDBError.cantBeginTransactionWhileInsideTransaction
//        }
//
//        isInsideTransaction = true
//        transactionQueryStrings.append( "begin transaction" )
//    }
//
//    open func transactionCommit ( ) throws {
//        if !isInsideTransaction {
//           throw MDBError.cantEndTransactionWhileNotInsideTransaction
//        }
//
//        isInsideTransaction = false
//        transactionQueryStrings.append( "commit" )
//
//        try executeQueryString( transactionQueryStrings.joined(separator: ";") )
//
//        transactionQueryStrings = []
//    }
//
//    open func transactionRollback ( ) throws {
//        if !isInsideTransaction {
//           throw MDBError.cantEndTransactionWhileNotInsideTransaction
//        }
//
//        isInsideTransaction = false
//        transactionQueryStrings = []
//    }
}
