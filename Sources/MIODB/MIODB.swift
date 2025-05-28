//
//  MDB.swift
//  MIODB
//
//  Created by Javier Segura Perez on 24/12/2019.
//  Copyright © 2019 Javier Segura Perez. All rights reserved.
//

import Foundation
import MIOCoreLogger

public protocol MDBDelegate : AnyObject
{
    func didConnect( db: MIODB )
    func didDisconnect( db: MIODB )
}


open class MIODB: MDBConnection
{
    public weak var delegate: MDBDelegate?
    
    public var connectionString:String?
    
//    public var isInsideTransaction : Bool = false
//    var transactionQueryStrings : [String] = []
        
    open func connect( _ to_db: String? = nil ) throws {
//        try changeScheme( scheme )
        delegate?.didConnect( db: self )
    }
    
    open func disconnect() {
        delegate?.didDisconnect( db: self )
    }
    
    open func changeScheme( _ scheme: String? ) throws { self.scheme = scheme }

    deinit { disconnect() }
    
    @discardableResult open func fetch ( _ table: String, _ id: String ) throws -> [String : Any]? {
        let entity = try execute( MDBQuery( table ).select().andWhere( "id", .EQ, id ) )!

        return entity.first
    }
    
    @discardableResult open func execute(_ query: MDBQuery ) throws -> [[String : Any]]? {
        let result = try executeQueryString( query.rawQuery() )
        startIdleTimer()
        return result
    }

    @discardableResult open func executeQueryString(_ query:String) throws -> [[String : Any]]? {
        return []
    }
    
    open func queryWillExecute() { stopIdleTimer() }
    open func queryDidExecute() { startIdleTimer() }
    
    // Build query methods
//    open func query() -> MDBQuery {
//        let query = MDBQuery(db: self)
//        return query
//    }
    
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
