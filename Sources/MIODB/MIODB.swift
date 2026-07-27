//
//  MDB.swift
//  MIODB
//
//  Created by Javier Segura Perez on 24/12/2019.
//  Copyright © 2019 Javier Segura Perez. All rights reserved.
//

import Foundation
import MIOCore
//import MIOCoreLogger

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
        
    /// The SQL dialect queries are rendered with. Backends whose SQL differs
    /// from the ANSI/PostgreSQL default (SQLite, MySQL, Oracle) override this.
    open var dialect: MDBDialect { return .ansi }

    open func connect( _ to_db: String? = nil ) throws {
//        try changeScheme( scheme )
        try sessionSetup()
        delegate?.didConnect( db: self )
    }

    /// Per-backend session configuration, run on every (re)connect once the
    /// raw connection is established: pragmas (SQLite), statement_timeout
    /// (PostgreSQL), sql_mode (MySQL), NLS formats (Oracle). Overrides decide
    /// per statement whether a failure is fatal (throw) or cosmetic (try?).
    open func sessionSetup ( ) throws { }

    open func disconnect() {
        delegate?.didDisconnect( db: self )
    }

    deinit { disconnect() }
    
//    @discardableResult open func fetch ( _ table: String, _ id: String ) throws -> [String : Any]? {
//        let query = try MDBQuery( table ).select().andWhere( "id", .EQ, id )
//        query.delegate = queryDelegate
//        let entity = try execute( query )!
//
//        return entity.first
//    }
    
    @discardableResult open func execute(_ query: MDBQuery ) throws -> MDBResultSet {
        let result = try executeQuery( query.rawQuery( dialect: dialect ) )
        startIdleTimer()
        return result
    }

    /// Executes a query and returns a lazy result set. Rows keep the raw
    /// server response and convert each cell to its Swift value only when it
    /// is accessed, preserving the column order of the query. Backends
    /// override this; the base implementation returns an empty result.
    @discardableResult open func executeQuery(_ queryString:String) throws -> MDBResultSet {
        return .empty()
    }

    /// Legacy API: executes the query and materializes every row into a
    /// dictionary, converting all the values upfront. Kept as a wrapper over
    /// `executeQuery` for old installations — new code should use the lazy
    /// result set instead.
    @available(*, deprecated, message: "Use executeQuery(_:) and the lazy MDBResultSet instead")
    @discardableResult open func executeQueryString(_ query:String) throws -> [[String : Any]]? {
        return try MIOCoreAutoReleasePool {
            try executeQuery( query ).dictionaries()
        }
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
