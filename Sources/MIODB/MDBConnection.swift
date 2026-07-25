//
//  MDBConnection.swift
//  MDBPostgresSQL
//
//  Created by Javier Segura Perez on 27/12/2019.
//  Copyright © 2019 Javier Segura Perez. All rights reserved.
//

import Foundation

/// Base connection factory. Holds only what every backend has: an identity,
/// the database name (for embedded backends like SQLite this is the file
/// path) and the pool bookkeeping. Network backends that need credentials or
/// schemes subclass `MDBNetworkConnection` instead.
open class MDBConnection
{
    public var identifier:String
    public var label:String
    public var poolID:String?

    public var database:String?
    public var userInfo:[String:Any]?

    private let idleTimerQueue = DispatchQueue(label: "com.mdbconnection.idletimer")

    var connectionNumber:Int = 0
    private var _isExecuting = false
    private var _idleTimeInSeconds:Int = 0

    var isExecuting: Bool {
        get { idleTimerQueue.sync { _isExecuting } }
        set { idleTimerQueue.sync { _isExecuting = newValue } }
    }
    var idleTimeInSeconds: Int {
        get { idleTimerQueue.sync { _idleTimeInSeconds } }
        set { idleTimerQueue.sync { _idleTimeInSeconds = newValue } }
    }

    public init ( database:String? = nil
         , identifier:String = "-1"
         , label:String = "mdb-connection"
         , userInfo:[String:Any]? = nil ) {
        self.database = database
        self.identifier = identifier
        self.label = label
        self.userInfo = userInfo
    }

    public convenience init(connection:MDBConnection) {
        self.init( database: connection.database
                   , identifier: connection.identifier
                   , label: connection.label
                   , userInfo: connection.userInfo )
         self.poolID = connection.poolID
     }

    open func create ( _ to_db: String? = nil, identifier: String? = nil, label: String? = nil, delegate: MDBDelegate? = nil ) throws -> MIODB { throw MDBError.createNotImplemented( ) }

    open func startIdleTimer ( ) {
        idleTimerQueue.sync {
            _isExecuting = false
            _idleTimeInSeconds = 0
        }
    }

    open func stopIdleTimer ( ) {
        idleTimerQueue.sync {
            _isExecuting = true
            _idleTimeInSeconds = 0
        }
    }

    open func updateIdleTime(seconds:Int) {
        idleTimerQueue.sync {
            if _isExecuting { return }
            _idleTimeInSeconds += seconds
        }
    }
}
