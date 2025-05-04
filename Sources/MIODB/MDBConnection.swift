//
//  MDBConnection.swift
//  MDBPostgresSQL
//
//  Created by Javier Segura Perez on 27/12/2019.
//  Copyright © 2019 Javier Segura Perez. All rights reserved.
//

import Foundation

open class MDBConnection
{
    public var identifier:String
    public var label:String
    public var poolID:String?

    public var host:String?
    public var port:Int32?
    public var user:String?
    public var password:String?
    public var database:String?
    public var scheme:String?
    public var userInfo:[String:Any]?
        
    var connectionNumber:Int = 0
    var isExecuting = false
    var idleTimeInSeconds:Int = 0
    
    public init ( host:String? = nil
         , port:Int32? = nil
         , user:String? = nil
         , password:String? = nil
         , database:String? = nil
         , scheme:String? = nil
         , identifier:String = "-1"
         , label:String = "mdb-connection"
         , userInfo:[String:Any]? = nil ) {
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.scheme = scheme
        self.identifier = identifier
        self.label = label
        self.userInfo = userInfo
    }
    
    
    public convenience init(connection:MDBConnection) {
        self.init( host: connection.host
                   , port: connection.port
                   , user: connection.user
                   , password: connection.password
                   , database: connection.database
                   , scheme: connection.scheme
                   , identifier: connection.identifier
                   , label: connection.label
                   , userInfo: connection.userInfo )
         self.poolID = connection.poolID
     }
    
    open func create ( _ to_db: String? = nil, identifier: String? = nil, label: String? = nil ) throws -> MIODB { throw MDBError.createNotImplemented( ) }
    
    open func startIdleTimer ( ) {
        isExecuting = false
        idleTimeInSeconds = 0
    }
    
    open func stopIdleTimer ( ) {
        isExecuting = true
        idleTimeInSeconds = 0
    }
    
    open func updateIdleTime(seconds:Int) {
        if isExecuting { return }
        idleTimeInSeconds += seconds
    }
}
