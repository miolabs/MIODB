//
//  MIONetworkDB.swift
//  MIODB
//
//  Created by Javier Segura Perez on 25/07/2026.
//  Copyright © 2026 Javier Segura Perez. All rights reserved.
//

import Foundation

/// Base class for network database backends. `MIODBPostgreSQL`, MySQL and
/// Oracle subclass this, gaining the credential and scheme capabilities.
/// Embedded backends (SQLite) subclass `MIODB` directly and conform to
/// neither protocol.
open class MIONetworkDB : MIODB, MDBCredentials, MDBSchemes
{
    public var host:String?
    public var port:Int32?
    public var user:String?
    public var password:String?
    public var scheme:String?

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
        self.scheme = scheme
        super.init( database: database, identifier: identifier, label: label, userInfo: userInfo )
    }

    public convenience init ( connection: MDBConnection ) {
        self.init( host: (connection as? MDBCredentials)?.host
                   , port: (connection as? MDBCredentials)?.port
                   , user: (connection as? MDBCredentials)?.user
                   , password: (connection as? MDBCredentials)?.password
                   , database: connection.database
                   , scheme: (connection as? MDBSchemes)?.scheme
                   , identifier: connection.identifier
                   , label: connection.label
                   , userInfo: connection.userInfo )
        self.poolID = connection.poolID
    }

    open func changeScheme ( _ scheme: String? ) throws { self.scheme = scheme }
}
