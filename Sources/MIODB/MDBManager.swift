//
//  MDBManager.swift
//  MIODB
//
//  Created by Javier Segura Perez on 14/12/2019.
//  Copyright © 2019 Javier Segura Perez. All rights reserved.
//

import Foundation

enum MDBType {
    case None
    case SQLLite
    case Postgres
    case MySQL
}

public class MDBManager {
    
    public static let shared = MDBManager()
        
    // Initialization
    private init() {}

    var connections: [String:MDBConnection] = [:]
    var pool: [String:[MIODB]] = [:]

    
    public func addConnection( _ connection: MDBConnection, forIdentifier poolID:String ) {
        connections[ poolID ] = connection
    }
    
//    public func addConnection(identifier:String, host:String?, port:Int32?, user:String?, password:String?, database:String?) -> MDBConnection {
//        addConnection(identifier: identifier, host: host, port: port, user: user, password: password, database: database, userInfo: nil)
//    }
//
//    public func addConnection(identifier:String, host:String?, port:Int32?, user:String?, password:String?, database:String?, userInfo:[String:Any]?) -> MDBConnection {
//        var conn = MDBConnection()
//        conn.host = host
//        conn.port = port
//        conn.user = user
//        conn.password = password
//        conn.database = database
//        conn.userInfo = userInfo
//
//        connections[identifier] = conn
//
//        return conn
//    }
    
    public func connection ( _ poolID: String ) throws -> MIODB {
        if pool[ poolID ]?.count ?? 0 > 0 {
            let conn = pool[ poolID ]!.first!
            
            pool[ poolID ]!.remove(at:0) // .dropFirst( )
            
            return conn
        }
        
        guard let factory = connections[ poolID ] else {
            throw MDBError.invalidPoolID( poolID )
        }
        
        let conn = try factory.create( )
        conn.poolID = poolID
        
        return conn
    }
    
    
    public func release ( _ db: MIODB ) {
        if let poolID = db.poolID {
            
            if pool[ poolID ] == nil { pool[ poolID ] = [] }
            
            pool[ poolID ]!.append( db )
        }
    }
}
