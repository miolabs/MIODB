//
//  MDBManager.swift
//  MIODB
//
//  Created by Javier Segura Perez on 14/12/2019.
//  Copyright © 2019 Javier Segura Perez. All rights reserved.
//

import Foundation
import MIOCore

enum MDBType
{
    case None
    case SQLLite
    case Postgres
    case MySQL
}

public class MDBManager 
{
    public static let shared = MDBManager()
        
    // Initialization
    private init() {
//        startIdleTimer()
    }

    static let connectionQueue = DispatchQueue(label: "com.miolabs.connection.queue")
    
    var connections: [String:MDBConnection] = [:]
    var pool: [String:[MIODB]] = [:]

    
    public func addConnection( _ connection: MDBConnection, forIdentifier poolID:String ) {
        MDBManager.connectionQueue.sync( flags: .barrier ) {
            self.connections[ poolID ] = connection
        }
        
    }
        
    // db_id = db_59, db_63,...
    // to_db = nil means uses schema. Otherwise connects to the DB in the "db_id" cluster
    public func connection ( _ db_id: String, _ to_db: String? = nil ) throws -> MIODB {
//        var conn:MIODB? = nil
//        let pool_id = to_db ?? db_id
//
//        try MDBManager.connectionQueue.sync( flags: .barrier ) {
//            
//            if self.pool[ pool_id ]?.count ?? 0 > 0 {
//                conn = pool[ pool_id ]!.first!
//                
//                self.pool[ pool_id ]!.remove(at:0) // .dropFirst( )
//            }
//            else {
//                guard let factory = self.connections[ db_id ] else {
//                    throw MDBError.invalidPoolID( db_id )
//                }
//                conn = try factory.create( to_db )
//                conn!.poolID = pool_id
//            }
//        }
//        
//        return conn!
        
        guard let factory = self.connections[ db_id ] else {
            throw MDBError.invalidPoolID( db_id )
        }
        
        return try factory.create( to_db )
    }
    
    
    public func release ( _ db: MIODB ) {
//        MDBManager.connectionQueue.sync( flags: .barrier ) {
//            
//            if let poolID = db.poolID {
//                
//                if self.pool[ poolID ] == nil { self.pool[ poolID ] = [] }
//                
//                self.pool[ poolID ]!.append( db )
//            }
//        }
        db.disconnect()
    }
    
    let timerQueue = DispatchQueue(label: "idle-pool-timer")
    
    func startIdleTimer() {
        /*
        timerQueue.async {
            
            Timer.scheduledTimer(withTimeInterval: 60, repeats: true) { _ in
                
                MDBManager.connectionQueue.sync( flags: .barrier ) {
                    
                    var new_pool:[String:[MIODB]] = [:]
                    var needs_update = false
                    
                    for (poolID, connections) in self.pool {
                        var index = 0
                        for db in connections {
                            db.updateIdleTime(seconds: 60)
                            if db.idleTimeInSeconds > (60 * 10) {
                                db.disconnect()
                                needs_update = true
                            }
                            else {
                                var dbs = new_pool[ poolID ] ?? []
                                dbs.append( db )
                                new_pool[ poolID ] = dbs
                            }
                            index += 1
                        }
                    }
                    
                    if needs_update { 
                        self.pool = new_pool
                    }
                }
            }
            RunLoop.current.run()
        }
         */
    }
}
