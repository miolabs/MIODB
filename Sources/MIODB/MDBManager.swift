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
    private init() {
        startIdleTimer()
    }

    let connectionQueue = DispatchQueue(label: "com.miolabs.connection.queue")
    
    var connections: [String:MDBConnection] = [:]
    var pool: [String:[MIODB]] = [:]

    
    public func addConnection( _ connection: MDBConnection, forIdentifier poolID:String ) {
        connectionQueue.sync {
            self.connections[ poolID ] = connection
        }
        
    }
        
    public func connection ( _ poolID: String ) throws -> MIODB {
        var conn:MIODB? = nil
        
        try connectionQueue.sync {
        
            if self.pool[ poolID ]?.count ?? 0 > 0 {
                conn = pool[ poolID ]!.first!
                
                self.pool[ poolID ]!.remove(at:0) // .dropFirst( )
            }
            else {
                guard let factory = self.connections[ poolID ] else {
                    throw MDBError.invalidPoolID( poolID )
                }
                
                conn = try factory.create( )
                conn!.poolID = poolID
            }
        }
        
        return conn!
    }
    
    
    public func release ( _ db: MIODB ) {
        connectionQueue.sync {
            
            if let poolID = db.poolID {
                
                if self.pool[ poolID ] == nil { self.pool[ poolID ] = [] }
                
                self.pool[ poolID ]!.append( db )
            }
        }
    }
    
    let timerQueue = DispatchQueue(label: "idle-pool-timer")
    
    func startIdleTimer() {
        
        timerQueue.async {
            
            Timer.scheduledTimer(withTimeInterval: 60, repeats: true) { _ in
                
                self.connectionQueue.sync {
                
                    var new_pool:[String:[MIODB]] = [:]
                    
                    for (poolID, connections) in self.pool {
                                              
                        var new_conns:[MIODB] = []
                        for db in connections {
                            db.updateIdleTime(seconds: 60)
                            if db.idleTimeInSeconds > (60 * 10) {
                                db.disconnect()
                            }
                            else {
                                new_conns.append(db)
                            }
                        }
                        new_pool [poolID ] = new_conns
                    }
                    
                    self.pool = new_pool
                }
            }
            RunLoop.current.run()
        }
    }
}
