//
//  MDB.swift
//  MIODB
//
//  Created by Javier Segura Perez on 24/12/2019.
//  Copyright © 2019 Javier Segura Perez. All rights reserved.
//

import CoreFoundation

open class MDB: NSObject {

    public var host:String?
    public var port:Int32?
    public var user:String?
    public var password:String?
    public var database:String?
    public var schema:String?
    public var connectionString:String?
    
    public convenience init(connection:MDBConnection){
         self.init()
         
         self.host = connection.host
         self.port = connection.port
         self.user = connection.user
         self.password = connection.password
         self.database = connection.database
     }
    
    open func connect(){
    }

    open func connect(schema:String?){
    }
    
    open func disconnect(){
        
    }
    
    open func executeQueryString(_ query:String) -> [Any]{
        return []
    }
    
    open func changeSchema(_ schema:String?){
    }
    
    // Build query methods
    open func query() -> MDBQuery {
        let query = MDBQuery(db: self)
        return query
    }
    
    open func like(key:String, value:String) -> String {
        return "\(key) LIKE '%\(value)%'"
    }

}