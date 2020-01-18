//
//  MDBQuery.swift
//  MDBPostgresSQL
//
//  Created by Javier Segura Perez on 28/12/2019.
//  Copyright © 2019 Javier Segura Perez. All rights reserved.
//

import Foundation

public class MDBQuery {

    var db:MIODB!
    var items = [String]()
    var orderBy = [String]()
        
    public convenience init(db:MDB){
        self.init()
        self.db = db
    }
    
    public func append(_ item:String){
        items.append(item)
    }
    
    public func execute() -> [Any]{
        var queryString = items.joined(separator: " ")
        queryString += " " + orderBy.joined(separator: ",")
        return db.executeQueryString(queryString)
    }

    public func selectFields(_ fields:String) -> MDBQuery {
        items.append("SELECT \(fields)")
        return self
    }
    
    public func fromTables(_ tables:String) -> MDBQuery {
        items.append("FROM \(tables)")
        return self
    }
    
    public func whereValues() -> MDBQuery {
        items.append("WHERE")
        return self
    }
    
    public func rawItem(_ item:String) -> MDBQuery {
        items.append(item)
        return self
    }

    public func isNull(field:String) -> MDBQuery {
        items.append("\(field) IS NULL")
        return self
    }
    
    public func isNotNull(field:String) -> MDBQuery {
        items.append("\(field) IS NOT NULL")
        return self
    }
    
    public func equal(field:String, value:Any?) -> MDBQuery {
        
        var valueString = ""
        
        if value == nil {
            valueString = "NULL"
        }
        else if value is String {
            valueString = "'\(value as! String)'"
        }
        else if value is Bool {
            valueString = (value as! Bool) ? "TRUE" : "FALSE"
        }
        else if value is Int {
            valueString = String(value as! Int)
        }
        else if value is Float {
            valueString = String(value as! Float)
        }            
        
        items.append("\(field) = \(valueString)")
        return self
    }
    
    public func like(field:String, value:String?) -> MDBQuery {
        if value == nil {
            return self
        }
        
        items.append(db.like(key: field, value: value!))
        return self
    }
    
    public func andOperator() -> MDBQuery {
        items.append("AND")
        return self
    }

    public func orOperator() -> MDBQuery {
        items.append("OR")
        return self
    }
    
    public func openGroup() -> MDBQuery {
        items.append("(")
        return self
    }

    public func closeGroup() -> MDBQuery {
        items.append(")")
        return self
    }

    public func orderByValues() -> MDBQuery {
        items.append("ORDER BY")
        return self
    }
    
    public func asc(_ field: String) -> MDBQuery {
        orderBy.append("\(field) ASC")
        return self
    }

    public func desc(_ field: String) -> MDBQuery {
        orderBy.append("\(field) DESC")
        return self
    }
    
    public func order(field:String, typeString:String) -> MDBQuery{
        switch typeString.lowercased() {
        case "asc":
            return asc(field)
        
        case "desc":
            return desc(field)
        
        default:
            return self
        }
    }
    
}
