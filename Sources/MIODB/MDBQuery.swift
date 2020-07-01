//
//  MDBQuery.swift
//  MDBPostgresSQL
//
//  Created by Javier Segura Perez on 28/12/2019.
//  Copyright © 2019 Javier Segura Perez. All rights reserved.
//

import Foundation

public class MDBQuery {

    public enum OrderType {
        case Asc
        case Desc
    }
    
    public enum StamentType {
        case select
        case insert
        case update
        case delete
    }

    
    var db:MIODB!
    var currentStatment = StamentType.select
    
    var insertTable:String = ""
    var insertFields = [String]()
    var insertValues = [String]()
    
    var updateTable:String = ""
    var updateValues = [String]()
    
    public var items = [String]()
    var useOrderBy = false
    var orderBy = [String]()
    var extras = [String]()
        
    public convenience init(db:MIODB){
        self.init()
        self.db = db
    }
    
    public func append(_ item:String){
        items.append(item)
    }
    
    // TODO: Return here [[String : Any]], so we don't have to make always a cast
    public func execute() throws -> [Any]{
        var queryString = items.joined(separator: " ")
        if useOrderBy {
            queryString += " ORDER BY " + orderBy.joined(separator: ",")
        }
        return try db.executeQueryString(queryString)
    }

    public func selectFields(_ fields:String) -> MDBQuery {
        items.append("SELECT \(fields)")
        return self
    }
    
    public func fromTable(_ table:String) -> MDBQuery {
        items.append("FROM \"\(table)\"")
        return self
    }
    
    /// If you use this function, be responsible about quoting the table names
    public func fromTables(_ tables:String) -> MDBQuery {
        items.append("FROM \(tables)")
        return self
    }
    
    public func insertInto(_ table:String) -> MDBQuery {
        insertTable = table
        currentStatment = .insert
        return self
    }
        
    public func field(_ field:String, value:Any?) -> MDBQuery {
        
        var safeValue = value
        if let stringValue = safeValue as? String {
            safeValue = "'\(stringValue.replacingOccurrences(of: "'", with: "''"))'"
        } else if value is NSNull {
            safeValue = nil
        }
        
        switch currentStatment {
        case .insert:
            if safeValue == nil {
                return self
            }
            insertFields.append("\"\(field)\"")
            insertValues.append("\(safeValue!)")

        case .update:
            if value == nil {
                updateValues.append("\"\(field)\"=NULL")
            } else {
                updateValues.append("\"\(field)\"=\(safeValue!)")
            }
            
        default:
            print("Not implemented")
        }
                
        return self
    }
    
    public func insert() throws {
        var queryString = "INSERT INTO \"\(insertTable)\""
        queryString += " (" + insertFields.joined(separator: ",") + ")"
        queryString += " VALUES (" + insertValues.joined(separator: ",") + ")"
        _ = try db.executeQueryString(queryString)
    }
    
    public func insertAndReturnFieldValue(_ field:String) throws -> Any {
        var queryString = "INSERT INTO \"\(insertTable)\""
        queryString += " (" + insertFields.joined(separator: ",") + ")"
        queryString += " VALUES (" + insertValues.joined(separator: ",") + ")"
        let items = try db.executeQueryString(queryString + " RETURNING \(field)") as! [[String:Any]]
                                
        return items[0][field]!
    }

    
    public func updateTo(_ table:String) -> MDBQuery {
        updateTable = table
        currentStatment = .update
        return self
    }
    
    public func update() throws {
        var queryString = "UPDATE \"\(updateTable)\" SET"
        queryString += " " + updateValues.joined(separator: ",")
        queryString += " " + items.joined(separator: " ")
        _ = try db.executeQueryString(queryString)
    }
    
    public func deleteFrom(_ table:String) -> MDBQuery {
        items.append("DELETE FROM \"\(table)\"")
        return self
    }
    
    public func delete() throws {
        var queryString = items.joined(separator: " ")
        queryString += " " + orderBy.joined(separator: ",")
        _ = try db.executeQueryString(queryString)
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
            items.append("\(field) IS NULL")
            return self
        }
        else if value is String {
            valueString = "'\(value as! String)'"
        }
        else if value is Bool {
            valueString = (value as! Bool) ? "TRUE" : "FALSE"
        }
        else if value is Int8 {
                valueString = String(value as! Int8)
        }
        else if value is Int16 {
            valueString = String(value as! Int16)
        }
        else if value is Int {
            valueString = String(value as! Int)
        }
        else if value is Int32 {
            valueString = String(value as! Int32)
        }
        else if value is Int64 {
            valueString = String(value as! Int64)
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
        //items.append("ORDER BY")
        useOrderBy = true
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
    
    public func order(field:String, type:OrderType) -> MDBQuery{
        switch type {
        case .Asc:
            return asc(field)
        
        case .Desc:
            return desc(field)
        }
    }
    
    public func limit(_ rows:Int) -> MDBQuery {
        let rowsString = String(rows)
        extras.append("LIMIT \(rowsString)")
        return self
    }
}
