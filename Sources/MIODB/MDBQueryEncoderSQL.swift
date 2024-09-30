
import Foundation

// PostgreSQL specific:
    // select for update
    // returning
    // distinct on
    // on conflict (upsert)

public class MDBQueryEncoderSQL {
    let mdbquery : MDBQuery

    public init ( _ mdbquery: MDBQuery ) {
        self.mdbquery = mdbquery
    }

    func tableAliasRaw ( ) -> String { return mdbquery._tableAlias == nil ? "" : "AS \(mdbquery._tableAlias!)" }    

    func returningRaw ( ) -> String
    {
        return mdbquery._returning.isEmpty ? "" : "RETURNING " + mdbquery._returning.joined( separator: "," )
    }

    public func selectFieldsRaw ( ) -> String {
        //return mdbquery._selectFields.isEmpty ? "*" : mdbquery._selectFields.joined( separator: "," )
        var strFields : [String] = []
        for field in mdbquery._selectFields {
            switch field {
                case let f as String:
                    strFields.append( f )
                case let f as SelectType:
                    switch f {
                        case .alias( let field, let as_what ):
                            strFields.append("\"" + field + "\"" + " AS " + "\"" + as_what + "\"")
                    }
                default:
                    break
            }
        }
        return strFields.isEmpty ? "*" : strFields.joined( separator: "," )
    }
    
    public func distinctOnRaw ( ) -> String {
        return mdbquery.distinct_on.count == 0 ?
                ""
            : "distinct on (" + MDBValue( fromTable:mdbquery.distinct_on.joined( separator: "," ) ).value + ") "
    }
    
    func whereRaw ( ) -> String {
        if ( mdbquery._where == nil ) { return "" }
        return mdbquery._where!._whereCond != nil ? "WHERE " + mdbquery._where!._whereCond!.raw( ) : ""
    }

    func groupRaw ( ) -> String {
        return mdbquery._group_by.count > 0 ? "GROUP BY " + mdbquery._group_by.joined(separator: ",") : ""
    }
    
    func orderRaw ( ) -> String {
        return mdbquery.order.isEmpty ? "" : "ORDER BY " + mdbquery.order.map{ $0.raw( ) }.joined( separator: "," )
    }

    func limitRaw ( ) -> String { return mdbquery._limit > 0 ? "LIMIT " + String( mdbquery._limit ) : "" }

    func offsetRaw ( ) -> String { return mdbquery._offset > 0 ? "OFFSET " + String( mdbquery._offset ) : "" }

    func joinRaw ( ) -> String {
        return mdbquery.joins.map{ $0.raw( ) }.joined( separator: " " )
    }

    public func valuesRaw ( ) -> String {
        var key_eq_values: [String] = []

        for (key,value) in sortedValues() {
            key_eq_values.append( "\"" + key + "\"=" + value.value )
        }
        
        return key_eq_values.joined(separator: ",")
    }

    public func multiUpdateValuesRaw ( ) -> String {
        var key_eq_values: [String] = []

        if !mdbquery.multiValues.isEmpty {
            for (key,_) in sortedValues( mdbquery.multiValues[ 0 ] ) {
                key_eq_values.append( "\"\(key)\"= s.\"\(key)\"")
            }
        }
        
        return key_eq_values.joined(separator: ",")
    }

        // toma un array de strings opcionales, filtra las que son nil o vacías, hace un casting forzado a lista de strings "normales" y une las restantes en una sola cadena
    func composeQuery ( _ parts: [String?] ) -> String {
        return (parts.filter{ $0 != nil && $0 != "" } as! [String]).joined(separator: " " )
    }
    
  
    // Unfortunatelly dictionaries do not respect the declaration order, so we need to sorted them for testing
    func sortedValues ( _ v: MDBValues? = nil ) -> [(key:String,value:MDBValue)] {
        #if testing
        return ( v == nil ? mdbquery.values : v! ).sorted { (v1,v2) in v1.key < v2.key }
        #else
        if (mdbquery.unitTest){ // Unfortunatelly dictionaries do not respect the declaration order, so we need to sorted them for testing
            return ( v == nil ? mdbquery.values : v! ).sorted { (v1,v2) in v1.key < v2.key }
        }
        return (v ?? mdbquery.values).map { (key, value) in (key, value) }
        #endif
    }
    
    func valuesFieldsRaw( _ sorted_values: [(key:String,value:MDBValue)] ) -> String {
        return  "(" + (sorted_values.map{ "\"\($0.key)\"" }).joined(separator: ",") + ")"
    }

    func valuesValuesRaw( _ sorted_values: [(key:String,value:MDBValue)] ) -> String {
        return "(" + (sorted_values.map{ $0.value.value }).joined(separator: ",") + ")"
    }


//
//    func multiValuesKeyValue ( _ sorted_values: [(key:String,value:MDBValue)] ) -> [[(key:String,value:Any)]] {
//        // THIS IS UGLY: During the migration it happens that some entities do have relations and other do not.
//        // When using a multi-insert, the ones that has relations makes "spaces to fill-in" for the other entities
//        func def_value ( col: String ) -> String {
//            return col.starts(with: "_relation") ? "''" : "null"
//        }
//        
//        return  multiValues.map{ row in
//            sorted_values.map{ col in (key: col.key, value:(row[ col.key ]?.value ?? def_value(col: col.key) )) }
//           }
//    }
//
    
    func multiValuesRaw ( _ sorted_values: [(key:String,value:MDBValue)] ) -> String {
        // THIS IS UGLY: During the migration it happens that some entities do have relations and other do not.
        // When using a multi-insert, the ones that has relations makes "spaces to fill-in" for the other entities
        func def_value ( col: String ) -> String {
            return col.starts(with: "_relation") ? "''" : "null"
        }
        
        return  mdbquery.multiValues.map{ row in
             "(" + sorted_values.map{ col in (row[ col.key ]?.value ?? def_value(col: col.key) ) }.joined(separator: "," ) + ")"
           }.joined(separator: ",")
    }
    
    func multiExcludedRaw ( _ sorted_values: [(key:String,value:MDBValue)] ) -> String {
        let conflict_keys = Set( mdbquery.on_conflict.components(separatedBy: ",").map{ $0.trimmingCharacters(in: .whitespacesAndNewlines) } )
        
        return sorted_values.filter{ col in !conflict_keys.contains( col.key ) }
                            .map{ col in ("\"\(col.key)\" = excluded.\"\(col.key)\"" ) }.joined(separator: ",")
    }
    
    func multi_insert_cursor ( ) {
        
    }

    public func rawQuery () -> String {
        switch mdbquery.queryType {
            case .UNKOWN:
                 return ""
            case .SELECT, .SELECT_FOR_UPDATE:
                 let for_update = mdbquery.queryType == .SELECT_FOR_UPDATE ? " FOR UPDATE" : ""
                 return composeQuery( [ "SELECT " + distinctOnRaw( ) + selectFieldsRaw( ) + " FROM " + MDBValue( fromTable: mdbquery.table ).value
                                      , tableAliasRaw( )
                                      , joinRaw( )
                                      , whereRaw( )
                                      , groupRaw( )
                                      , orderRaw( )
                                      , limitRaw( )
                                      , offsetRaw( )
                                      , for_update
                                      ] )
                 
            case .UPSERT:
                let sorted_values = sortedValues()
                let t = MDBValue( fromTable: mdbquery.table ).value

                return sorted_values.isEmpty ? ""
                     : mdbquery.delegate?.upsert( table: t, values: sorted_values, conflict: mdbquery.on_conflict, returning: mdbquery._returning ) ??
                       composeQuery( [ "INSERT INTO " + t
                                     , valuesFieldsRaw( sorted_values )
                                     , "VALUES"
                                     , valuesValuesRaw( sorted_values )
                                     , "ON CONFLICT (" + mdbquery.on_conflict + ") DO UPDATE SET"
                                     , valuesRaw()
                                     , returningRaw()
                                     ] )
            case .MULTI_UPSERT:
                 let sorted_values = sortedValues( mdbquery.multiValues.count > 0 ? mdbquery.multiValues[ 0 ] : [:] )
                 return sorted_values.isEmpty ? ""
//                      : delegate?.multi_upsert( table: table, keys: sorted_values, values: multiValuesKeyValue( sorted_values ), conflict: on_conflict, returning: _returning ) ??
                      : composeQuery( [ "INSERT INTO " + MDBValue( fromTable: mdbquery.table ).value
                                      , valuesFieldsRaw( sorted_values )
                                      , "VALUES"
                                      , multiValuesRaw( sorted_values )
                                      , "ON CONFLICT (" + mdbquery.on_conflict + ") DO UPDATE SET"
                                      , multiExcludedRaw( sorted_values )
                                      , returningRaw()
                                      ] )

            case .INSERT:
                 let sorted_values = sortedValues()
                 
                 return sorted_values.isEmpty ? ""
                      : composeQuery( [ "INSERT INTO " + MDBValue( fromTable: mdbquery.table ).value
                                      , valuesFieldsRaw( sorted_values )
                                      , "VALUES"
                                      , valuesValuesRaw( sorted_values )
                                      , whereRaw( )
                                      , returningRaw()
                                      ] )
            case .MULTI_INSERT:
                 let sorted_values = sortedValues( mdbquery.multiValues.count > 0 ? mdbquery.multiValues[ 0 ] : [:] )
                 return sorted_values.isEmpty ? ""
                      : composeQuery( [ "INSERT INTO " + MDBValue( fromTable: mdbquery.table ).value
                                      , valuesFieldsRaw( sorted_values )
                                      , "VALUES"
                                      , multiValuesRaw( sorted_values )
                                      , whereRaw( )
                                      , returningRaw()
                                      ] )
            case .UPDATE:
                 return mdbquery.values.isEmpty ? ""
                      : composeQuery( [ "UPDATE " + MDBValue( fromTable: mdbquery.table ).value + " SET"
                                      , valuesRaw( )
                                      , whereRaw( )
                                      , returningRaw()
                                      ] )
            case .MULTI_UPDATE:
                let sorted_values = sortedValues( mdbquery.multiValues.count > 0 ? mdbquery.multiValues[ 0 ] : [:] )

                return sorted_values.isEmpty ? ""
                     : composeQuery( [ "UPDATE " + MDBValue( fromTable: mdbquery.table ).value + " SET"
                                     , multiUpdateValuesRaw( )
                                     , "FROM (SELECT * FROM (VALUES "
                                     , multiValuesRaw( sorted_values )
                                     , ") AS t(\( sorted_values.map{ "\"\($0.key)\"" }.joined(separator: ", ") ))) AS s"
                                     , whereRaw( )
                                     , returningRaw()
                                     ] )

            case .DELETE:
                 return composeQuery( [ "DELETE FROM " + MDBValue( fromTable: mdbquery.table ).value
                                      , whereRaw( )
                                      , returningRaw()
                                      ] )
        }
    }

}
