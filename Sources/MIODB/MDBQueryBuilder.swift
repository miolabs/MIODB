

import Foundation


public func alias ( _ field: String, _ as_what: String ) -> SelectType {
        return SelectType.alias( field, as_what )
    }
public extension MDBQuery {

    func alias ( _ alias_name: String ) throws -> MDBQuery { 
        throw MDBError.general( "alias is deprecated for clarity. Use tableAlias" )
    }
    
    func tableAlias ( _ alias_name: String ) -> MDBQuery { 
        _tableAlias = MDBValue( fromTable: alias_name).value
        return self 
    }

    func returning ( _ args: String... ) -> MDBQuery {
        for field in args {
            _returning.append( MDBValue( fromTable: field ).value )
        }
        return self
    }

    

    @discardableResult
    func select ( _ args: Any... ) -> MDBQuery {
        for field in args {
            if field is MDBValue || field is String {
                let select_field: MDBValue = field is MDBValue ? field as! MDBValue : MDBValue( fromTable: field as! String )
                _selectFields.append( select_field.value )
            }
            else if field is SelectType {
                _selectFields.append( field )
                // switch field as! SelectType {
                //     case .alias(let field, let as_what):
                // }
            }
        }
                
        queryType = .SELECT

        return self
    }

    @discardableResult
    func select_for_update ( _ args: Any... ) -> MDBQuery {
        select( args )
        queryType = .SELECT_FOR_UPDATE

        return self
    }

    func update ( _ val: [String:Any?] ) throws -> MDBQuery {
        queryType = .UPDATE
        self.values = try toValues( val )
        return self
    }
    
    func insert ( _ val: [String:Any?] ) throws -> MDBQuery {
        queryType = .INSERT
        self.values = try toValues( val )
        return self
    }
    
    func upsert ( _ val: [String:Any?], _ conflict: String ) throws -> MDBQuery {
        queryType = .UPSERT
        self.values = try toValues( val )
        self.on_conflict = conflict
        return self
    }

    func upsert ( _ val: [[String:Any?]], _ conflict: String ) throws -> MDBQuery {
        queryType = .MULTI_UPSERT
        self.multiValues = try val.map{ try toValues( $0 ) }
        self.on_conflict = conflict
        return self
    }

    
    func insert ( _ val: [[String:Any?]] ) throws -> MDBQuery {
        queryType = .MULTI_INSERT
        self.multiValues = try val.map{ try toValues( $0 ) }
        try check_all_rows_has_same_keys( )
        return self
    }
    
    func check_all_rows_has_same_keys ( ) throws
    {
        if multiValues.isEmpty {
            return
        }
        
        let keys = Set( multiValues[ 0 ].keys )
        
        for i in 1..<multiValues.count {
            let row_keys = Set( multiValues[ i ].keys )
            let cnt = keys.intersection( keys ).count
            
            if cnt != keys.count || row_keys.count != cnt {  // The error message is used in tests, so it is not a good idea to change it
                throw MDBError.general( "The inserted dictionary does not have the same keys: \(row_keys) vs first row keys: \(keys)" )
            }
        }
    }

    
    func update ( _ val: [[String:Any?]], _ fields: [String] ) throws -> MDBQuery {
        queryType = .MULTI_UPDATE
        self.multiValues = try val.map{ try toValues( $0 ) }
        try check_all_rows_has_same_keys( )

        for f in fields {
            let without_casting = f.components(separatedBy: "::")
            let casting = without_casting.count > 1 ? "::\(without_casting[ 1 ])" : ""
            try andWhere( "\(table).\(without_casting[0])", MDBValue( raw: "s.\"\(without_casting[0])\"\(casting)" ) )
        }
        
        return self
    }

    
    func delete ( ) -> MDBQuery {
        queryType = .DELETE
        return self
    }
    

    @discardableResult
    func distinctOn ( _ cols: [String] ) -> MDBQuery {
        distinct_on = cols
        return self
    }

    //
    // ORDER BY
    //
    
    @discardableResult
    func groupBy ( _ field: String, _ dir: ORDER_BY_DIRECTION = .ASC ) -> MDBQuery {
        _group_by.append( field )
        return self
    }
    
    @discardableResult
    func orderBy ( _ field: String, _ dir: ORDER_BY_DIRECTION = .ASC ) -> MDBQuery {
        order.append( OrderBy( field: MDBValue( fromTable: field ).value, dir: dir ) )
        return self
    }
  
    func limit ( _ value: Int32 ) -> MDBQuery
    {
        _limit = value ;
        return self
    }

    func offset ( _ value: Int32 ) -> MDBQuery { _offset = value ; return self }



    @discardableResult
    func join ( table: String, from: String? = nil, to: String, joinType: JOIN_TYPE = .INNER, as as_what: String? = nil, _ cb: @escaping ( Join 		) throws -> Void = { _ in } ) throws -> MDBQuery {
        // let from_table = MDBValue( fromTable: from != nil ? from! : table + ".id" ).value
        // let to_table   = MDBValue( fromTable: to ).value
        let from_table = from != nil ?  MDBValue( fromTable: from! ).value :  // we use what we get if something comes in the 'from'
                                        MDBValue( fromTable: table + ".id" ).value
        let to_table   = to.contains(".") ? MDBValue( fromTable: to ).value : // we use what we get if it is a table.field format
                                            MDBValue( fromTable: self.table ).value + "." + MDBValue( fromTable: to ).value

        let new_join   = Join( joinType: joinType, table: table, fromTable: from_table, toTable: to_table, asWhat: as_what )
        let join_already_done = joins.filter{ j in j.raw( ) == new_join.raw( ) }.count > 0
        
        if !join_already_done {
          joins.append( new_join )
        }
        
        try cb( new_join )
        
        return self
    }

    
    @discardableResult
    func join ( table: String, json: String, to: String, joinType: JOIN_TYPE = .INNER, as as_what: String? = nil, _ cb: @escaping ( JoinJSON ) throws -> Void = { _ in }  ) throws -> MDBQuery {
        let to_table   = MDBValue( fromTable: to ).value
        let new_join   = JoinJSON( joinType: joinType, table: table, json: json, toTable: to_table, asWhat: as_what )
        let join_already_done = joins.filter{ j in j.raw( ) == new_join.raw( ) }.count > 0
        
        if !join_already_done {
          joins.append( new_join )
        }
        
        try cb( new_join )
        
        return self
    }
    

      func test() -> MDBQuery {
        unitTest = true
        return self
    }

    ///
    ///  'operand-grouped' expressions
    ///

    @discardableResult
    func `where` ( ) throws ->  MDBQuery {
        if ( _where == nil ) {
            _where = MDBQueryWhere()
            usingQueryBuilderV2 = true
        }
        else {
          throw MDBError.whereAlreadyDefined
        }
        return self ;
    }

     @discardableResult
    func addCondition(_ field: Any, _ op: WHERE_LINE_OPERATOR, _ value: Any? ) throws -> MDBQuery {
        if ( _where != nil ) {
            try _where!.add_where_line( .UNDEF, field, op, value )
        }
        else {
          throw MDBError.whereNotFound
        }
        return self
    }

    @discardableResult
    func beginOrGroup ( ) throws -> MDBQuery {
        if ( _where != nil ) {
            try _where!.begin_group(.OR)
        }
        else {
            throw MDBError.whereNotFound
        }
        return self ;
    }

    @discardableResult
    func beginAndGroup ( ) throws -> MDBQuery {
        if ( _where != nil ) {
            try _where!.begin_group(.AND)
        }
         else {
            throw MDBError.whereNotFound
        }
        return self ;
    }

    @discardableResult
    func endGroup ( ) throws -> MDBQuery {
        if ( _where != nil ) {
            _where!.end_group()
        }
        else {
            throw MDBError.whereNotFound
        }
        return self ;
    }

    //////////////////////////   DEPRECATED. DONT USE
    ///
    /// You should use now the 'operand-grouped' expressions
    ///
    @discardableResult
    func beginGroup ( ) throws -> MDBQuery {
        if (usingQueryBuilderV2) {
            throw MDBError.usingDeprecatedFunctionWithNewFunctions
        }
        if ( _where == nil ) {
            _where = MDBQueryWhere( )
        }
        try! _where!.begin_group()
        return self
    }

    @discardableResult
    func addWhereLine( _ where_op: WHERE_OPERATOR, _ field: Any, _ op: WHERE_LINE_OPERATOR, _ value: Any? ) throws -> MDBQuery {
        if (usingQueryBuilderV2) {
            throw MDBError.usingDeprecatedFunctionWithNewFunctions
        }
        if ( _where == nil ) {
            _where = MDBQueryWhere( )
        }
        try _where!.add_where_line( where_op, field, op, value )

        return self
    }

    @discardableResult
    func andWhereRaw ( _ raw: String ) -> MDBQuery {
        return try! addWhereLine( .AND, "", WHERE_LINE_OPERATOR.RAW, MDBValue.init(raw: raw) )
    }

    @discardableResult
    func orWhereRaw ( _ raw: String ) -> MDBQuery {
        return try! addWhereLine( .OR, "", WHERE_LINE_OPERATOR.RAW, MDBValue.init(raw: raw) )
    }

    @discardableResult
    func andWhereNULL ( _ field: String ) -> MDBQuery {
        return try! addWhereLine( .AND, field, WHERE_LINE_OPERATOR.IS, try! MDBValue( nil ) )
    }

    @discardableResult
    func orWhereNULL ( _ field: String ) throws -> MDBQuery {
        return try! addWhereLine( .OR, field, WHERE_LINE_OPERATOR.IS, try! MDBValue( nil ) )
    }

    @discardableResult
    func andWhereNotNULL ( _ field: String ) -> MDBQuery {
        return try! addWhereLine( .AND, field, WHERE_LINE_OPERATOR.IS_NOT, try! MDBValue( nil ) )
    }

    @discardableResult
    func orWhereNotNULL ( _ field: String ) -> MDBQuery {
        return try! addWhereLine( .OR, field, WHERE_LINE_OPERATOR.IS_NOT, try! MDBValue( nil ) )
    }

    @discardableResult
    func andWhereIN ( _ field: String, _ vals: [Any] ) throws -> MDBQuery {
        return try! addWhereLine( .AND, field, WHERE_LINE_OPERATOR.IN, try MDBValue.fromValue( vals ) )
    }

    @discardableResult
    func andWhereNotIN ( _ field: String, _ vals: [Any] ) throws -> MDBQuery {
        return try! addWhereLine( .AND, field, WHERE_LINE_OPERATOR.NOT_IN, try MDBValue.fromValue( vals ) )
    }

    @discardableResult
    func orWhereIN ( _ field: String, _ vals: [Any] ) throws -> MDBQuery {
        return try addWhereLine( .OR, field, WHERE_LINE_OPERATOR.IN, try MDBValue.fromValue( vals ) )
    }

    @discardableResult
    func orWhereNotIN ( _ field: String, _ vals: [Any] ) throws -> MDBQuery {
        return try addWhereLine( .OR, field, WHERE_LINE_OPERATOR.NOT_IN, try MDBValue.fromValue( vals ) )
    }

    @discardableResult
    func andWhere ( _ field: String, _ value: Any ) throws -> MDBQuery {
        return try addWhereLine( .AND, field, .EQ, value )
    }

    @discardableResult
    func andWhere ( _ field: String, _ op: WHERE_LINE_OPERATOR, _ value: Any ) throws -> MDBQuery {
        return try addWhereLine( .AND, field, op, value )
    }

    @discardableResult
    func orWhere ( _ field: String, _ value: Any ) throws -> MDBQuery {
        return try addWhereLine( .OR, field, .EQ, value )
    }
    
    @discardableResult
    func orWhere ( _ field: String, _ op: WHERE_LINE_OPERATOR, _ value: Any ) throws -> MDBQuery {
        return try addWhereLine( .OR, field, op, value )
    }
    
}
