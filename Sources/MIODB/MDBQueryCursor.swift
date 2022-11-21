//
//  File.swift
//  
//
//  Created by David Trallero on 21/11/22.
//

import Foundation

public class MDBQueryCursorIterator<T> {
    var allValues: [T] = []
    var curOffset: Int = 0
    var curLimit: Int = 1000
    
    public init ( _ values: [T]  ) {
        self.allValues = values
    }
    
    public func hasData ( ) -> Bool {
        return curOffset < allValues.count
    }
    
    public func next ( _ cb: @escaping ( _ l: [T] ) throws -> Void ) throws {
        while hasData() {
            let end = min( curOffset + curLimit, allValues.count )
            
            try cb( Array( allValues[ curOffset..<end ] ) )
            
            curOffset = end
        }
    }
}



public class MDBQueryCursor : MDBQueryCursorIterator<MDBValues> {
    var query: MDBQuery
    
    public init ( _ query: MDBQuery  ) {
        self.query = query
        super.init(query.multiValues)        
    }
        
    public func exec ( cb: @escaping ( _ l: MDBQuery ) throws -> Void ) throws {
        let it = MDBQueryCursorIterator<MDBValues>( allValues )
        
        try it.next{ values in
          self.query.multiValues = values
        
          try cb( self.query )
        }
    }
}
