//
//  MDBResultSet.swift
//  MIODB
//
//  Created by Javier Segura Perez on 07/07/2026.
//  Copyright © 2026 Javier Segura Perez. All rights reserved.
//

import Foundation
import MIOCoreLogger

/// Base class for lazy query results. A backend (PostgreSQL, MySQL, ...)
/// subclasses it, keeps the raw server response alive and overrides the
/// cell primitives (`isNull`, `rawValue`, `value`, ...) so that values are
/// converted to Swift types only when they are accessed.
///
/// Column order is preserved exactly as reported by the server, and rows are
/// addressed by integer index. Row access behaves like the legacy row
/// dictionaries: `result[0]["name"]` returns `NSNull` for SQL NULL and `nil`
/// for a column that does not exist in the result.
open class MDBResultSet
{
    /// Column names in the order reported by the server.
    public let columns: [String]

    /// Number of rows in the result. Zero for commands (INSERT, UPDATE, ...).
    public let rowCount: Int

    /// Rows affected by INSERT/UPDATE/DELETE. Zero for SELECT.
    public let affectedRowCount: Int

    /// Column name -> index. On duplicated column names the last one wins,
    /// matching the legacy dictionary behavior.
    public let columnIndex: [String:Int]

    public init ( columns: [String], rowCount: Int, affectedRowCount: Int )
    {
        self.columns = columns
        self.rowCount = rowCount
        self.affectedRowCount = affectedRowCount

        var index = [String:Int]( minimumCapacity: columns.count )
        for ( i, name ) in columns.enumerated() { index[ name ] = i }
        self.columnIndex = index
    }

    // MARK: - Cell primitives. Backends override these.

    open func isNull ( row: Int, col: Int ) -> Bool { return true }

    /// The raw text the server sent for a cell, without any type conversion.
    /// Returns nil for SQL NULL.
    open func rawValue ( row: Int, col: Int ) -> String? { return nil }

    /// Converts a cell to its Swift value. `NSNull` for SQL NULL. Returns nil
    /// only when a custom conversion resolves to nil, to mirror the legacy
    /// dictionary behavior where that key was skipped.
    open func value ( row: Int, col: Int ) throws -> Any? { return NSNull() }

    /// Integer fast path. Backends can parse straight from the raw buffer
    /// without allocating a String or boxing into Any.
    open func intValue ( row: Int, col: Int ) -> Int? {
        guard let raw = rawValue( row: row, col: col ) else { return nil }
        return Int( raw )
    }

    open func boolValue ( row: Int, col: Int ) -> Bool? {
        return ( try? value( row: row, col: col ) ) as? Bool
    }

    // MARK: - Legacy materialization

    /// Converts every cell and returns the rows as dictionaries, like the
    /// legacy `executeQueryString` API.
    public func dictionaries ( ) throws -> [[String:Any]] {
        var items = [[String:Any]]()
        items.reserveCapacity( rowCount )

        for row in 0..<rowCount {
            var item = [String:Any]( minimumCapacity: columns.count )
            for col in 0..<columns.count {
                if let v = try value( row: row, col: col ) { item[ columns[ col ] ] = v }
            }
            items.append( item )
        }

        return items
    }
}

extension MDBResultSet : RandomAccessCollection
{
    // Associated types pinned to the base class so subclasses don't re-infer
    // them (IndexingIterator<Self> breaks iteration on subclass instances).
    public typealias Element = MDBRow
    public typealias Index = Int
    public typealias Indices = Range<Int>
    public typealias SubSequence = Slice<MDBResultSet>

    public var startIndex: Int { 0 }
    public var endIndex: Int { rowCount }

    public subscript ( row: Int ) -> MDBRow {
        return MDBRow( resultSet: self, row: row )
    }

    public struct Iterator : IteratorProtocol {
        let resultSet: MDBResultSet
        var row: Int = 0

        public mutating func next ( ) -> MDBRow? {
            guard row < resultSet.rowCount else { return nil }
            defer { row += 1 }
            return MDBRow( resultSet: resultSet, row: row )
        }
    }

    public func makeIterator ( ) -> Iterator {
        return Iterator( resultSet: self )
    }
}

extension MDBResultSet {
    public static func empty ( ) -> MDBResultSet {
        return MDBResultSet(columns: [], rowCount: 0, affectedRowCount: 0)
    }
}

/// A lightweight view of one row. Nothing is copied or converted until a
/// column is accessed.
public struct MDBRow
{
    public let resultSet: MDBResultSet
    public let row: Int

    public init ( resultSet: MDBResultSet, row: Int ) {
        self.resultSet = resultSet
        self.row = row
    }

    /// Dictionary-like access: `NSNull` for SQL NULL, nil when the column
    /// does not exist or its value can't be converted. A cell is always its
    /// native Swift type, never a raw-string stand-in; use `value(_:)` to
    /// receive conversion errors as thrown errors.
    public subscript ( column: String ) -> Any? {
        guard let col = resultSet.columnIndex[ column ] else { return nil }
        return self[ col ]
    }

    /// Access by column position, following the order of the query.
    public subscript ( col: Int ) -> Any? {
        do { return try resultSet.value( row: row, col: col ) }
        catch {
            Log.error( "Column \"\(resultSet.columns[ col ])\": \(error.localizedDescription)" )
            return nil
        }
    }

    /// Throwing variant of the subscript, propagates conversion errors.
    public func value ( _ column: String ) throws -> Any? {
        guard let col = resultSet.columnIndex[ column ] else { return nil }
        return try resultSet.value( row: row, col: col )
    }

    public func isNull ( _ column: String ) -> Bool {
        guard let col = resultSet.columnIndex[ column ] else { return true }
        return resultSet.isNull( row: row, col: col )
    }

    /// The raw text the server sent, skipping type conversion entirely.
    public func rawString ( _ column: String ) -> String? {
        guard let col = resultSet.columnIndex[ column ] else { return nil }
        return resultSet.rawValue( row: row, col: col )
    }

    // MARK: Typed accessors. These skip the Any boxing where possible.

    public func int ( _ column: String ) -> Int? {
        guard let col = resultSet.columnIndex[ column ] else { return nil }
        return resultSet.intValue( row: row, col: col )
    }

    public func string ( _ column: String ) -> String? {
        return rawString( column )
    }

    public func bool ( _ column: String ) -> Bool? {
        guard let col = resultSet.columnIndex[ column ] else { return nil }
        return resultSet.boolValue( row: row, col: col )
    }

    public func date ( _ column: String ) -> Date? { return self[ column ] as? Date }
    public func uuid ( _ column: String ) -> UUID? { return self[ column ] as? UUID }
    public func decimal ( _ column: String ) -> Decimal? { return self[ column ] as? Decimal }

    // MARK: Throwing, non-optional accessors. Same conversions as above, but
    // a missing column, a SQL NULL or a failed conversion throw an MDBError
    // instead of returning nil.

    func index ( of column: String ) throws -> Int {
        guard let col = resultSet.columnIndex[ column ] else { throw MDBError.columnNotFound( column ) }
        return col
    }

    public func intValue ( _ column: String ) throws -> Int {
        let col = try index( of: column )
        if resultSet.isNull( row: row, col: col ) { throw MDBError.nullValue( column ) }
        guard let v = resultSet.intValue( row: row, col: col ) else { throw MDBError.typeMismatch( column, "Int" ) }
        return v
    }

    public func stringValue ( _ column: String ) throws -> String {
        let col = try index( of: column )
        guard let v = resultSet.rawValue( row: row, col: col ) else { throw MDBError.nullValue( column ) }
        return v
    }

    public func boolValue ( _ column: String ) throws -> Bool {
        let col = try index( of: column )
        if resultSet.isNull( row: row, col: col ) { throw MDBError.nullValue( column ) }
        guard let v = resultSet.boolValue( row: row, col: col ) else { throw MDBError.typeMismatch( column, "Bool" ) }
        return v
    }

    public func dateValue ( _ column: String ) throws -> Date { return try typedValue( column ) }
    public func uuidValue ( _ column: String ) throws -> UUID { return try typedValue( column ) }
    public func decimalValue ( _ column: String ) throws -> Decimal { return try typedValue( column ) }

    /// Converts the cell and casts it to the requested type, throwing on a
    /// missing column, SQL NULL or conversion mismatch.
    public func typedValue<T> ( _ column: String ) throws -> T {
        let col = try index( of: column )
        let v = try resultSet.value( row: row, col: col )
        if v == nil || v is NSNull { throw MDBError.nullValue( column ) }
        guard let t = v as? T else { throw MDBError.typeMismatch( column, String( describing: T.self ) ) }
        return t
    }

    // MARK: convert row as key value pairs ( dictionary )
    
    /// Materializes this row as a dictionary (legacy shape).
    public var dictionary: [String:Any] {
        var item = [String:Any]( minimumCapacity: resultSet.columns.count )
        for col in 0..<resultSet.columns.count {
            if let v = self[ col ] { item[ resultSet.columns[ col ] ] = v }
        }
        return item
    }
}

extension MDBRow
{
    public static func empty() -> MDBRow {
        return MDBRow(resultSet: .empty(), row: 0)
    }
}
