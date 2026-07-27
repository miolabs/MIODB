# MIODB

A Swift database abstraction layer: one query builder, one result-set API, and one connection model shared across PostgreSQL, MySQL, Oracle and SQLite backends.

Write queries once with a fluent, strongly-typed builder; run them against any backend; read the results through lazy, typed result sets.

```swift
import MIODB
import MIODBSQLite   // or MIODBPostgreSQL, MIODBMySQL, MIODBOracleSQL

let conn = MDBSQLiteConnection( database: "/data/shop.sqlite" )
let db = try conn.create()

let rows = try db.execute( MDBQuery( "product" )
    .select( "id", "name", "price" )
    .andWhere( "enabled", true )
    .orderBy( "name" )
    .limit( 20 ) )

for row in rows {
    print( row.string( "name" ) ?? "-", row.decimal( "price" ) ?? 0 )
}
```

## Backends

MIODB itself contains no driver code — each backend is a separate package that subclasses the base classes:

| Package | Database | Kind |
|---|---|---|
| [MIODBPostgreSQL](https://github.com/miolabs/MIODBPostgreSQL) | PostgreSQL (libpq) | network |
| [MIODBMySQL](https://github.com/miolabs/MIODBMySQL) | MySQL (libmysqlclient) | network |
| [MIODBOracleSQL](https://github.com/miolabs/MIODBOracleSQL) | Oracle (OCI) | network |
| [MIODBSQLite](https://github.com/miolabs/MIODBSQLite) | SQLite (embedded) | embedded |

## Installation

```swift
dependencies: [
    .package( url: "https://github.com/miolabs/MIODB.git", from: "1.0.0" ),
    // plus the backend(s) you need:
    .package( url: "https://github.com/miolabs/MIODBSQLite.git", from: "1.0.0" ),
]
```

Requires Swift 5.9+, macOS 12+ or Linux.

## Architecture

Two class families, mirrored at both levels because a live database session *is a* connection:

- **`MDBConnection`** — a connection *factory*. Holds the configuration and creates live connections with `create(_ to_db:)`. Backends subclass it (`MDBSQLiteConnection`, `MDBPostgreConnection`, ...).
- **`MIODB`** — a *live connection*. Executes SQL (`execute(_:)` / `executeQuery(_:)`) and returns `MDBResultSet`s.

Capabilities that only network databases have are split into protocols, so embedded backends don't carry meaningless concepts:

- **`MDBCredentials`** — `host` / `port` / `user` / `password`. Adopted by `MDBNetworkConnection` and `MIONetworkDB`, the intermediate base classes the PostgreSQL, MySQL and Oracle backends subclass.
- **`MDBSchemes`** — `scheme` / `changeScheme(_:)` for in-database namespaces (PostgreSQL `search_path`).

SQLite subclasses the plain bases and conforms to neither: its whole configuration is `database` — the file path.

```swift
// Network backend: credentials + scheme
let pg = MDBPostgreConnection( host: "localhost", port: 5432,
                               user: "app", password: "secret",
                               database: "shop", scheme: "tenant_1" )
let db = try pg.create()

// Embedded backend: just a file path
let lite = MDBSQLiteConnection( database: "/data/shop.sqlite" )
let db2 = try lite.create()

// Generic code can test for capabilities:
if let creds = db as? MDBCredentials { print( creds.host ?? "-" ) }
if let schemes = db as? MDBSchemes   { try schemes.changeScheme( "tenant_2" ) }
```

### Connection registry

`MDBManager.shared` keeps named connection factories and hands out live connections:

```swift
MDBManager.shared.addConnection( pg, forIdentifier: "main" )

let db = try MDBManager.shared.connection( "main" )          // default database
let db2 = try MDBManager.shared.connection( "main", "other" ) // create(to_db:)
defer { MDBManager.shared.release( db ) }
```

## The query builder

`MDBQuery` builds SQL from chained calls. Nothing touches the database until the query is passed to `db.execute(_:)`; `rawQuery()` returns the generated SQL string.

Identifiers are always double-quoted, values are rendered as SQL literals with proper escaping (see [Values and types](#values-and-types)).

### SELECT

```swift
MDBQuery( "product" ).select()                          // SELECT * FROM "product"
MDBQuery( "product" ).select( "name", "price" )         // SELECT "name","price" FROM "product"
MDBQuery( "product" ).select( "product.*", "cat.name AS category" )
```

### WHERE

`andWhere` / `orWhere` take a field, an optional operator (defaults to `.EQ`) and a value:

```swift
try MDBQuery( "product" ).select()
    .andWhere( "price", .GT, 100 )                      // =, !=, <, <=, >, >=
    .andWhere( "name", .ILIKE, "beer%" )                // LIKE, ILIKE
    .andWhereIN( "status", [ "active", "draft" ] )      // IN / NOT IN
    .andWhereNULL( "deleted" )                          // IS NULL / IS NOT NULL
    .andWhereRaw( "char_length(\"name\") > 3" )         // raw SQL escape hatch
```

Parenthesized groups nest with `beginGroup()` / `endGroup()`:

```swift
// WHERE "enabled" = TRUE AND ("stock" > 0 OR "backorder" = TRUE)
try MDBQuery( "product" ).select()
    .andWhere( "enabled", true )
    .beginGroup()
        .andWhere( "stock", .GT, 0 )
        .orWhere( "backorder", true )
    .endGroup()
```

### JOIN

`join` qualifies unqualified columns automatically: `from` refers to the joined table, `to` to the query's base table. `from` defaults to `"id"`.

```swift
// SELECT * FROM "product" INNER JOIN "productCategory"
//   ON "productCategory"."id" = "product"."category"
try MDBQuery( "product" ).select()
    .join( table: "productCategory", to: "category" )

// LEFT JOIN with an alias and an extra ON condition
try MDBQuery( "order" ).select( "order.*", "u.name AS buyer" )
    .join( table: "user", to: "buyer_id", joinType: .LEFT, as: "u" ) { join in
        try join.addWhereLine( .AND, "u.active", .EQ, true )
    }
```

Join types: `.INNER`, `.LEFT`, `.RIGHT`, `.FULL`. There is also a JSON-relation join (`join(table:json:to:)`) that matches a `jsonb` array of ids (PostgreSQL only).

### ORDER BY, GROUP BY, LIMIT, OFFSET, DISTINCT ON

```swift
try MDBQuery( "product" ).select()
    .orderBy( "category" )                 // ASC by default
    .orderBy( "price", .DESC )
    .groupBy( "category" )
    .limit( 50 )
    .offset( 100 )
    .distinctOn( [ "category" ] )          // PostgreSQL only
```

### INSERT

Values are passed as a dictionary. Keys are sorted alphabetically so the generated SQL is deterministic:

```swift
// INSERT INTO "product" ("id","name","price") VALUES ('...','Beer',3.5)
try db.execute( MDBQuery( "product" ).insert( [
    "id"   : UUID(),
    "name" : "Beer",
    "price": Decimal( string: "3.50" )!
] ) )
```

Multi-row insert — one statement, all rows must share the same keys:

```swift
let rows: [[String:Any?]] = (0..<500).map { [ "id": UUID(), "name": "p\($0)" ] }
try db.execute( MDBQuery( "product" ).insert( rows ) )
```

### RETURNING

Any writing query can return columns; the result set carries the rows:

```swift
let rs = try db.execute( MDBQuery( "product" )
    .insert( [ "id": UUID(), "name": "Beer" ] )
    .returning( "id", "created" ) )
let created = rs[ 0 ].date( "created" )
```

### UPDATE

```swift
// UPDATE "product" SET "price"=4.0 WHERE "id" = '...'
try db.execute( MDBQuery( "product" )
    .update( [ "price": 4.0 ] )
    .andWhere( "id", id ) )
```

Multi-row update — different values per row via a `VALUES` table, joined on the given key fields:

```swift
try db.execute( MDBQuery( "product" ).update( [
    [ "id": id1, "price": 4.0 ],
    [ "id": id2, "price": 5.5 ],
], [ "id" ] ) )
```

### UPSERT

`INSERT ... ON CONFLICT (...) DO UPDATE SET`, single or multi-row:

```swift
try db.execute( MDBQuery( "product" )
    .upsert( [ "id": id, "name": "Beer", "stock": 10 ], "id" ) )
```

### DELETE

```swift
try db.execute( MDBQuery( "product" ).delete().andWhere( "id", id ) )
```

### Transactions

```swift
try db.executeQuery( MDBQuery.beginTransactionStament() )
do {
    try db.execute( ... )
    try db.execute( ... )
    try db.executeQuery( MDBQuery.commitTransactionStament() )
}
catch {
    try db.executeQuery( "ROLLBACK" )
    throw error
}
```

### Batching large multi-row queries

`MDBQueryCursor` slices a multi-row query into chunks (default 1000 rows) so a huge insert doesn't become one giant statement:

```swift
let query = try MDBQuery( "product" ).insert( thousandsOfRows )
try MDBQueryCursor( query ).exec { chunk in try db.execute( chunk ) }
```

## Result sets

`execute` / `executeQuery` return an `MDBResultSet` — a **lazy** window over the backend's raw response. Rows are `MDBRow` views; a cell is converted to its Swift type only when accessed. The result set is a `RandomAccessCollection`, so it iterates, maps and slices like an array.

```swift
let rs = try db.execute( MDBQuery( "product" ).select() )

rs.rowCount              // number of rows (0 for INSERT/UPDATE/DELETE without RETURNING)
rs.affectedRowCount      // rows touched by INSERT/UPDATE/DELETE (0 for SELECT)
rs.columns               // column names, in query order

for row in rs { ... }
let names = rs.map { $0.string( "name" ) ?? "" }
```

### Reading cells

Optional accessors return `nil` on a missing column, SQL NULL, or a failed conversion:

```swift
row.int( "quantity" )       // Int?
row.string( "name" )        // String?  (raw text, no conversion)
row.bool( "enabled" )       // Bool?
row.date( "created" )       // Date?
row.uuid( "id" )            // UUID?
row.decimal( "price" )      // Decimal?
row.isNull( "info" )        // Bool
row.rawString( "price" )    // String?  raw wire text, skips conversion
```

Throwing accessors distinguish *why* the value is unavailable — they throw `MDBError.columnNotFound`, `.nullValue` or `.typeMismatch`:

```swift
let id: UUID     = try row.uuidValue( "id" )
let name: String = try row.stringValue( "name" )
let price        = try row.decimalValue( "price" )
let custom: MyT  = try row.typedValue( "col" )     // generic cast
```

Dictionary-style access mirrors the legacy dict API — `NSNull` for SQL NULL, `nil` for a column that doesn't exist:

```swift
row[ "name" ]          // Any?  (native Swift type, NSNull for NULL)
row[ 0 ]               // by column position
row.dictionary         // [String:Any] — materialize one row
try rs.dictionaries()  // [[String:Any]] — materialize everything (legacy shape)
```

Because result sets return `NSNull` for SQL NULL, a fetched row can be fed straight back into an `insert`/`update` dictionary — both `nil` and `NSNull` render as `NULL`.

## Values and types

Dictionary values and `where` arguments accept these Swift types out of the box:

| Swift type | SQL literal |
|---|---|
| `nil`, `NSNull` | `NULL` |
| `Bool` | `TRUE` / `FALSE` |
| `Int`, `Int8/16/32/64` | `42` |
| `Float`, `Double` | `3.14` |
| `Decimal` | `3.50` (exact, no binary-float dirt) |
| `String` | `'escaped ''text'''` |
| `UUID` | `'UPPERCASED-UUID'` |
| `Date` | `'2026-07-27 10:00:00.000000'` (UTC, µs precision) |
| `[String:Any]` | JSON text literal |
| `[Any]` | `(a,b,c)` — for IN clauses |
| `MDBValue` | passed through as-is |

Escape hatches when you need SQL that isn't a literal:

```swift
MDBValue( raw: "now()" )                    // any SQL fragment
MDBValue( fromTable: "product.name" )       // a quoted identifier: "product"."name"
try MDBValue( "beer", isPartialString: true ) // '%beer%' for LIKE
```

Custom types can register a global conversion once:

```swift
MDBValue.register_type_conversion( MyType.self ) { v in
    "'\( MDBValue.escape_string( (v as! MyType).sqlText ) )'"
}
```

Timestamps use the ANSI SQL text format (`YYYY-MM-DD HH:MM:SS.ffffff`, UTC) that PostgreSQL, MySQL, SQL Server and SQLite all read natively. `MDBSQLTimestampString(_:)` / `MDBSQLParseTimestamp(_:)` are exposed for backends and callers — pure integer math, no formatters, exact inverses.

## Errors

Everything throws `MDBError` (or a backend-specific error for connection/execution failures):

- `.general(message)`
- `.invalidPoolID(id)` — unknown `MDBManager` identifier
- `.columnNotFound(column)`, `.nullValue(column)`, `.typeMismatch(column, expected)` — throwing row accessors
- `.conversionFailed(expected, rawValue)` — a cell that can't convert to its column's declared type

## Implementing a backend

A backend is ~3 classes:

1. Subclass `MIODB` (embedded) or `MIONetworkDB` (network) and override `connect(_:)`, `disconnect()` and `executeQuery(_:) -> MDBResultSet`.
2. Subclass `MDBResultSet`, keep the raw response alive and override the cell primitives: `isNull`, `rawValue`, `value`, and optionally `intValue` / `boolValue` fast paths.
3. Subclass `MDBConnection` / `MDBNetworkConnection` and override `create(_:identifier:label:delegate:)`.

`MIODBSQLite` is the smallest complete example of all three.

## License

Copyright © 2019-2026 Javier Segura Perez. All rights reserved.
