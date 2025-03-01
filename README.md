# MIODB

A Swift ORM database abstraction layer that provides a unified interface to interact with various database systems using a common syntax.

## Overview

MIODB allows developers to work with different database backends using a consistent, strongly-typed API. It abstracts away the specifics of each database implementation, providing:

- Unified query building with a fluent interface
- Transaction support
- Connection management
- Cross-database compatibility

## Features

- **Database Agnostic**: Write once, run on any supported database backend
- **Fluent Query Builder**: Construct complex SQL queries using a chainable Swift API
- **Type Safety**: Leverage Swift's type system for safer database operations
- **Composable Queries**: Build complex queries using joins, where clauses, and more
- **Support for Multiple Query Types**: SELECT, INSERT, UPDATE, DELETE, UPSERT operations

## Usage Examples

### Basic Connection

```swift
let db = MIODB(host: "localhost", port: 5432, user: "username", password: "password", database: "mydb")
try db.connect()
```

### Simple Queries

```swift
// Fetch a record by ID
let user = try db.fetch("users", "123")

// Select with conditions
let query = MDBQuery("users")
    .select("id", "name", "email")
    .andWhere("active", true)
    .limit(10)
    
let results = try db.execute(query)
```

### Complex Queries

```swift
// Joins, where clauses, and ordering
let query = MDBQuery("orders")
    .select("orders.id", "orders.amount", "users.name")
    .try! join(table: "users", from: "orders.user_id", to: "users.id")
    .andWhere("orders.status", .EQ, "completed")
    .andWhere("orders.amount", .GT, 100)
    .orderBy("orders.created_at", .DESC)
    .limit(20)
    
let results = try db.execute(query)
```

### Inserts and Updates

```swift
// Insert a new record
let newUser = ["name": "John Doe", "email": "john@example.com", "active": true]
let insertQuery = try MDBQuery("users").insert(newUser)
try db.execute(insertQuery)

// Update a record
let updates = ["last_login": Date(), "login_count": 42]
let updateQuery = try MDBQuery("users")
    .update(updates)
    .andWhere("id", "123")
try db.execute(updateQuery)
```

## Requirements

- Swift 5.0+
- MIOCoreLogger

## License

Copyright © 2019 Javier Segura Perez. All rights reserved.