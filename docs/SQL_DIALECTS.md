# Plan: SQL dialect abstraction

**Status: design — not implemented.**

## Problem

`MDBQuery.defaultRawQuery()` generates PostgreSQL-flavored SQL. SQLite gets by
with two string shims on the final SQL (strip trailing `FOR UPDATE`, rewrite
`" ILIKE "`), which is fragile (a string literal containing `" ILIKE "` would
be corrupted). MySQL and Oracle can't get by at all: `ON CONFLICT`,
`RETURNING`, `DISTINCT ON`, the `UPDATE ... FROM (VALUES ...)` multi-update,
`LIMIT/OFFSET` (Oracle) and `TRUE/FALSE` (Oracle) have no direct equivalent.
Porting them requires moving statement generation behind an abstraction
instead of patching strings.

## Direction already agreed

- **Defer SQL rendering to the end** (agreed 2026-07-12 with the
  render/COPY work): `MDBValue` keeps the typed value (`MDBValueStorage`);
  rendering to an SQL literal happens as late as possible. The storage enum
  and lazy `MDBValue.value` already exist — this plan extends the same idea
  to the places `MDBQuery` still renders eagerly.
- **No per-query delegate.** `MDBQueryDelegate` was deliberately removed
  (2026-07-12). The dialect is owned by the *connection* (`MIODB`), not
  attached to queries.

## Key simplifying decisions

1. **Identifiers stay ANSI double-quoted everywhere.** No per-dialect quoting.
   PostgreSQL, SQLite and Oracle accept `"name"` natively; MySQL does after
   `SET SESSION sql_mode = CONCAT(@@sql_mode, ',ANSI_QUOTES')` at connect.
   This keeps `MDBValue(fromTable:)` / `fromField:` pre-rendering unchanged
   and shrinks the refactor massively: select fields, order-by, returning,
   group-by and join identifiers stay pre-rendered strings.
2. **String escaping stays `'' `-doubling everywhere.** MySQL treats `\` as
   an escape character by default; instead of a second escaping rule, add
   `NO_BACKSLASH_ESCAPES` to the session `sql_mode`. One escaping rule for
   all backends.
3. **Timestamp literals stay `'YYYY-MM-DD HH:MM:SS.ffffff'` everywhere.**
   PostgreSQL, MySQL, SQLite read it natively; Oracle does after
   `ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6'
   NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'` at connect.
4. **What a backend can't express throws** (`MDBError.unsupported`), it is
   never silently dropped. Exception: `FOR UPDATE` on SQLite is dropped by
   design — the whole file locks on write anyway, so the semantics hold.
5. **Raw fragments bypass the dialect** (`andWhereRaw`, `MDBValue(raw:)`,
   `::jsonb` casts). Callers that use them own their portability, as today.
   DualLinkServerKit is PostgreSQL-only and unaffected.

## Architecture

New open class in MIODB (mirrors how backends already subclass everything):

```swift
open class MDBDialect {
    public static let ansi = MDBDialect()   // current output, byte-identical

    // Statement composition (moved from MDBQuery.defaultRawQuery)
    open func render ( _ q: MDBQuery ) throws -> String   // switch on queryType
    open func select ( _ q: MDBQuery ) throws -> String
    open func insert ( _ q: MDBQuery ) throws -> String   // + multiInsert
    open func update ( _ q: MDBQuery ) throws -> String   // + multiUpdate
    open func upsert ( _ q: MDBQuery ) throws -> String   // + multiUpsert
    open func delete ( _ q: MDBQuery ) throws -> String

    // Clause hooks the statement methods call
    open func limitOffset ( limit: Int32, offset: Int32 ) -> String
    open func forUpdate ( ) throws -> String
    open func returning ( _ cols: [String] ) throws -> String
    open func distinctOn ( _ cols: [String] ) throws -> String
    open func whereOperator ( _ op: WHERE_LINE_OPERATOR, field: String, value: MDBValue ) throws -> String
    open func renderValue ( _ storage: MDBValueStorage ) -> String  // default: MDBValue.render
}
```

- `MIODB` gains `open var dialect: MDBDialect` (base: `.ansi`). `execute(_:)`
  renders with it: `executeQuery( try query.rawQuery( dialect: dialect ) )`.
- `MIODB` also gains an overridable `sessionSetup()` hook called from
  `connect(_:)` — the one place per-backend session configuration lives.
  Today this is ad-hoc: Postgres sets `statement_timeout` inline, SQLite runs
  its pragmas inline. MySQL (`sql_mode`) and Oracle (NLS formats) need the
  same thing, and Postgres's auto-reconnect must re-run it, so formalize it.
- `MDBQuery.rawQuery()` (non-throwing, 12 direct call sites in
  DualLinkServerKit) stays and renders with `.ansi` — unchanged output.
  New `rawQuery( dialect: ) throws` overload does the real work.
- `defaultRawQuery()` body moves into `MDBDialect`; `MDBQuery` keeps its
  public `*Raw()` helpers delegating to `.ansi` for source compatibility.

### The one deferral the refactor needs

WHERE values are rendered at build time today
(`MDBQueryWhere.add_where_line` stores `MDBWhereLine.value: String`), which
would lock booleans/dates in WHERE clauses to the ANSI rendering before the
dialect ever sees them. Change:

- `MDBWhereLine.value: String` → `MDBValue` (keep `field: String`,
  `op: WHERE_LINE_OPERATOR` as-is).
- `MDBWhereString.raw(firstLine:)` → `raw(firstLine:, dialect:)`, threaded
  through `MDBWhere` / `MDBWhereGroup` / `Join`.

Nobody outside MIODB constructs `MDBWhereLine` (checked DualLinkServerKit:
0 hits), so this is internal. `values` / `multiValues` are already typed
(`MDBValues`), nothing to do there.

## Per-dialect divergence matrix

| Construct | ANSI/PG (default) | SQLite | MySQL | Oracle |
|---|---|---|---|---|
| UPSERT | `ON CONFLICT..DO UPDATE, excluded.` | same (3.24+) | `ON DUPLICATE KEY UPDATE` (conflict target implicit — any unique key) | `MERGE INTO .. USING dual` (formula existed in the old backend) |
| RETURNING | native | native (3.35+) | **throw** (later: LAST_INSERT_ID emulation) | **throw** (RETURNING INTO needs binds) |
| multi-UPDATE | `FROM (VALUES..) AS t(cols)` | `FROM (SELECT column1 AS c1.. FROM (VALUES..))` — current SQL is invalid on SQLite today | `VALUES ROW(..)` (8.0.19+) | `FROM (SELECT .. FROM dual UNION ALL ..)` |
| LIMIT/OFFSET | `LIMIT n OFFSET m` | same | same | `OFFSET m ROWS FETCH NEXT n ROWS ONLY` (12c+) |
| FOR UPDATE | native | drop (by design) | native | native |
| DISTINCT ON | native | **throw** | **throw** | **throw** |
| ILIKE | native | `LIKE` | `LIKE` | `UPPER(f) LIKE UPPER(v)` |
| `?|` / JSON join | native | **throw** | **throw** | **throw** |
| Bool literal | `TRUE/FALSE` | same | same | `1/0` |
| Session setup at connect | — | pragmas (done) | `sql_mode += ANSI_QUOTES, NO_BACKSLASH_ESCAPES` | `NLS_TIMESTAMP_FORMAT / NLS_DATE_FORMAT` |

## Phases

**Phase 1 — MIODB core (no behavior change).**
`MDBDialect` with the current `defaultRawQuery` body; `MIODB.dialect`;
`rawQuery(dialect:) throws`; WHERE-value deferral. Gate: all existing
query tests pass with byte-identical SQL under `.ansi`; new tests lock the
clause-hook outputs. This ships independently — nothing downstream changes.

**Phase 2 — SQLite adopts it.** `MDBSQLiteDialect` (FOR UPDATE, ILIKE,
multi-UPDATE form); delete `adaptToDialect` string shims. Fixes the latent
multi-UPDATE breakage and the `" ILIKE "`-inside-a-literal corruption. Gate:
18 existing tests + new multi-update/ILIKE-literal tests.

**Phase 3 — MySQL port** (first real payoff): rewrite `MIODBMySQL` on the
current API — `connect(to_db:) throws` with real error handling, port 3306
(today it says 5432), `MDBMySQLConnection` factory, `MDBMySQLResultSet`
(materialized, like SQLite — `mysql_store_result` is already a full copy),
session `sql_mode`, `MDBMySQLDialect`, delete the dead `equal(field:mysqlHexString:)`
extension. Compile-verified locally via `brew install mysql-client`;
integration-tested against a Docker MySQL.

**Phase 4 — Oracle port** (largest): current API + `MDBOracleDialect`
(MERGE, FETCH FIRST, bool 1/0), NLS session setup, `changeScheme` →
`ALTER SESSION SET CURRENT_SCHEMA` (today it's a silent no-op), fix
`makeCString`/`Field` leaks, `MIOCoreLogger` instead of `print`, per-type
column buffers. Needs Instant Client + Oracle Docker to verify.

**Phase 5 — docs.** Backends table in the README back to four 1.0 rows;
dialect-authoring section in "Implementing a backend".

## Non-goals

- No change to the raw-fragment escape hatches — they stay dialect-blind.
- No cross-dialect emulation of DISTINCT ON / JSON operators (throw instead).
- The PostgreSQL COPY fast path reads `MDBValueStorage` directly and is
  unaffected.

## Risks

- `MDBWhereLine` is public; changing `value` to `MDBValue` is technically
  API-breaking (no known external constructors). Do it in the same minor as
  the dialect introduction.
- MySQL `ON DUPLICATE KEY UPDATE` ignores the conflict-column list — any
  unique key triggers the update. Acceptable for our usage (conflict target
  is always the PK); documented in the dialect.
- `sql_mode` append must not clobber server defaults — read-modify-write via
  `CONCAT(@@sql_mode, ...)`.
