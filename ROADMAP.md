# Quarry Roadmap

SQLite features not yet implemented in Quarry, organized by priority.

## Implemented

- [x] Database open/close (file and memory)
- [x] Execute SQL statements
- [x] Query with results (Array and single row)
- [x] Parameter binding (positional and named)
- [x] Type-safe binding (`ToSql` typeclass)
- [x] Type-safe extraction (`FromSql` typeclass)
- [x] Transactions with automatic rollback
- [x] Savepoints (nested transactions)
- [x] Transaction variants (deferred, immediate, exclusive)
- [x] Last insert rowid / changes count
- [x] Busy timeout
- [x] WAL mode helper

## High Priority

Features that would benefit most users.

### User-Defined Functions
Register Lean functions callable from SQL.

```lean
-- Goal API
db.createFunction "double" fun (x : Int) => x * 2
db.query "SELECT double(value) FROM numbers"
```

**SQLite API**: `sqlite3_create_function_v2`
**Complexity**: Medium - need to handle callbacks from C to Lean

### Backup API
Online backup to another database file.

```lean
-- Goal API
db.backup "/path/to/backup.db"
-- Or incremental
let backup ← db.backupInit destDb
while !(← backup.isDone) do
  backup.step 100
backup.finish
```

**SQLite API**: `sqlite3_backup_init`, `sqlite3_backup_step`, `sqlite3_backup_finish`
**Complexity**: Medium

### Interrupt
Cancel long-running queries from another thread.

```lean
-- Goal API
db.interrupt  -- cancels current operation
```

**SQLite API**: `sqlite3_interrupt`
**Complexity**: Low - single FFI function

## Medium Priority

Useful for specific use cases.

### Incremental BLOB I/O
Stream large blobs without loading entirely into memory.

```lean
-- Goal API
let blob ← db.openBlob "table" "column" rowid .readWrite
blob.read offset size
blob.write offset data
blob.close
```

**SQLite API**: `sqlite3_blob_open`, `sqlite3_blob_read`, `sqlite3_blob_write`, `sqlite3_blob_close`
**Complexity**: Medium

### Update Hook
React to INSERT/UPDATE/DELETE operations.

```lean
-- Goal API
db.setUpdateHook fun op table rowid => do
  IO.println s!"{op} on {table} row {rowid}"
```

**SQLite API**: `sqlite3_update_hook`
**Complexity**: Medium - callback handling

### Commit/Rollback Hooks
React to transaction events.

```lean
-- Goal API
db.setCommitHook do IO.println "committing"
db.setRollbackHook do IO.println "rolling back"
```

**SQLite API**: `sqlite3_commit_hook`, `sqlite3_rollback_hook`
**Complexity**: Medium

### Column Metadata
Get source table/column information for query results.

```lean
-- Goal API
let meta ← stmt.columnMetadata 0
-- meta.database, meta.table, meta.originName
```

**SQLite API**: `sqlite3_column_database_name`, `sqlite3_column_table_name`, `sqlite3_column_origin_name`
**Complexity**: Low

### Progress Handler
Callback during long operations (for progress bars, cancellation).

```lean
-- Goal API
db.setProgressHandler 1000 do
  IO.println "still working..."
  return false  -- true to interrupt
```

**SQLite API**: `sqlite3_progress_handler`
**Complexity**: Medium

### Authorizer
Fine-grained access control for SQL operations.

```lean
-- Goal API
db.setAuthorizer fun action arg1 arg2 dbName trigger =>
  if action == .delete && arg1 == "users" then .deny
  else .ok
```

**SQLite API**: `sqlite3_set_authorizer`
**Complexity**: Medium-High

## Low Priority / Advanced

Niche features for specialized use cases.

### Virtual Tables
Custom table implementations backed by Lean code.

**SQLite API**: `sqlite3_create_module_v2`
**Complexity**: High - requires implementing multiple callbacks

### Custom Collations
Custom string sorting rules.

**SQLite API**: `sqlite3_create_collation_v2`
**Complexity**: Medium

### Custom Aggregates
Create custom aggregate functions like SUM/AVG.

**SQLite API**: `sqlite3_create_function_v2` with step/final callbacks
**Complexity**: Medium-High

### Full-Text Search (FTS5)
Full-text search capabilities.

**Note**: FTS5 is a compile-time extension. Would need to enable in SQLite build.
**Complexity**: Low (if enabled) - just SQL syntax

### R-Tree
Spatial/geographic indexing.

**Note**: R-Tree is a compile-time extension.
**Complexity**: Low (if enabled) - just SQL syntax

## PRAGMA Helpers

Convenience wrappers for common PRAGMA statements. These can all be done via `exec` today, but dedicated helpers would be cleaner.

- [ ] `db.foreignKeys true/false` - Enable/disable foreign key enforcement
- [ ] `db.synchronous .off/.normal/.full` - Sync mode
- [ ] `db.cacheSize n` - Page cache size
- [ ] `db.tempStore .default/.file/.memory` - Temp storage location
- [ ] `db.autoVacuum .none/.full/.incremental` - Auto vacuum mode
- [ ] `db.encoding` - Get database encoding
- [ ] `db.pageSize` - Get/set page size
- [ ] `db.maxPageCount` - Limit database size

## Contributing

To implement a feature:

1. Add the C FFI function to `native/src/quarry_ffi.c`
2. Add the Lean FFI binding to `Quarry/FFI/Database.lean` or `Quarry/FFI/Statement.lean`
3. Add the high-level API to the appropriate module
4. Add tests to `Tests/Main.lean`
5. Update this roadmap to mark as complete
