# Quarry Roadmap

SQLite features to consider implementing, organized by priority.

## Implemented

Core database operations, transactions, parameter binding, type-safe extraction, user-defined functions (scalar & aggregate), backup API, virtual tables, FTS5, R-Tree, incremental BLOB I/O, update hooks, serialize/deserialize, and comprehensive PRAGMA helpers.

See `Quarry.lean` for the full API surface.

---

## High Priority

Powerful features with broad applicability.

### Trace/Profile
Log every SQL statement with execution timing.

```lean
-- Goal API
db.setTrace fun sql => IO.println s!"SQL: {sql}"
db.setProfile fun sql nanos => IO.println s!"{sql} took {nanos}ns"
```

**Use cases**: Debugging, performance analysis, query logging
**SQLite API**: `sqlite3_trace_v2`

### Preupdate Hook
Like update hook but with access to old/new column values.

```lean
-- Goal API
db.setPreupdateHook fun op table oldRow newRow => do
  IO.println s!"Changed {table}: {oldRow} → {newRow}"
```

**Use cases**: Audit logging, detailed change tracking, undo/redo
**SQLite API**: `sqlite3_preupdate_hook`, `sqlite3_preupdate_old`, `sqlite3_preupdate_new`

---

## Medium Priority

Useful for specific use cases.

### Commit/Rollback Hooks
React to transaction events.

```lean
-- Goal API
db.setCommitHook do IO.println "committing"
db.setRollbackHook do IO.println "rolling back"
```

**SQLite API**: `sqlite3_commit_hook`, `sqlite3_rollback_hook`

### Progress Handler
Callback during long operations (for progress bars, cancellation).

```lean
-- Goal API
db.setProgressHandler 1000 do
  IO.println "still working..."
  return false  -- true to interrupt
```

**SQLite API**: `sqlite3_progress_handler`

### Database Status
Memory usage, cache statistics, schema size.

```lean
-- Goal API
let (current, peak) ← db.status .cacheUsed
let schemaSize ← db.status .schemaUsed
```

**Use cases**: Monitoring, diagnostics, memory profiling
**SQLite API**: `sqlite3_db_status`

### Statement Status
Query performance metrics (rows scanned, sorts, index hits).

```lean
-- Goal API
let rowsScanned ← stmt.status .fullscanStep
let sortOps ← stmt.status .sort
```

**Use cases**: Query optimization, identifying slow queries
**SQLite API**: `sqlite3_stmt_status`

### Expanded SQL
Get SQL with bound parameters substituted.

```lean
-- Goal API
let stmt ← db.prepare "SELECT * FROM users WHERE id = ?"
stmt.bind 1 42
let sql ← stmt.expandedSql  -- "SELECT * FROM users WHERE id = 42"
```

**Use cases**: Logging, debugging
**SQLite API**: `sqlite3_expanded_sql`

### Authorizer
Fine-grained access control for SQL operations.

```lean
-- Goal API
db.setAuthorizer fun action arg1 arg2 dbName trigger =>
  if action == .delete && arg1 == "users" then .deny
  else .ok
```

**SQLite API**: `sqlite3_set_authorizer`

---

## Low Priority / Advanced

Niche features for specialized use cases.

### Session Extension
Track changes and generate changesets for sync.

```lean
-- Goal API
let session ← db.createSession "main"
session.attachTable "users"
-- ... make changes ...
let changeset ← session.changeset
db2.applyChangeset changeset
```

**Use cases**: Offline sync, change replication, undo/redo systems
**SQLite API**: Session extension (`sqlite3session_*`)

### WAL Checkpoint
Manual checkpoint control for WAL mode databases.

```lean
-- Goal API
let (log, checkpointed) ← db.walCheckpoint .truncate
```

**SQLite API**: `sqlite3_wal_checkpoint_v2`

### Custom Collations
Custom string sorting rules.

```lean
-- Goal API
db.createCollation "nocase_french" fun a b => ...
```

**SQLite API**: `sqlite3_create_collation_v2`

### Window Functions
Create custom window functions (OVER clauses).

**SQLite API**: `sqlite3_create_window_function`

### Limits
Set maximum columns, query length, etc. for security hardening.

**SQLite API**: `sqlite3_limit`

### Snapshots
Point-in-time snapshots for WAL mode databases.

**SQLite API**: `sqlite3_snapshot_*`

---

## Contributing

To implement a feature:

1. Add C FFI functions to `native/src/quarry_ffi.c`
2. Add Lean FFI bindings to `Quarry/FFI/*.lean`
3. Add high-level API to the appropriate module
4. Add tests
5. Update this roadmap
