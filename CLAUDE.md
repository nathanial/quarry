# CLAUDE.md

SQLite library for Lean 4 using the vendored amalgamation (no system dependencies).

## Build Commands

```bash
./build.sh         # Downloads SQLite amalgamation and builds (required first time)
lake build         # Build library (after SQLite downloaded)
lake test          # Run tests
```

The `build.sh` script downloads SQLite amalgamation to `native/sqlite/` on first run.

## Dependencies

- crucible - Test framework
- staple - Utility macros
- chisel - SQL DSL for type-safe statement parsing

## Project Structure

```
Quarry/
├── Core/           # Value, Row, Column, Error types
├── FFI/            # Low-level SQLite C bindings
│   ├── Database.lean
│   ├── Statement.lean
│   ├── Backup.lean
│   ├── Blob.lean
│   └── VirtualTable.lean
├── Chisel/         # Integration with Chisel SQL DSL
├── Database.lean   # High-level database API
├── Bind.lean       # ToSql typeclass for parameter binding
├── Extract.lean    # FromSql typeclass for result extraction
├── Transaction.lean
├── Function.lean   # User-defined SQL functions
├── VirtualTable.lean
├── Backup.lean
├── Blob.lean
├── Hook.lean       # Update/commit hooks
└── Serialize.lean
native/
├── sqlite/         # SQLite amalgamation (downloaded by build.sh)
└── src/
    └── quarry_ffi.c
Tests/
```

## Key APIs

```lean
-- Open database
Database.openFile : String → IO Database
Database.openMemory : IO Database

-- Execute SQL
Database.exec : Database → String → IO Unit
Database.query : Database → String → IO (Array Row)

-- With Chisel parsing
Database.execSqlDdl : Database → String → IO Unit
Database.execSqlSelect : Database → String → IO (Array Row)
Database.execSqlInsert : Database → String → IO Int
Database.execSqlModify : Database → String → IO Int

-- Transactions
Database.transaction : Database → IO α → IO α
Database.withSavepoint : Database → String → IO α → IO α
```

## FFI Pattern

Uses external class registration for SQLite handles:

```c
static lean_external_class* g_db_class = NULL;
g_db_class = lean_register_external_class(db_finalizer, NULL);
lean_alloc_external(g_db_class, sqlite3_ptr);
```

Lean side:
```lean
opaque DatabasePointed : NonemptyType
def Database : Type := DatabasePointed.type

@[extern "lean_sqlite_open"]
opaque Database.openFile : String → IO Database
```

## Type Classes

```lean
-- For binding parameters
class ToSql (α : Type) where
  toSql : α → Value

-- For extracting results
class FromSql (α : Type) where
  fromSql : Value → SqlResult α
```

Built-in instances: Int, Nat, Float, String, ByteArray, Bool, Option α
