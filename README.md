# Quarry

SQLite library for Lean 4 using the amalgamated source (no system dependencies).

## Features

- **Zero dependencies**: Uses vendored SQLite amalgamation
- **Type-safe binding**: `ToSql` typeclass for parameter binding
- **Type-safe extraction**: `FromSql` typeclass for result extraction
- **Both binding styles**: Positional (`?1`) and named (`:param`) parameters
- **Transaction support**: RAII-style with automatic rollback on error
- **Savepoints**: Nested transaction support

## Installation

Add to your `lakefile.lean`:

```lean
require quarry from git "https://github.com/nathanial/quarry" @ "v0.0.1"
```

## Quick Start

```lean
import Quarry

def main : IO Unit := do
  -- Open in-memory database
  let db ← Quarry.Database.openMemory

  -- Create a table
  db.exec "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"

  -- Insert data
  db.exec "INSERT INTO users (name, age) VALUES ('Alice', 30)"
  db.exec "INSERT INTO users (name, age) VALUES ('Bob', 25)"

  -- Query data
  let rows ← db.query "SELECT * FROM users"
  for row in rows do
    IO.println row

  -- Use transactions
  db.transaction do
    db.exec "UPDATE users SET age = age + 1"
```

## API

### Database Operations

```lean
-- Open database
Database.openFile : String → IO Database
Database.openMemory : IO Database

-- Execute SQL (no results)
Database.exec : Database → String → IO Unit

-- Query with results
Database.query : Database → String → IO (Array Row)
Database.queryOne : Database → String → IO (Option Row)

-- Metadata
Database.lastInsertRowid : Database → IO Int
Database.changes : Database → IO Int
```

### Value Types

```lean
inductive Value where
  | null
  | integer (v : Int)
  | real (v : Float)
  | text (v : String)
  | blob (v : ByteArray)
```

### Row Access

```lean
-- By index
Row.get? : Row → Nat → Option Value

-- By column name (case-insensitive)
Row.getByName? : Row → String → Option Value

-- With type conversion
Row.getAs [FromSql α] : Row → Nat → SqlResult α
Row.getByNameAs [FromSql α] : Row → String → SqlResult α
```

### Transactions

```lean
-- Basic transaction
Database.transaction : Database → IO α → IO α

-- Savepoints (nested transactions)
Database.withSavepoint : Database → String → IO α → IO α

-- Transaction types
Database.readTransaction : Database → IO α → IO α
Database.writeTransaction : Database → IO α → IO α
```

### Type Classes

```lean
-- For binding parameters
class ToSql (α : Type) where
  toSql : α → Value

-- For extracting results
class FromSql (α : Type) where
  fromSql : Value → SqlResult α
```

Built-in instances: `Int`, `Nat`, `Float`, `String`, `ByteArray`, `Bool`, `Option α`, `Value`

## Building

```bash
./build.sh        # Downloads SQLite amalgamation and builds
lake build        # Build library (after SQLite is downloaded)
lake test         # Run tests
```

## Project Structure

```
quarry/
├── Quarry/
│   ├── Core/           # Value, Row, Column, Error types
│   ├── FFI/            # Low-level SQLite bindings
│   ├── Database.lean   # High-level database API
│   ├── Bind.lean       # Parameter binding (ToSql)
│   ├── Extract.lean    # Result extraction (FromSql)
│   └── Transaction.lean
├── native/
│   ├── sqlite/         # SQLite amalgamation (downloaded)
│   └── src/
│       └── quarry_ffi.c
└── Tests/
```

## License

MIT License - see [LICENSE](LICENSE)
