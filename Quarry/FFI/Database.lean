/-
  Quarry.FFI.Database
  Low-level FFI bindings for sqlite3 database operations
-/
import Quarry.FFI.Types

namespace Quarry.FFI

-- Database lifecycle
@[extern "quarry_db_open"]
opaque dbOpen (path : @& String) : IO Database

@[extern "quarry_db_open_memory"]
opaque dbOpenMemory : IO Database

@[extern "quarry_db_close"]
opaque dbClose (db : @& Database) : IO Unit

-- Error handling
@[extern "quarry_db_errmsg"]
opaque dbErrmsg (db : @& Database) : IO String

@[extern "quarry_db_errcode"]
opaque dbErrcode (db : @& Database) : IO Int

-- Direct execution (for statements without results)
@[extern "quarry_db_exec"]
opaque dbExec (db : @& Database) (sql : @& String) : IO Unit

-- Last insert rowid
@[extern "quarry_db_last_insert_rowid"]
opaque dbLastInsertRowid (db : @& Database) : IO Int

-- Changes count
@[extern "quarry_db_changes"]
opaque dbChanges (db : @& Database) : IO Int

-- Total changes count
@[extern "quarry_db_total_changes"]
opaque dbTotalChanges (db : @& Database) : IO Int

end Quarry.FFI
