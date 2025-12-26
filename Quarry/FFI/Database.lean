/-
  Quarry.FFI.Database
  Low-level FFI bindings for sqlite3 database operations
-/
import Quarry.FFI.Types
import Quarry.Core.Value

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

-- Busy timeout (milliseconds)
@[extern "quarry_db_busy_timeout"]
opaque dbBusyTimeout (db : @& Database) (ms : UInt32) : IO Unit

-- Interrupt a long-running query
@[extern "quarry_db_interrupt"]
opaque dbInterrupt (db : @& Database) : IO Unit

-- Check if database connection has been interrupted
@[extern "quarry_db_is_interrupted"]
opaque dbIsInterrupted (db : @& Database) : IO Bool

-- User-Defined Functions

/-- Create a scalar SQL function.
    The callback receives an array of Values and returns a Value.
    Use nArgs = -1 for variadic functions. -/
@[extern "quarry_db_create_scalar_function"]
opaque dbCreateScalarFunction (db : @& Database) (name : @& String)
    (nArgs : Int32) (callback : Array Quarry.Value → IO Quarry.Value) : IO Unit

/-- Create an aggregate SQL function (like SUM, AVG, COUNT).
    - init: Returns the initial accumulator value
    - step: Called for each row, updates accumulator
    - final: Called after all rows, produces final result -/
@[extern "quarry_db_create_aggregate_function"]
opaque dbCreateAggregateFunction (db : @& Database) (name : @& String)
    (nArgs : Int32)
    (init : IO Quarry.Value)
    (step : Quarry.Value → Array Quarry.Value → IO Quarry.Value)
    (final : Quarry.Value → IO Quarry.Value)
    : IO Unit

/-- Remove a previously registered function (scalar or aggregate) -/
@[extern "quarry_db_remove_function"]
opaque dbRemoveFunction (db : @& Database) (name : @& String)
    (nArgs : Int32) : IO Unit

-- Update Hook

/-- Set update hook callback.
    The callback receives (opCode, tableName, rowid) where opCode is:
    0 = INSERT, 1 = UPDATE, 2 = DELETE -/
@[extern "quarry_db_set_update_hook"]
opaque dbSetUpdateHook (db : @& Database)
    (callback : UInt8 → String → Int → IO Unit) : IO Unit

/-- Clear update hook -/
@[extern "quarry_db_clear_update_hook"]
opaque dbClearUpdateHook (db : @& Database) : IO Unit

-- Serialize/Deserialize

/-- Serialize database to ByteArray -/
@[extern "quarry_db_serialize"]
opaque dbSerialize (db : @& Database) (schema : @& String) : IO ByteArray

/-- Deserialize ByteArray into database, replacing current content -/
@[extern "quarry_db_deserialize"]
opaque dbDeserialize (db : @& Database) (schema : @& String)
    (data : @& ByteArray) (readOnly : UInt8) : IO Unit

end Quarry.FFI
