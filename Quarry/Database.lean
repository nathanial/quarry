/-
  Quarry.Database
  High-level database connection API
-/
import Quarry.Core.Error
import Quarry.Core.Value
import Quarry.Core.Row
import Quarry.Core.Column
import Quarry.FFI.Database
import Quarry.FFI.Statement

namespace Quarry

/-- Database connection wrapper with high-level operations -/
structure Database where
  private mk ::
  handle : FFI.Database

namespace Database

/-- Open a database file -/
def openFile (path : String) : IO Database := do
  let handle ← FFI.dbOpen path
  return ⟨handle⟩

/-- Open an in-memory database -/
def openMemory : IO Database := do
  let handle ← FFI.dbOpenMemory
  return ⟨handle⟩

/-- Close the database connection -/
def close (db : Database) : IO Unit :=
  FFI.dbClose db.handle

/-- Execute raw SQL that doesn't return results (CREATE, INSERT, UPDATE, DELETE).
    Prefer execSql from Quarry.Chisel.Execute for parsed SQL execution. -/
def execRaw (db : Database) (sql : String) : IO Unit :=
  FFI.dbExec db.handle sql

/-- Get the last inserted rowid -/
def lastInsertRowid (db : Database) : IO Int :=
  FFI.dbLastInsertRowid db.handle

/-- Get number of rows changed by last statement -/
def changes (db : Database) : IO Int :=
  FFI.dbChanges db.handle

/-- Get total number of rows changed since connection opened -/
def totalChanges (db : Database) : IO Int :=
  FFI.dbTotalChanges db.handle

/-- Get the last error message -/
def errorMessage (db : Database) : IO String :=
  FFI.dbErrmsg db.handle

/-- Get the last error code -/
def errorCode (db : Database) : IO SqliteCode := do
  let code ← FFI.dbErrcode db.handle
  return SqliteCode.fromInt code

/-- SQLite type code constants -/
private def sqliteRow : Int := 100
private def sqliteDone : Int := 101
private def sqliteInteger : Int := 1
private def sqliteFloat : Int := 2
private def sqliteText : Int := 3
private def sqliteBlob : Int := 4

/-- Execute a query and return all rows -/
def query (db : Database) (sql : String) : IO (Array Row) := do
  let stmt ← FFI.stmtPrepare db.handle sql
  let mut rows : Array Row := #[]

  -- Get column metadata
  let colCount ← FFI.stmtColumnCount stmt
  let mut columns : Array Column := #[]
  for i in [:colCount.toNat] do
    let name ← FFI.stmtColumnName stmt i.toUInt32
    columns := columns.push ⟨name, none, none⟩

  -- Fetch rows
  let mut done := false
  while !done do
    let rc ← FFI.stmtStep stmt
    if rc == sqliteRow then
      let mut values : Array Value := #[]
      for i in [:colCount.toNat] do
        let colType ← FFI.stmtColumnType stmt i.toUInt32
        let value ← match colType with
          | 1 => do  -- SQLITE_INTEGER
            let v ← FFI.stmtColumnInt stmt i.toUInt32
            pure (Value.integer v)
          | 2 => do  -- SQLITE_FLOAT
            let v ← FFI.stmtColumnDouble stmt i.toUInt32
            pure (Value.real v)
          | 3 => do  -- SQLITE_TEXT
            let v ← FFI.stmtColumnText stmt i.toUInt32
            pure (Value.text v)
          | 4 => do  -- SQLITE_BLOB
            let v ← FFI.stmtColumnBlob stmt i.toUInt32
            pure (Value.blob v)
          | _ => pure Value.null  -- SQLITE_NULL or unknown
        values := values.push value
      rows := rows.push ⟨values, columns⟩
    else if rc == sqliteDone then
      done := true
    else
      let msg ← FFI.dbErrmsg db.handle
      throw (IO.userError msg)

  return rows

/-- Execute a query and return the first row (or none) -/
def queryOne (db : Database) (sql : String) : IO (Option Row) := do
  let rows ← db.query sql
  return rows[0]?

/-- Execute a query with a callback for each row -/
def queryForEach (db : Database) (sql : String) (f : Row -> IO Unit) : IO Unit := do
  let rows ← db.query sql
  for row in rows do
    f row

/-- Run operations inside a transaction -/
def transaction (db : Database) (f : IO α) : IO α := do
  db.execRaw "BEGIN TRANSACTION"
  try
    let result ← f
    db.execRaw "COMMIT"
    return result
  catch e =>
    db.execRaw "ROLLBACK"
    throw e

/-- Prepare a statement for repeated execution -/
def prepare (db : Database) (sql : String) : IO FFI.Statement :=
  FFI.stmtPrepare db.handle sql

/-- Set busy timeout in milliseconds.
    When another connection holds a lock, SQLite will retry for up to this
    many milliseconds before returning SQLITE_BUSY. Default is 0 (return immediately). -/
def busyTimeout (db : Database) (ms : UInt32) : IO Unit :=
  FFI.dbBusyTimeout db.handle ms

/-- Interrupt a long-running query.
    This can be called from another thread to cancel the current operation.
    The interrupted operation will return SQLITE_INTERRUPT.
    This is safe to call even if no operation is in progress. -/
def interrupt (db : Database) : IO Unit :=
  FFI.dbInterrupt db.handle

/-- Check if the database connection has been interrupted.
    Returns true if sqlite3_interrupt has been called and not yet cleared. -/
def isInterrupted (db : Database) : IO Bool :=
  FFI.dbIsInterrupted db.handle

/-- Journal mode for the database -/
inductive JournalMode where
  | delete    -- Default: delete journal after commit
  | truncate  -- Truncate journal to zero length
  | persist   -- Keep journal file, zero header
  | memory    -- Store journal in memory
  | wal       -- Write-Ahead Logging (best for concurrent reads)
  | off       -- Disable journaling (dangerous!)
  deriving Repr, BEq

namespace JournalMode
def toString : JournalMode -> String
  | .delete => "DELETE"
  | .truncate => "TRUNCATE"
  | .persist => "PERSIST"
  | .memory => "MEMORY"
  | .wal => "WAL"
  | .off => "OFF"

def fromString? : String -> Option JournalMode
  | "delete" | "DELETE" => some .delete
  | "truncate" | "TRUNCATE" => some .truncate
  | "persist" | "PERSIST" => some .persist
  | "memory" | "MEMORY" => some .memory
  | "wal" | "WAL" => some .wal
  | "off" | "OFF" => some .off
  | _ => none
end JournalMode

/-- Set the journal mode. Returns the new journal mode (may differ from requested
    if the mode couldn't be set, e.g., WAL on in-memory databases). -/
def setJournalMode (db : Database) (mode : JournalMode) : IO JournalMode := do
  let rows ← db.query s!"PRAGMA journal_mode={mode.toString}"
  match rows[0]? with
  | some row =>
    match row.get? 0 with
    | some (Value.text s) =>
      match JournalMode.fromString? s with
      | some m => return m
      | none => return mode  -- Fallback
    | _ => return mode
  | none => return mode

/-- Get the current journal mode -/
def getJournalMode (db : Database) : IO JournalMode := do
  let rows ← db.query "PRAGMA journal_mode"
  match rows[0]? with
  | some row =>
    match row.get? 0 with
    | some (Value.text s) =>
      match JournalMode.fromString? s with
      | some m => return m
      | none => return .delete  -- Fallback to default
    | _ => return .delete
  | none => return .delete

/-- Enable WAL mode for better concurrent read performance.
    Returns true if WAL was successfully enabled. -/
def enableWAL (db : Database) : IO Bool := do
  let mode ← db.setJournalMode .wal
  return mode == .wal

/-- Synchronous mode for the database -/
inductive SyncMode where
  | off     -- No syncs (fastest, but unsafe on crash)
  | normal  -- Sync at critical moments
  | full    -- Sync after each transaction (default, safest)
  | extra   -- Extra syncs for extra safety
  deriving Repr, BEq

namespace SyncMode
def toInt : SyncMode -> Int
  | .off => 0
  | .normal => 1
  | .full => 2
  | .extra => 3
end SyncMode

/-- Set the synchronous mode -/
def setSynchronous (db : Database) (mode : SyncMode) : IO Unit :=
  db.execRaw s!"PRAGMA synchronous={mode.toInt}"

/-- Get the synchronous mode -/
def getSynchronous (db : Database) : IO SyncMode := do
  let rows ← db.query "PRAGMA synchronous"
  match rows[0]?.bind (·.get? 0) with
  | some (Value.integer 0) => return .off
  | some (Value.integer 1) => return .normal
  | some (Value.integer 2) => return .full
  | some (Value.integer 3) => return .extra
  | _ => return .full  -- Default

-- ============================================================================
-- PRAGMA Helpers
-- ============================================================================

/-- Enable or disable foreign key enforcement.
    Note: This must be set before any tables are accessed in a session. -/
def setForeignKeys (db : Database) (enabled : Bool) : IO Unit :=
  db.execRaw s!"PRAGMA foreign_keys = {if enabled then 1 else 0}"

/-- Check if foreign key enforcement is enabled -/
def getForeignKeys (db : Database) : IO Bool := do
  let rows ← db.query "PRAGMA foreign_keys"
  match rows[0]?.bind (·.get? 0) with
  | some (Value.integer n) => return n != 0
  | _ => return false

/-- Set the page cache size (in pages, or negative for KiB).
    Default is -2000 (2MB). -/
def setCacheSize (db : Database) (size : Int) : IO Unit :=
  db.execRaw s!"PRAGMA cache_size = {size}"

/-- Get the current page cache size -/
def getCacheSize (db : Database) : IO Int := do
  let rows ← db.query "PRAGMA cache_size"
  match rows[0]?.bind (·.get? 0) with
  | some (Value.integer n) => return n
  | _ => return -2000  -- Default

/-- Temporary storage location -/
inductive TempStore where
  | default  -- Use compile-time default (usually file)
  | file     -- Store temp tables in a file
  | memory   -- Store temp tables in memory
  deriving Repr, BEq

namespace TempStore
def toInt : TempStore → Int
  | .default => 0
  | .file => 1
  | .memory => 2

def fromInt : Int → TempStore
  | 0 => .default
  | 1 => .file
  | 2 => .memory
  | _ => .default
end TempStore

/-- Set where temporary tables and indices are stored -/
def setTempStore (db : Database) (mode : TempStore) : IO Unit :=
  db.execRaw s!"PRAGMA temp_store = {mode.toInt}"

/-- Get the current temporary storage mode -/
def getTempStore (db : Database) : IO TempStore := do
  let rows ← db.query "PRAGMA temp_store"
  match rows[0]?.bind (·.get? 0) with
  | some (Value.integer n) => return TempStore.fromInt n
  | _ => return .default

/-- Auto-vacuum mode -/
inductive AutoVacuum where
  | none         -- No auto-vacuum (default)
  | full         -- Full auto-vacuum after each transaction
  | incremental  -- Incremental vacuum (must call incremental_vacuum)
  deriving Repr, BEq

namespace AutoVacuum
def toInt : AutoVacuum → Int
  | .none => 0
  | .full => 1
  | .incremental => 2

def fromInt : Int → AutoVacuum
  | 0 => .none
  | 1 => .full
  | 2 => .incremental
  | _ => .none
end AutoVacuum

/-- Set the auto-vacuum mode.
    Note: Can only be changed when the database is empty (before first table). -/
def setAutoVacuum (db : Database) (mode : AutoVacuum) : IO Unit :=
  db.execRaw s!"PRAGMA auto_vacuum = {mode.toInt}"

/-- Get the current auto-vacuum mode -/
def getAutoVacuum (db : Database) : IO AutoVacuum := do
  let rows ← db.query "PRAGMA auto_vacuum"
  match rows[0]?.bind (·.get? 0) with
  | some (Value.integer n) => return AutoVacuum.fromInt n
  | _ => return .none

/-- Run incremental vacuum to reclaim free pages.
    Only works if auto_vacuum is set to incremental.
    Pass 0 to vacuum all freelist pages, or n to vacuum at most n pages. -/
def incrementalVacuum (db : Database) (pages : Nat := 0) : IO Unit :=
  db.execRaw s!"PRAGMA incremental_vacuum({pages})"

/-- Get the database text encoding (UTF-8, UTF-16le, or UTF-16be).
    This is read-only after the first table is created. -/
def getEncoding (db : Database) : IO String := do
  let rows ← db.query "PRAGMA encoding"
  match rows[0]?.bind (·.get? 0) with
  | some (Value.text s) => return s
  | _ => return "UTF-8"  -- Default

/-- Set the database page size (must be power of 2, 512 to 65536).
    Can only be set before any tables are created, or via VACUUM. -/
def setPageSize (db : Database) (size : Nat) : IO Unit :=
  db.execRaw s!"PRAGMA page_size = {size}"

/-- Get the current page size in bytes -/
def getPageSize (db : Database) : IO Nat := do
  let rows ← db.query "PRAGMA page_size"
  match rows[0]?.bind (·.get? 0) with
  | some (Value.integer n) => return n.toNat
  | _ => return 4096  -- Default

/-- Set the maximum number of pages in the database file.
    Use 0 for no limit. -/
def setMaxPageCount (db : Database) (count : Nat) : IO Unit :=
  db.execRaw s!"PRAGMA max_page_count = {count}"

/-- Get the maximum page count (0 means no limit) -/
def getMaxPageCount (db : Database) : IO Nat := do
  let rows ← db.query "PRAGMA max_page_count"
  match rows[0]?.bind (·.get? 0) with
  | some (Value.integer n) => return n.toNat
  | _ => return 0

/-- Get the current number of pages in the database -/
def getPageCount (db : Database) : IO Nat := do
  let rows ← db.query "PRAGMA page_count"
  match rows[0]?.bind (·.get? 0) with
  | some (Value.integer n) => return n.toNat
  | _ => return 0

/-- Get the number of unused pages in the database -/
def getFreelistCount (db : Database) : IO Nat := do
  let rows ← db.query "PRAGMA freelist_count"
  match rows[0]?.bind (·.get? 0) with
  | some (Value.integer n) => return n.toNat
  | _ => return 0

/-- Create a scalar SQL function.
    The callback receives an array of Values and returns a Value.
    Use nArgs = -1 for variadic functions. -/
def createScalarFunction (db : Database) (name : String) (nArgs : Int)
    (f : Array Value → IO Value) : IO Unit :=
  FFI.dbCreateScalarFunction db.handle name nArgs.toInt32 f

/-- Create an aggregate SQL function (like SUM, AVG, COUNT).
    - init: Returns the initial accumulator value
    - step: Called for each row, takes current accumulator and row values, returns new accumulator
    - final: Called after all rows, takes final accumulator and returns result -/
def createAggregateFunction (db : Database) (name : String) (nArgs : Int)
    (init : IO Value)
    (step : Value → Array Value → IO Value)
    (final : Value → IO Value) : IO Unit :=
  FFI.dbCreateAggregateFunction db.handle name nArgs.toInt32 init step final

/-- Remove a previously registered function (scalar or aggregate) -/
def removeFunction (db : Database) (name : String) (nArgs : Int) : IO Unit :=
  FFI.dbRemoveFunction db.handle name nArgs.toInt32

/-- Get metadata about a result column's source.
    Returns information about which database, table, and column the result came from.
    For expressions or computed columns, some fields may be none.
    The statement must be prepared and have column information available. -/
def columnMetadata (stmt : FFI.Statement) (idx : Nat) : IO ColumnMetadata := do
  let i := idx.toUInt32
  let database ← FFI.stmtColumnDatabaseName stmt i
  let table ← FFI.stmtColumnTableName stmt i
  let originName ← FFI.stmtColumnOriginName stmt i
  return { database, table, originName }

/-- Get metadata for all columns in a prepared statement -/
def allColumnMetadata (stmt : FFI.Statement) : IO (Array ColumnMetadata) := do
  let count ← FFI.stmtColumnCount stmt
  let mut result : Array ColumnMetadata := #[]
  for i in [:count.toNat] do
    let m ← columnMetadata stmt i
    result := result.push m
  return result

end Database

end Quarry
