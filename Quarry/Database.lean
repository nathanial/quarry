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

/-- Execute SQL that doesn't return results (CREATE, INSERT, UPDATE, DELETE) -/
def exec (db : Database) (sql : String) : IO Unit :=
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
  db.exec "BEGIN TRANSACTION"
  try
    let result ← f
    db.exec "COMMIT"
    return result
  catch e =>
    db.exec "ROLLBACK"
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
  db.exec s!"PRAGMA synchronous={mode.toInt}"

end Database

end Quarry
