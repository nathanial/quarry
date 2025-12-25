/-
  Quarry.Backup
  High-level API for SQLite online backup
-/
import Quarry.Database
import Quarry.FFI.Backup

namespace Quarry

/-- SQLite result codes for backup operations -/
private def sqliteOk : Int := 0
private def sqliteDone : Int := 101

/-- Backup handle with high-level operations.
    Stores references to source and destination databases to prevent
    garbage collection during the backup operation. -/
structure Backup where
  private mk ::
  handle : FFI.Backup
  /-- Destination database - kept alive during backup -/
  destDb : Database
  /-- Source database - kept alive during backup -/
  srcDb : Database

namespace Backup

/-- Initialize a backup from source to destination database.
    Copies data from `src` to `dest`. -/
def init (dest : Database) (src : Database)
    (destName : String := "main") (srcName : String := "main") : IO Backup := do
  let handle ← FFI.backupInit dest.handle destName src.handle srcName
  return ⟨handle, dest, src⟩

/-- Perform a step of the backup, copying up to `nPages` pages.
    Returns `true` if there are more pages to copy, `false` when done.
    Throws on error. -/
def step (backup : Backup) (nPages : Int := -1) : IO Bool := do
  let rc ← FFI.backupStep backup.handle nPages.toInt32
  if rc == sqliteOk then
    return true  -- More pages to copy
  else if rc == sqliteDone then
    return false -- Backup complete
  else
    throw (IO.userError s!"Backup step failed with code {rc}")

/-- Finish and release the backup.
    This should be called when done, though the finalizer will also clean up. -/
def finish (backup : Backup) : IO Unit := do
  let rc ← FFI.backupFinish backup.handle
  if rc != sqliteOk then
    throw (IO.userError s!"Backup finish failed with code {rc}")

/-- Get the number of pages remaining to be backed up -/
def remaining (backup : Backup) : IO Nat := do
  let n ← FFI.backupRemaining backup.handle
  return n.toNat

/-- Get the total number of pages in the source database -/
def pageCount (backup : Backup) : IO Nat := do
  let n ← FFI.backupPageCount backup.handle
  return n.toNat

/-- Get backup progress as a percentage (0-100) -/
def progress (backup : Backup) : IO Float := do
  let total ← backup.pageCount
  let rem ← backup.remaining
  if total == 0 then return 100.0
  let done := total - rem
  return (done.toFloat / total.toFloat) * 100.0

/-- Check if the backup is complete -/
def isDone (backup : Backup) : IO Bool := do
  let rem ← backup.remaining
  return rem == 0

/-- Run the entire backup in one step -/
def runAll (backup : Backup) : IO Unit := do
  let _ ← backup.step (-1)
  backup.finish

end Backup

namespace Database

/-- Backup this database to a file.
    Creates a new database file at the given path with a complete copy.
    This is a convenience method that handles the full backup process. -/
def backupToFile (db : Database) (path : String) : IO Unit := do
  let destDb ← Database.openFile path
  let backup ← Backup.init destDb db
  let _ ← backup.step (-1)  -- Copy all pages
  backup.finish
  -- Keep destDb alive until after backup completes
  pure ()

/-- Backup this database to another open database connection.
    Useful for backing up to an in-memory database or another open file. -/
def backupTo (db : Database) (dest : Database) : IO Unit := do
  let backup ← Backup.init dest db
  backup.runAll

/-- Initialize an incremental backup from this database to a destination.
    Use for large databases where you want progress feedback or to yield
    between steps.

    Example:
    ```lean
    let backup ← db.backupInit destDb
    while !(← backup.isDone) do
      let _ ← backup.step 100
      IO.println s!"Progress: {← backup.progress}%"
    backup.finish
    ``` -/
def backupInit (db : Database) (dest : Database)
    (srcName : String := "main") (destName : String := "main") : IO Backup :=
  Backup.init dest db destName srcName

end Database

end Quarry
