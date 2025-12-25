/-
  Quarry.FFI.Backup
  Low-level FFI bindings for sqlite3_backup operations
-/
import Quarry.FFI.Types

namespace Quarry.FFI

/-- Initialize a backup from source database to destination database.
    Both databases must be open. Copies from srcDb to destDb.
    The name parameters are typically "main" for the main database. -/
@[extern "quarry_backup_init"]
opaque backupInit (destDb : @& Database) (destName : @& String)
    (srcDb : @& Database) (srcName : @& String) : IO Backup

/-- Perform a step of the backup, copying up to nPages pages.
    Use -1 to copy all remaining pages in one step.
    Returns SQLite result code:
    - 0 (SQLITE_OK): more pages to copy
    - 101 (SQLITE_DONE): backup complete
    - Other: error -/
@[extern "quarry_backup_step"]
opaque backupStep (backup : @& Backup) (nPages : Int32) : IO Int

/-- Finish and release the backup handle.
    Returns SQLITE_OK (0) on success. -/
@[extern "quarry_backup_finish"]
opaque backupFinish (backup : @& Backup) : IO Int

/-- Get the number of pages remaining to be backed up -/
@[extern "quarry_backup_remaining"]
opaque backupRemaining (backup : @& Backup) : IO Int

/-- Get the total number of pages in the source database -/
@[extern "quarry_backup_page_count"]
opaque backupPageCount (backup : @& Backup) : IO Int

end Quarry.FFI
