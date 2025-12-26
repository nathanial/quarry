/-
  Quarry.FFI.Types
  Opaque FFI handle types
-/

namespace Quarry.FFI

/-- Opaque handle to sqlite3 database connection -/
opaque DatabasePointed : NonemptyType
def Database := DatabasePointed.type
instance : Nonempty Database := DatabasePointed.property

/-- Opaque handle to sqlite3_stmt prepared statement -/
opaque StatementPointed : NonemptyType
def Statement := StatementPointed.type
instance : Nonempty Statement := StatementPointed.property

/-- Opaque handle to sqlite3_backup backup operation -/
opaque BackupPointed : NonemptyType
def Backup := BackupPointed.type
instance : Nonempty Backup := BackupPointed.property

/-- Opaque handle to sqlite3_blob for incremental BLOB I/O -/
opaque BlobPointed : NonemptyType
def Blob := BlobPointed.type
instance : Nonempty Blob := BlobPointed.property

end Quarry.FFI
