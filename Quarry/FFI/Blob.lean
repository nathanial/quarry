/-
  Quarry.FFI.Blob
  Low-level FFI bindings for SQLite incremental BLOB I/O
-/
import Quarry.FFI.Types

namespace Quarry.FFI

/-- Open a blob for incremental I/O.
    flags: 0 = read-only, 1 = read-write -/
@[extern "quarry_blob_open"]
opaque blobOpen (db : @& Database) (dbName : @& String)
    (table : @& String) (column : @& String)
    (rowid : @& Int) (flags : UInt8) : IO Blob

/-- Read bytes from blob at offset -/
@[extern "quarry_blob_read"]
opaque blobRead (blob : @& Blob) (offset : UInt32) (size : UInt32) : IO ByteArray

/-- Write bytes to blob at offset -/
@[extern "quarry_blob_write"]
opaque blobWrite (blob : @& Blob) (offset : UInt32) (data : @& ByteArray) : IO Unit

/-- Get total blob size in bytes -/
@[extern "quarry_blob_bytes"]
opaque blobBytes (blob : @& Blob) : IO Int

/-- Close blob handle explicitly -/
@[extern "quarry_blob_close"]
opaque blobClose (blob : @& Blob) : IO Unit

/-- Reopen blob for a different row (reuses handle) -/
@[extern "quarry_blob_reopen"]
opaque blobReopen (blob : @& Blob) (newRowid : @& Int) : IO Unit

end Quarry.FFI
