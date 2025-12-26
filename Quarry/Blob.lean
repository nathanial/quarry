/-
  Quarry.Blob
  High-level API for SQLite incremental BLOB I/O
-/
import Quarry.Database
import Quarry.FFI.Blob

namespace Quarry

/-- Blob open mode -/
inductive BlobMode where
  | readOnly
  | readWrite
  deriving Repr, BEq

/-- Handle for streaming BLOB I/O.
    Allows reading and writing portions of a blob without loading
    the entire blob into memory. Keeps database reference alive
    during blob operations.

    Note: BLOB I/O cannot change blob size - use UPDATE for that. -/
structure Blob where
  private mk ::
  handle : FFI.Blob
  /-- Database reference - kept alive during blob operations -/
  db : Database

namespace Blob

/-- Read bytes from blob starting at offset.

    Example:
    ```lean
    let chunk ← blob.read 0 1024  -- read first 1KB
    ``` -/
def read (blob : Blob) (offset : Nat) (size : Nat) : IO ByteArray :=
  FFI.blobRead blob.handle offset.toUInt32 size.toUInt32

/-- Write bytes to blob starting at offset.
    Cannot resize the blob - the data must fit within the existing blob size.

    Example:
    ```lean
    blob.write 0 newData
    ``` -/
def write (blob : Blob) (offset : Nat) (data : ByteArray) : IO Unit :=
  FFI.blobWrite blob.handle offset.toUInt32 data

/-- Get total blob size in bytes -/
def bytes (blob : Blob) : IO Nat := do
  let size ← FFI.blobBytes blob.handle
  pure size.toNat

/-- Close blob handle explicitly.
    This is optional - the finalizer will clean up automatically.
    Calling close multiple times is safe (idempotent). -/
def close (blob : Blob) : IO Unit :=
  FFI.blobClose blob.handle

/-- Reopen blob for a different row (reuses handle).
    Useful when iterating over multiple rows with blob data. -/
def reopen (blob : Blob) (newRowid : Int) : IO Unit :=
  FFI.blobReopen blob.handle newRowid

/-- Read the entire blob into memory -/
def readAll (blob : Blob) : IO ByteArray := do
  let size ← blob.bytes
  blob.read 0 size

end Blob

namespace Database

/-- Open a blob for streaming read/write access.

    Example:
    ```lean
    let blob ← db.openBlob "files" "content" rowid .readWrite
    let data ← blob.read 0 1024
    blob.write 0 newData
    blob.close
    ```

    Note: The blob must exist (INSERT a row first), and BLOB I/O
    cannot change the blob size - use UPDATE for that. -/
def openBlob (db : Database) (table : String) (column : String)
    (rowid : Int) (mode : BlobMode := .readOnly)
    (dbName : String := "main") : IO Blob := do
  let flags : UInt8 := match mode with
    | .readOnly => 0
    | .readWrite => 1
  let handle ← FFI.blobOpen db.handle dbName table column rowid flags
  pure ⟨handle, db⟩

end Database

end Quarry
