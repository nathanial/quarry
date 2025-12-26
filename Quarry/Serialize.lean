/-
  Quarry.Serialize
  Database serialization to/from ByteArray
-/
import Quarry.Database
import Quarry.FFI.Database

namespace Quarry

namespace Database

/-- Serialize database to ByteArray.

    Returns the complete database as a byte array that can be:
    - Sent over network
    - Stored in files or other databases
    - Used to create clones

    Example:
    ```lean
    let bytes ← db.serialize
    IO.FS.writeBinFile "backup.db" bytes
    ```
-/
def serialize (db : Database) (schema : String := "main") : IO ByteArray :=
  FFI.dbSerialize db.handle schema

/-- Deserialize ByteArray into this database connection.

    Replaces the current database content with the deserialized data.
    The database must not be in an active transaction.

    - `readOnly`: If true, database cannot be modified after deserialize

    Example:
    ```lean
    let bytes ← IO.FS.readBinFile "backup.db"
    db.deserializeInto bytes
    ```
-/
def deserializeInto (db : Database) (data : ByteArray)
    (readOnly : Bool := false) (schema : String := "main") : IO Unit :=
  FFI.dbDeserialize db.handle schema data (if readOnly then 1 else 0)

end Database

/-- Create a new in-memory database from serialized bytes.

    Example:
    ```lean
    let bytes ← IO.FS.readBinFile "backup.db"
    let db ← Database.deserialize bytes
    ```
-/
def Database.deserialize (data : ByteArray) (readOnly : Bool := false) : IO Database := do
  let db ← Database.openMemory
  db.deserializeInto data readOnly
  pure db

/-- Clone a database by serializing and deserializing.

    Creates an independent in-memory copy of the database.

    Example:
    ```lean
    let clone ← db.clone
    ```
-/
def Database.clone (db : Database) : IO Database := do
  let bytes ← db.serialize
  Database.deserialize bytes

end Quarry
