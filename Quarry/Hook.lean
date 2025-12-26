/-
  Quarry.Hook
  High-level API for SQLite hooks (update, commit, rollback)
-/
import Quarry.Database
import Quarry.FFI.Database

namespace Quarry

/-- Type of database operation that triggered the update hook -/
inductive UpdateOp where
  | insert
  | update
  | delete
  deriving Repr, BEq, Inhabited

namespace UpdateOp

def toString : UpdateOp → String
  | .insert => "INSERT"
  | .update => "UPDATE"
  | .delete => "DELETE"

instance : ToString UpdateOp := ⟨toString⟩

end UpdateOp

namespace Database

/-- Set a hook to be called on INSERT, UPDATE, or DELETE operations.

    The hook receives:
    - `op`: The operation type (insert, update, delete)
    - `table`: The table name
    - `rowid`: The rowid of the affected row

    **Important**: The hook is called synchronously during the operation.
    Do not execute SQL or modify the database from within the hook.

    Example:
    ```lean
    db.setUpdateHook fun op table rowid => do
      IO.println s!"{op} on {table} row {rowid}"
    ```
-/
def setUpdateHook (db : Database)
    (hook : UpdateOp → String → Int → IO Unit) : IO Unit := do
  let callback : UInt8 → String → Int → IO Unit := fun opCode table rowid => do
    let op := match opCode with
      | 0 => UpdateOp.insert
      | 1 => UpdateOp.update
      | _ => UpdateOp.delete
    hook op table rowid
  FFI.dbSetUpdateHook db.handle callback

/-- Remove the update hook -/
def clearUpdateHook (db : Database) : IO Unit :=
  FFI.dbClearUpdateHook db.handle

end Database

end Quarry
