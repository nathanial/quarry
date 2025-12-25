/-
  Quarry.FFI.VirtualTable
  Low-level FFI bindings for SQLite virtual table operations
-/
import Quarry.FFI.Types
import Quarry.Core.Value
import Quarry.VirtualTable

namespace Quarry.FFI

/-- Register a virtual table module with the given callbacks.
    This is a low-level function - prefer the high-level wrappers.

    Parameters:
    - db: Database connection
    - name: Module name (used in CREATE VIRTUAL TABLE ... USING name)
    - tableData: Opaque table instance data (τ)
    - schemaFn: Function to get schema from table data
    - bestIndexFn: Query planner callback
    - openFn: Create cursor from filter parameters
    - eofFn: Check if cursor is at end
    - nextFn: Advance cursor to next row
    - columnFn: Get column value at current row
    - rowidFn: Get rowid of current row
    - updateFn: Handle INSERT/UPDATE/DELETE (none for read-only)
-/
@[extern "quarry_db_create_vtab_module"]
opaque dbCreateVTableModule
    {τ σ α β γ δ ε ζ η θ : Type}
    (db : @& Database)
    (name : @& String)
    (tableData : τ)
    (schemaFn : α)        -- τ → VTableSchema
    (bestIndexFn : β)     -- τ → VTableIndexInfo → IO VTableIndexOutput
    (openFn : γ)          -- τ → Int → Array Value → IO σ
    (eofFn : δ)           -- σ → IO Bool
    (nextFn : ε)          -- σ → IO σ
    (columnFn : ζ)        -- σ → Nat → IO Value
    (rowidFn : η)         -- σ → IO Int
    (updateFn : θ)        -- τ → VTableUpdateOp → IO (Option Int)
    : IO Unit

end Quarry.FFI
