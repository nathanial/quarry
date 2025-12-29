/-
  Quarry.VirtualTable.Generator
  Read-only streaming virtual table backed by a generator function
-/
import Quarry.VirtualTable
import Quarry.FFI.VirtualTable
import Quarry.Database

namespace Quarry.VirtualTable

/-- Generator configuration for a streaming virtual table.
    The generator produces rows on demand without storing them in memory.

    Type parameter `σ` is the cursor state type maintained during iteration.

    Example:
    ```lean
    -- Generate series of integers
    let gen : Generator (Int × Int × Int) := {
      schema := { columns := #[{ name := "value", sqlType := "INTEGER" }] }
      init := fun args => do
        let start := args[0]?.bind Value.toInt |>.getD 1
        let stop := args[1]?.bind Value.toInt |>.getD 10
        pure (start, stop, start)  -- (start, stop, current)
      hasMore := fun (_, stop, current) => current <= stop
      current := fun (_, _, current) => #[.integer current]
      advance := fun (start, stop, current) => (start, stop, current + 1)
    }
    ```
-/
structure Generator (σ : Type) where
  /-- Schema for the virtual table -/
  schema : VTableSchema
  /-- Initialize cursor state from filter arguments.
      Arguments come from hidden columns or WHERE clause constraints. -/
  init : Array Value → IO σ
  /-- Check if there are more rows to generate -/
  hasMore : σ → Bool
  /-- Get column values for current row -/
  current : σ → Array Value
  /-- Advance to next row (pure state update) -/
  advance : σ → σ
  /-- Optional: get rowid for current row (defaults to auto-increment) -/
  rowid : Option (σ → Int) := none

/-- Internal cursor that wraps the state with a rowid counter -/
structure GeneratorCursor (σ : Type) where
  /-- Current iteration state -/
  state : σ
  /-- Auto-incrementing rowid counter -/
  rowidCounter : Int
  /-- Whether we're at EOF -/
  atEof : Bool

/-- Internal wrapper that holds all callbacks for FFI registration.
    Uses the same state type throughout to avoid type erasure. -/
structure GeneratorModule (σ : Type) where
  schema : VTableSchema
  initFn : Array Value → IO σ
  hasMoreFn : σ → Bool
  currentFn : σ → Array Value
  advanceFn : σ → σ
  rowidFn : Option (σ → Int)

namespace GeneratorModule

/-- Get schema -/
def getSchema (mod : GeneratorModule σ) : IO VTableSchema := pure mod.schema

/-- Default bestIndex: full table scan -/
def bestIndex (_ : GeneratorModule σ) (_ : VTableIndexInfo) : IO VTableIndexOutput := do
  pure {
    constraintUsage := #[]
    idxNum := 0
    estimatedCost := 1000.0
    estimatedRows := 1000
  }

/-- Open a cursor with filter arguments -/
def openCursor (mod : GeneratorModule σ) (_ : Int) (args : Array Value) : IO (GeneratorCursor σ) := do
  let state ← mod.initFn args
  let atEof := !mod.hasMoreFn state
  pure { state := state, rowidCounter := 1, atEof := atEof }

/-- Check if cursor is at EOF -/
def cursorEof (cursor : GeneratorCursor σ) : IO Bool := do
  pure cursor.atEof

/-- Advance cursor to next row -/
def cursorNext (mod : GeneratorModule σ) (cursor : GeneratorCursor σ) : IO (GeneratorCursor σ) := do
  let newState := mod.advanceFn cursor.state
  let atEof := !mod.hasMoreFn newState
  pure { state := newState, rowidCounter := cursor.rowidCounter + 1, atEof := atEof }

/-- Get column value at current row -/
def cursorColumn (mod : GeneratorModule σ) (cursor : GeneratorCursor σ) (col : Nat) : IO Value := do
  if cursor.atEof then
    pure .null
  else
    let values := mod.currentFn cursor.state
    pure (values[col]?.getD .null)

/-- Get rowid of current row -/
def cursorRowid (mod : GeneratorModule σ) (cursor : GeneratorCursor σ) : IO Int := do
  match mod.rowidFn with
  | some fn => pure (fn cursor.state)
  | none => pure cursor.rowidCounter

/-- Generators are read-only, so update always fails -/
def handleUpdate (_ : GeneratorModule σ) (_ : VTableUpdateOp) : IO (Option Int) := do
  throw (IO.userError "Generator virtual tables are read-only")

end GeneratorModule

end VirtualTable

namespace Database

/-- Register a generator-backed virtual table.
    The table is read-only and generates rows on demand.

    Example:
    ```lean
    -- Generate a series of integers
    db.registerGenerator "generate_series" {
      schema := { columns := #[
        { name := "value", sqlType := "INTEGER" },
        { name := "start", sqlType := "INTEGER", isHidden := true },
        { name := "stop", sqlType := "INTEGER", isHidden := true }
      ]}
      init := fun args => do
        let start := args[0]?.bind Value.toInt |>.getD 1
        let stop := args[1]?.bind Value.toInt |>.getD 10
        pure (start, stop, start)  -- (start, stop, current)
      hasMore := fun (_, stop, current) => current <= stop
      current := fun (_, _, current) => #[.integer current]
      advance := fun (start, stop, current) => (start, stop, current + 1)
    }
    let rows ← db.query "SELECT value FROM generate_series(1, 5)"
    ``` -/
def registerGenerator (db : Database) (name : String) (gen : VirtualTable.Generator σ)
    : IO Unit := do
  let mod : VirtualTable.GeneratorModule σ := {
    schema := gen.schema
    initFn := gen.init
    hasMoreFn := gen.hasMore
    currentFn := gen.current
    advanceFn := gen.advance
    rowidFn := gen.rowid
  }

  -- Create bound callbacks that close over the module
  let schemaFn := fun (_ : Unit) => (pure mod.schema : IO VTableSchema)
  let bestIndexFn := fun (_ : Unit) => VirtualTable.GeneratorModule.bestIndex mod
  let openFn := fun (_ : Unit) => VirtualTable.GeneratorModule.openCursor mod
  let eofFn := VirtualTable.GeneratorModule.cursorEof (σ := σ)
  let nextFn := fun (cursor : VirtualTable.GeneratorCursor σ) => VirtualTable.GeneratorModule.cursorNext mod cursor
  let columnFn := fun (cursor : VirtualTable.GeneratorCursor σ) => VirtualTable.GeneratorModule.cursorColumn mod cursor
  let rowidFn := fun (cursor : VirtualTable.GeneratorCursor σ) => VirtualTable.GeneratorModule.cursorRowid mod cursor
  let updateFn := fun (_ : Unit) => VirtualTable.GeneratorModule.handleUpdate mod

  -- Register the virtual table module
  FFI.dbCreateVTableModule (σ := VirtualTable.GeneratorCursor σ)
    db.handle
    name
    ()  -- tableData not used, callbacks close over mod
    schemaFn
    bestIndexFn
    openFn
    eofFn
    nextFn
    columnFn
    rowidFn
    updateFn

  -- Create the virtual table instance (execRaw needed for VIRTUAL TABLE syntax)
  db.execRaw s!"CREATE VIRTUAL TABLE {name} USING {name}"

end Database

end Quarry
