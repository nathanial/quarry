/-
  Quarry.VirtualTable.Array
  Array-backed virtual table implementation
-/
import Quarry.VirtualTable
import Quarry.FFI.VirtualTable
import Quarry.Database

namespace Quarry.VirtualTable

/-- Row stored in array table with its rowid -/
structure ArrayRow where
  rowid : Int
  values : Array Value
  deriving Repr, Inhabited

/-- Array-backed virtual table data -/
structure ArrayTableData where
  /-- Schema for this table -/
  schema : VTableSchema
  /-- Rows stored in the table -/
  rows : IO.Ref (Array ArrayRow)
  /-- Next rowid to assign -/
  nextRowid : IO.Ref Int

/-- Cursor state for iterating over array table -/
structure ArrayCursor where
  /-- Reference to the table data -/
  table : ArrayTableData
  /-- Current position in iteration -/
  idx : Nat
  /-- Snapshot of rows at time of open (for consistent reads) -/
  snapshot : Array ArrayRow

namespace ArrayTableData

/-- Get schema from table data -/
def getSchema (data : ArrayTableData) : IO VTableSchema := pure data.schema

/-- Default bestIndex: full table scan -/
def bestIndex (_ : ArrayTableData) (_ : VTableIndexInfo) : IO VTableIndexOutput := do
  pure {
    constraintUsage := #[]
    idxNum := 0
    estimatedCost := 1000000.0
    estimatedRows := 1000000
  }

/-- Open a cursor -/
def openCursor (data : ArrayTableData) (_ : Int) (_ : Array Value) : IO ArrayCursor := do
  let rows ← data.rows.get
  pure { table := data, idx := 0, snapshot := rows }

/-- Check if cursor is at EOF -/
def cursorEof (cursor : ArrayCursor) : IO Bool := do
  pure (cursor.idx >= cursor.snapshot.size)

/-- Advance cursor to next row -/
def cursorNext (cursor : ArrayCursor) : IO ArrayCursor := do
  pure { cursor with idx := cursor.idx + 1 }

/-- Get column value at current row -/
def cursorColumn (cursor : ArrayCursor) (col : Nat) : IO Value := do
  match cursor.snapshot[cursor.idx]? with
  | some row => pure (row.values[col]?.getD .null)
  | none => pure .null

/-- Get rowid of current row -/
def cursorRowid (cursor : ArrayCursor) : IO Int := do
  match cursor.snapshot[cursor.idx]? with
  | some row => pure row.rowid
  | none => pure 0

/-- Handle updates (INSERT/UPDATE/DELETE) -/
def handleUpdate (data : ArrayTableData) (op : VTableUpdateOp) : IO (Option Int) := do
  match op with
  | .delete rowid => do
    data.rows.modify fun rows =>
      rows.filter fun r => r.rowid != rowid
    pure none

  | .insert rowidOpt values => do
    let rowid ← match rowidOpt with
      | some r => pure r
      | none => do
        let r ← data.nextRowid.get
        data.nextRowid.set (r + 1)
        pure r
    data.rows.modify fun rows =>
      rows.push { rowid := rowid, values := values }
    pure (some rowid)

  | .update oldRowid newRowid values => do
    data.rows.modify fun rows =>
      rows.map fun r =>
        if r.rowid == oldRowid then
          { rowid := newRowid, values := values }
        else r
    pure none

/-- Insert a row programmatically (returns new rowid) -/
def insert (data : ArrayTableData) (values : Array Value) : IO Int := do
  let rowid ← data.nextRowid.get
  data.nextRowid.set (rowid + 1)
  data.rows.modify fun rows => rows.push { rowid := rowid, values := values }
  pure rowid

/-- Delete a row by rowid -/
def delete (data : ArrayTableData) (rowid : Int) : IO Bool := do
  let rows ← data.rows.get
  let newRows := rows.filter fun r => r.rowid != rowid
  data.rows.set newRows
  pure (newRows.size < rows.size)

/-- Get all rows -/
def allRows (data : ArrayTableData) : IO (Array ArrayRow) := data.rows.get

/-- Clear all rows -/
def clear (data : ArrayTableData) : IO Unit := data.rows.set #[]

/-- Get row count -/
def size (data : ArrayTableData) : IO Nat := do
  let rows ← data.rows.get
  pure rows.size

end ArrayTableData

end VirtualTable

namespace Database

/-- Create an array-backed virtual table.
    The table is writable and supports INSERT/UPDATE/DELETE via SQL.

    Example:
    ```lean
    let vtab ← db.createArrayVTable "users" {
      columns := #[
        { name := "id", sqlType := "INTEGER" },
        { name := "name", sqlType := "TEXT" }
      ]
    }
    vtab.insert #[.integer 1, .text "Alice"]
    db.execSqlInsert "INSERT INTO users VALUES (2, 'Bob')"
    let rows ← db.query "SELECT * FROM users"
    ``` -/
def createArrayVTable (db : Database) (name : String) (schema : VTableSchema)
    : IO VirtualTable.ArrayTableData := do
  let rows ← IO.mkRef #[]
  let nextRowid ← IO.mkRef 1
  let data : VirtualTable.ArrayTableData := { schema := schema, rows := rows, nextRowid := nextRowid }

  -- Register the virtual table module
  FFI.dbCreateVTableModule (σ := VirtualTable.ArrayCursor)
    db.handle
    name
    data
    VirtualTable.ArrayTableData.getSchema
    VirtualTable.ArrayTableData.bestIndex
    VirtualTable.ArrayTableData.openCursor
    VirtualTable.ArrayTableData.cursorEof
    VirtualTable.ArrayTableData.cursorNext
    VirtualTable.ArrayTableData.cursorColumn
    VirtualTable.ArrayTableData.cursorRowid
    VirtualTable.ArrayTableData.handleUpdate

  -- Create the virtual table instance (execRaw needed for VIRTUAL TABLE syntax)
  db.execRaw s!"CREATE VIRTUAL TABLE {name} USING {name}"

  pure data

end Database

end Quarry
