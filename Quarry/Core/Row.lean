/-
  Quarry.Core.Row
  Row representation for query results
-/
import Quarry.Core.Value
import Quarry.Core.Column

namespace Quarry

/-- A row of query results with column access by index or name -/
structure Row where
  values : Array Value
  columns : Array Column
  deriving Repr

namespace Row

/-- Get value by column index -/
def get? (row : Row) (idx : Nat) : Option Value :=
  row.values[idx]?

/-- Get value by column name (case-insensitive) -/
def getByName? (row : Row) (name : String) : Option Value :=
  let nameLower := name.toLower
  match row.columns.findIdx? (fun c => c.name.toLower == nameLower) with
  | some idx => row.values[idx]?
  | none => none

/-- Number of columns -/
def size (row : Row) : Nat := row.values.size

/-- Get column name by index -/
def columnName (row : Row) (idx : Nat) : Option String :=
  row.columns[idx]? |>.map (·.name)

/-- Get all column names -/
def columnNames (row : Row) : Array String :=
  row.columns.map (·.name)

instance : ToString Row where
  toString row :=
    let pairs := row.columns.zip row.values |>.map fun (c, v) => s!"{c.name}={v}"
    s!"Row({String.intercalate ", " pairs.toList})"

end Row

end Quarry
