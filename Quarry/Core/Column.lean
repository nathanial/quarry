/-
  Quarry.Core.Column
  Column metadata for query results
-/

namespace Quarry

/-- Column metadata from query results -/
structure Column where
  name : String
  declType : Option String := none
  tableName : Option String := none
  deriving Repr, Inhabited, BEq

namespace Column

instance : ToString Column where
  toString c := c.name

end Column

/-- Extended column metadata with source information.
    Provides information about where a result column originated from. -/
structure ColumnMetadata where
  /-- The database containing the column (e.g., "main", "temp") -/
  database : Option String
  /-- The table containing the column -/
  table : Option String
  /-- The original column name in the source table (may differ from alias) -/
  originName : Option String
  deriving Repr, Inhabited, BEq

namespace ColumnMetadata

/-- Check if metadata is available (column comes from a real table) -/
def isAvailable (m : ColumnMetadata) : Bool :=
  m.table.isSome

/-- Get a human-readable description like "main.users.id" -/
def toString (m : ColumnMetadata) : String :=
  match m.database, m.table, m.originName with
  | some db, some tbl, some col => s!"{db}.{tbl}.{col}"
  | none, some tbl, some col => s!"{tbl}.{col}"
  | _, _, some col => col
  | _, _, none => "(expression)"

instance : ToString ColumnMetadata where
  toString := ColumnMetadata.toString

end ColumnMetadata

end Quarry
