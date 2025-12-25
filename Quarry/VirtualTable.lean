/-
  Quarry.VirtualTable
  SQLite Virtual Table support - core types and typeclasses
-/
import Quarry.Core.Value
import Quarry.Database

namespace Quarry

/-- Column definition for virtual table schema -/
structure VTableColumn where
  /-- Column name -/
  name : String
  /-- SQL type: "INTEGER", "TEXT", "REAL", "BLOB" -/
  sqlType : String
  /-- Whether this is a hidden column (for table-valued function parameters) -/
  isHidden : Bool := false
  deriving Repr, BEq, Inhabited

/-- Virtual table schema declaration -/
structure VTableSchema where
  /-- Column definitions -/
  columns : Array VTableColumn
  deriving Repr, Inhabited

namespace VTableSchema

/-- Convert schema to CREATE TABLE SQL statement for sqlite3_declare_vtab -/
def toCreateSql (schema : VTableSchema) : String :=
  let cols := schema.columns.map fun c =>
    let hidden := if c.isHidden then " HIDDEN" else ""
    s!"{c.name} {c.sqlType}{hidden}"
  s!"CREATE TABLE x({String.intercalate ", " cols.toList})"

/-- Get number of columns -/
def size (schema : VTableSchema) : Nat := schema.columns.size

/-- Get column by index -/
def get? (schema : VTableSchema) (idx : Nat) : Option VTableColumn :=
  schema.columns[idx]?

end VTableSchema

/-- Constraint operation from WHERE clause -/
inductive VTableOp where
  | eq        -- =
  | lt        -- <
  | le        -- <=
  | gt        -- >
  | ge        -- >=
  | ne        -- != or <>
  | like      -- LIKE
  | glob      -- GLOB
  | regexp    -- REGEXP
  | match_    -- MATCH
  | isNull    -- IS NULL
  | isNotNull -- IS NOT NULL
  deriving Repr, BEq, Inhabited

namespace VTableOp

/-- Convert from SQLite SQLITE_INDEX_CONSTRAINT_* value -/
def fromSqlite (op : UInt8) : VTableOp :=
  match op with
  | 2   => .eq
  | 4   => .gt
  | 8   => .le
  | 16  => .lt
  | 32  => .ge
  | 64  => .match_
  | 65  => .like
  | 66  => .glob
  | 67  => .regexp
  | 68  => .ne
  | 69  => .isNotNull
  | 70  => .isNull
  | _   => .eq  -- Default

end VTableOp

/-- A single constraint from the WHERE clause -/
structure VTableConstraint where
  /-- Column index this constraint applies to -/
  column : Nat
  /-- The operation -/
  op : VTableOp
  /-- Whether SQLite considers this constraint usable -/
  usable : Bool
  deriving Repr, BEq, Inhabited

/-- Order by specification -/
structure VTableOrderBy where
  /-- Column index to order by -/
  column : Nat
  /-- True if descending order -/
  desc : Bool
  deriving Repr, BEq, Inhabited

/-- Query planning input from SQLite (xBestIndex) -/
structure VTableIndexInfo where
  /-- Constraints from WHERE clause -/
  constraints : Array VTableConstraint
  /-- ORDER BY columns -/
  orderBy : Array VTableOrderBy
  deriving Repr, Inhabited

/-- Query planning output (returned from xBestIndex) -/
structure VTableIndexOutput where
  /-- Which constraints will be handled by xFilter (indices into constraints array) -/
  constraintUsage : Array (Option Nat)  -- argvIndex for each constraint (1-based, none if not used)
  /-- Index number passed to xFilter -/
  idxNum : Int := 0
  /-- Optional index string passed to xFilter -/
  idxStr : Option String := none
  /-- True if output is already in ORDER BY order -/
  orderByConsumed : Bool := false
  /-- Estimated cost (lower is better) -/
  estimatedCost : Float := 1000000.0
  /-- Estimated number of rows -/
  estimatedRows : Nat := 1000000
  deriving Repr, Inhabited

/-- Update operation type for xUpdate callback -/
inductive VTableUpdateOp where
  /-- DELETE: rowid to delete -/
  | delete (rowid : Int)
  /-- INSERT: optional rowid (none = auto-assign), column values -/
  | insert (rowid : Option Int) (values : Array Value)
  /-- UPDATE: old rowid, new rowid, column values -/
  | update (oldRowid : Int) (newRowid : Int) (values : Array Value)
  deriving Repr, Inhabited

end Quarry
