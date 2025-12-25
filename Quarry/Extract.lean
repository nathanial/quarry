/-
  Quarry.Extract
  Result extraction typeclass for converting SQLite values to Lean types
-/
import Quarry.Core.Value
import Quarry.Core.Error
import Quarry.Core.Row

namespace Quarry

/-- Typeclass for extracting values from SQLite results -/
class FromSql (α : Type) where
  fromSql : Value -> SqlResult α

instance : FromSql Int where
  fromSql
    | .integer n => .ok n
    | .real f => .ok (Int.ofNat f.toUInt64.toNat)
    | .text s => match s.toInt? with
      | some n => .ok n
      | none => .error (.typeError "Int" "text")
    | v => .error (.typeError "Int" (toString v))

instance : FromSql Nat where
  fromSql v := do
    let n : Int ← FromSql.fromSql v
    if n >= 0 then .ok n.toNat
    else .error (.typeError "Nat" "negative Int")

instance : FromSql Float where
  fromSql
    | .real f => .ok f
    | .integer n => .ok (Float.ofInt n)
    | v => .error (.typeError "Float" (toString v))

instance : FromSql String where
  fromSql
    | .text s => .ok s
    | .integer n => .ok (toString n)
    | .real f => .ok (toString f)
    | .null => .ok ""
    | v => .error (.typeError "String" (toString v))

instance : FromSql ByteArray where
  fromSql
    | .blob b => .ok b
    | .text s => .ok s.toUTF8
    | v => .error (.typeError "ByteArray" (toString v))

instance : FromSql Bool where
  fromSql
    | .integer 0 => .ok false
    | .integer _ => .ok true
    | .null => .ok false
    | v => .error (.typeError "Bool" (toString v))

instance [FromSql α] : FromSql (Option α) where
  fromSql
    | .null => .ok none
    | v => match FromSql.fromSql v with
      | .ok x => .ok (some x)
      | .error e => .error e

instance : FromSql Value where
  fromSql v := .ok v

/-- Extract a column value by index with type conversion -/
def Row.getAs [FromSql α] (row : Row) (idx : Nat) : SqlResult α := do
  match row.get? idx with
  | some v => FromSql.fromSql v
  | none => .error (.columnNotFound s!"index {idx}")

/-- Extract a column value by name with type conversion -/
def Row.getByNameAs [FromSql α] (row : Row) (name : String) : SqlResult α := do
  match row.getByName? name with
  | some v => FromSql.fromSql v
  | none => .error (.columnNotFound name)

/-- Extract a column value by index, returning Option if null -/
def Row.getAsOption [FromSql α] (row : Row) (idx : Nat) : SqlResult (Option α) :=
  match row.get? idx with
  | some .null => .ok none
  | some v => match FromSql.fromSql v with
    | .ok x => .ok (some x)
    | .error e => .error e
  | none => .error (.columnNotFound s!"index {idx}")

/-- Extract a column value by name, returning Option if null -/
def Row.getByNameAsOption [FromSql α] (row : Row) (name : String) : SqlResult (Option α) :=
  match row.getByName? name with
  | some .null => .ok none
  | some v => match FromSql.fromSql v with
    | .ok x => .ok (some x)
    | .error e => .error e
  | none => .error (.columnNotFound name)

end Quarry
