/-
  Quarry.Bind
  Parameter binding support (positional and named)
-/
import Quarry.Core.Value
import Quarry.Core.Error
import Quarry.FFI.Statement

namespace Quarry

/-- Typeclass for values that can be bound to SQL parameters -/
class ToSql (α : Type) where
  toSql : α -> Value

instance : ToSql Int where
  toSql n := Value.integer n

instance : ToSql Nat where
  toSql n := Value.integer (Int.ofNat n)

instance : ToSql Float where
  toSql f := Value.real f

instance : ToSql String where
  toSql s := Value.text s

instance : ToSql ByteArray where
  toSql b := Value.blob b

instance : ToSql Bool where
  toSql b := Value.integer (if b then 1 else 0)

instance [ToSql α] : ToSql (Option α) where
  toSql
    | some x => ToSql.toSql x
    | none => Value.null

instance : ToSql Value where
  toSql v := v

/-- Bind a value to a positional parameter (1-indexed) -/
def bindValue (stmt : FFI.Statement) (idx : UInt32) (value : Value) : IO Unit := do
  match value with
  | .null => FFI.stmtBindNull stmt idx
  | .integer n => FFI.stmtBindInt stmt idx n
  | .real f => FFI.stmtBindDouble stmt idx f
  | .text s => FFI.stmtBindText stmt idx s
  | .blob b => FFI.stmtBindBlob stmt idx b

/-- Bind a typed value to a positional parameter -/
def bind [ToSql α] (stmt : FFI.Statement) (idx : UInt32) (value : α) : IO Unit :=
  bindValue stmt idx (ToSql.toSql value)

/-- Bind a value to a named parameter (:name, @name, $name) -/
def bindNamed (stmt : FFI.Statement) (name : String) (value : Value) : IO Unit := do
  let idx ← FFI.stmtBindParameterIndex stmt name
  if idx <= 0 then
    throw (IO.userError s!"Parameter not found: {name}")
  bindValue stmt idx.toNat.toUInt32 value

/-- Bind a typed value to a named parameter -/
def bindNamedTyped [ToSql α] (stmt : FFI.Statement) (name : String) (value : α) : IO Unit :=
  bindNamed stmt name (ToSql.toSql value)

/-- Bind an array of positional parameters -/
def bindAll (stmt : FFI.Statement) (values : Array Value) : IO Unit := do
  for h : i in [:values.size] do
    bindValue stmt (i.toUInt32 + 1) values[i]

/-- Bind named parameters from an association list -/
def bindAllNamed (stmt : FFI.Statement) (params : List (String × Value)) : IO Unit := do
  for (name, value) in params do
    bindNamed stmt name value

/-- Reset a statement for re-execution with new parameters -/
def resetStmt (stmt : FFI.Statement) : IO Unit :=
  FFI.stmtReset stmt

/-- Clear all parameter bindings -/
def clearBindings (stmt : FFI.Statement) : IO Unit :=
  FFI.stmtClearBindings stmt

/-- Get the number of parameters in a statement -/
def parameterCount (stmt : FFI.Statement) : IO UInt32 :=
  FFI.stmtBindParameterCount stmt

end Quarry
