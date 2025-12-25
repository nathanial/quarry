/-
  Quarry.Function
  Type-safe wrappers for user-defined SQL functions
-/
import Quarry.Database
import Quarry.Core.Value

namespace Quarry

/-- Typeclass for types that can be UDF arguments -/
class UdfArg (α : Type) where
  fromValue : Value → Option α

instance : UdfArg Int where
  fromValue
    | .integer n => some n
    | _ => none

instance : UdfArg Float where
  fromValue
    | .real f => some f
    | .integer n => some (Float.ofInt n)
    | _ => none

instance : UdfArg String where
  fromValue
    | .text s => some s
    | _ => none

instance : UdfArg Bool where
  fromValue
    | .integer 0 => some false
    | .integer _ => some true
    | _ => none

instance : UdfArg ByteArray where
  fromValue
    | .blob b => some b
    | _ => none

instance [UdfArg α] : UdfArg (Option α) where
  fromValue
    | .null => some none
    | v => UdfArg.fromValue v |>.map some

instance : UdfArg Value where
  fromValue v := some v

/-- Typeclass for types that can be UDF results -/
class UdfResult (α : Type) where
  toValue : α → Value

instance : UdfResult Int where
  toValue := Value.integer

instance : UdfResult Float where
  toValue := Value.real

instance : UdfResult String where
  toValue := Value.text

instance : UdfResult Bool where
  toValue b := Value.integer (if b then 1 else 0)

instance : UdfResult ByteArray where
  toValue := Value.blob

instance : UdfResult Value where
  toValue := id

instance [UdfResult α] : UdfResult (Option α) where
  toValue
    | none => Value.null
    | some x => UdfResult.toValue x

/-- Create a 1-argument function with type safety -/
def Database.createFunction1 [UdfArg α] [UdfResult β]
    (db : Database) (name : String) (f : α → β) : IO Unit :=
  db.createScalarFunction name 1 fun args => do
    match args[0]? >>= UdfArg.fromValue with
    | some a => return UdfResult.toValue (f a)
    | none => return Value.null

/-- Create a 2-argument function with type safety -/
def Database.createFunction2 [UdfArg α] [UdfArg β] [UdfResult γ]
    (db : Database) (name : String) (f : α → β → γ) : IO Unit :=
  db.createScalarFunction name 2 fun args => do
    match args[0]? >>= UdfArg.fromValue, args[1]? >>= UdfArg.fromValue with
    | some a, some b => return UdfResult.toValue (f a b)
    | _, _ => return Value.null

/-- Create a 3-argument function with type safety -/
def Database.createFunction3 [UdfArg α] [UdfArg β] [UdfArg γ] [UdfResult δ]
    (db : Database) (name : String) (f : α → β → γ → δ) : IO Unit :=
  db.createScalarFunction name 3 fun args => do
    match args[0]? >>= UdfArg.fromValue,
          args[1]? >>= UdfArg.fromValue,
          args[2]? >>= UdfArg.fromValue with
    | some a, some b, some c => return UdfResult.toValue (f a b c)
    | _, _, _ => return Value.null

/-- Create an IO-returning 1-argument function -/
def Database.createIOFunction1 [UdfArg α] [UdfResult β]
    (db : Database) (name : String) (f : α → IO β) : IO Unit :=
  db.createScalarFunction name 1 fun args => do
    match args[0]? >>= UdfArg.fromValue with
    | some a => return UdfResult.toValue (← f a)
    | none => return Value.null

/-- Create an IO-returning 2-argument function -/
def Database.createIOFunction2 [UdfArg α] [UdfArg β] [UdfResult γ]
    (db : Database) (name : String) (f : α → β → IO γ) : IO Unit :=
  db.createScalarFunction name 2 fun args => do
    match args[0]? >>= UdfArg.fromValue, args[1]? >>= UdfArg.fromValue with
    | some a, some b => return UdfResult.toValue (← f a b)
    | _, _ => return Value.null

end Quarry
