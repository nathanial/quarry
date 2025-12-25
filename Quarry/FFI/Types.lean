/-
  Quarry.FFI.Types
  Opaque FFI handle types
-/

namespace Quarry.FFI

/-- Opaque handle to sqlite3 database connection -/
opaque DatabasePointed : NonemptyType
def Database := DatabasePointed.type
instance : Nonempty Database := DatabasePointed.property

/-- Opaque handle to sqlite3_stmt prepared statement -/
opaque StatementPointed : NonemptyType
def Statement := StatementPointed.type
instance : Nonempty Statement := StatementPointed.property

end Quarry.FFI
