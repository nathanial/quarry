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

end Quarry
