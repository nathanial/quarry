/-
  Quarry.Chisel.Convert
  Type conversions between Chisel and Quarry
-/
import Chisel
import Quarry.Core.Value
import Quarry.Bind

namespace Quarry.Chisel

/-- Convert Chisel Literal to Quarry Value -/
def literalToValue : Chisel.Literal → Quarry.Value
  | .null => .null
  | .bool true => .integer 1
  | .bool false => .integer 0
  | .int n => .integer n
  | .float f => .real f
  | .string s => .text s
  | .blob b => .blob b

/-- Convert Quarry Value to Chisel Literal -/
def valueToLiteral : Quarry.Value → Chisel.Literal
  | .null => .null
  | .integer n => .int n
  | .real f => .float f
  | .text s => .string s
  | .blob b => .blob b

/-- Convert array of Chisel Literals to Quarry Values -/
def literalsToValues (lits : Array Chisel.Literal) : Array Quarry.Value :=
  lits.map literalToValue

/-- Convert list of Chisel Literals to Quarry Values -/
def literalListToValues (lits : List Chisel.Literal) : List Quarry.Value :=
  lits.map literalToValue

/-- ToSql instance for Chisel.Literal -/
instance : ToSql Chisel.Literal where
  toSql := literalToValue

end Quarry.Chisel
