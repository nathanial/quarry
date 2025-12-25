/-
  Quarry.Core.Value
  SQLite value types mapping to Lean types
-/

namespace Quarry

/-- SQLite value types -/
inductive Value where
  | null
  | integer (v : Int)
  | real (v : Float)
  | text (v : String)
  | blob (v : ByteArray)
  deriving Inhabited

instance : Repr Value where
  reprPrec v _ := match v with
    | .null => "Value.null"
    | .integer n => s!"Value.integer {repr n}"
    | .real f => s!"Value.real {repr f}"
    | .text s => s!"Value.text {repr s}"
    | .blob b => s!"Value.blob <{b.size} bytes>"

namespace Value

instance : BEq Value where
  beq a b := match a, b with
    | .null, .null => true
    | .integer x, .integer y => x == y
    | .real x, .real y => x == y || (x.isNaN && y.isNaN)
    | .text x, .text y => x == y
    | .blob x, .blob y => x == y
    | _, _ => false

instance : ToString Value where
  toString v := match v with
    | .null => "NULL"
    | .integer n => toString n
    | .real f => toString f
    | .text s => s!"\"{s}\""
    | .blob b => s!"<blob:{b.size} bytes>"

def asInt? : Value -> Option Int
  | .integer n => some n
  | _ => none

def asFloat? : Value -> Option Float
  | .real f => some f
  | .integer n => some (Float.ofInt n)
  | _ => none

def asString? : Value -> Option String
  | .text s => some s
  | _ => none

def asBlob? : Value -> Option ByteArray
  | .blob b => some b
  | _ => none

def isNull : Value -> Bool
  | .null => true
  | _ => false

/-- SQLite type code constants -/
def sqliteTypeNull : Int := 5
def sqliteTypeInteger : Int := 1
def sqliteTypeFloat : Int := 2
def sqliteTypeText : Int := 3
def sqliteTypeBlob : Int := 4

end Value

end Quarry
