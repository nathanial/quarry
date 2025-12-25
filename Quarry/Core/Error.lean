/-
  Quarry.Core.Error
  SQLite error codes and error handling
-/

namespace Quarry

/-- SQLite result codes -/
inductive SqliteCode where
  | ok
  | error
  | internal
  | perm
  | abort
  | busy
  | locked
  | nomem
  | readonly
  | interrupt
  | ioerr
  | corrupt
  | notfound
  | full
  | cantopen
  | protocol
  | schema
  | toobig
  | constraint
  | mismatch
  | misuse
  | nolfs
  | auth
  | range
  | notadb
  | row
  | done
  | unknown (code : Int)
  deriving Repr, BEq, Inhabited

namespace SqliteCode

def fromInt (n : Int) : SqliteCode :=
  match n with
  | 0 => .ok        | 1 => .error      | 2 => .internal   | 3 => .perm
  | 4 => .abort     | 5 => .busy       | 6 => .locked     | 7 => .nomem
  | 8 => .readonly  | 9 => .interrupt  | 10 => .ioerr     | 11 => .corrupt
  | 12 => .notfound | 13 => .full      | 14 => .cantopen  | 15 => .protocol
  | 17 => .schema   | 18 => .toobig    | 19 => .constraint | 20 => .mismatch
  | 21 => .misuse   | 22 => .nolfs     | 23 => .auth      | 25 => .range
  | 26 => .notadb   | 100 => .row      | 101 => .done
  | n => .unknown n

def toInt : SqliteCode -> Int
  | .ok => 0        | .error => 1      | .internal => 2   | .perm => 3
  | .abort => 4     | .busy => 5       | .locked => 6     | .nomem => 7
  | .readonly => 8  | .interrupt => 9  | .ioerr => 10     | .corrupt => 11
  | .notfound => 12 | .full => 13      | .cantopen => 14  | .protocol => 15
  | .schema => 17   | .toobig => 18    | .constraint => 19 | .mismatch => 20
  | .misuse => 21   | .nolfs => 22     | .auth => 23      | .range => 25
  | .notadb => 26   | .row => 100      | .done => 101
  | .unknown n => n

def toString : SqliteCode -> String
  | .ok => "OK"
  | .error => "SQL error or missing database"
  | .internal => "Internal logic error"
  | .perm => "Access permission denied"
  | .abort => "Callback routine requested abort"
  | .busy => "Database is locked"
  | .locked => "Table is locked"
  | .nomem => "Out of memory"
  | .readonly => "Database is read-only"
  | .interrupt => "Operation interrupted"
  | .ioerr => "I/O error"
  | .corrupt => "Database is corrupt"
  | .notfound => "Not found"
  | .full => "Database is full"
  | .cantopen => "Unable to open database file"
  | .protocol => "Database lock protocol error"
  | .schema => "Database schema changed"
  | .toobig => "String or blob exceeds size limit"
  | .constraint => "Constraint violation"
  | .mismatch => "Data type mismatch"
  | .misuse => "Library used incorrectly"
  | .nolfs => "Large file support unavailable"
  | .auth => "Authorization denied"
  | .range => "Parameter index out of range"
  | .notadb => "Not a database file"
  | .row => "Row ready"
  | .done => "Execution finished"
  | .unknown code => s!"Unknown error ({code})"

instance : ToString SqliteCode := ⟨toString⟩

end SqliteCode

/-- Quarry error types -/
inductive SqlError where
  | sqliteError (code : SqliteCode) (message : String)
  | bindError (message : String)
  | typeError (expected : String) (actual : String)
  | nullValue (column : String)
  | columnNotFound (name : String)
  | connectionClosed
  | statementFinalized
  deriving Repr

namespace SqlError

def toString : SqlError -> String
  | .sqliteError code msg => s!"SQLite error ({code}): {msg}"
  | .bindError msg => s!"Bind error: {msg}"
  | .typeError expected actual => s!"Type error: expected {expected}, got {actual}"
  | .nullValue col => s!"Null value in column: {col}"
  | .columnNotFound name => s!"Column not found: {name}"
  | .connectionClosed => "Database connection is closed"
  | .statementFinalized => "Statement has been finalized"

instance : ToString SqlError := ⟨toString⟩

end SqlError

/-- Result type for Quarry operations -/
abbrev SqlResult (α : Type) := Except SqlError α

end Quarry
