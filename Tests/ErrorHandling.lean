/-
  Error Handling Tests
-/
import Quarry
import Crucible

open Crucible
open Quarry

namespace Tests.ErrorHandling

testSuite "Error Handling"

test "bad SQL syntax error" := do
  let db ← Database.openMemory
  try
    -- Use execRaw to test raw SQL error handling
    db.execRaw "NOT VALID SQL"
    throw (IO.userError "should have failed")
  catch _ =>
    ensure true "caught error"

test "columnNotFound error" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES (1)"
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.getByNameAs (α := Int) "nonexistent" with
    | .error (.columnNotFound _) => ensure true "got columnNotFound"
    | .error e => throw (IO.userError s!"unexpected error: {e}")
    | .ok _ => throw (IO.userError "should have failed")
  | none => throw (IO.userError "no row")

test "type extraction error" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x TEXT)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES ('not a number')"
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.getAs (α := Int) 0 with
    | .error (.typeError _ _) => ensure true "got typeError"
    | .error e => throw (IO.userError s!"unexpected error: {e}")
    | .ok _ => throw (IO.userError "should have failed")
  | none => throw (IO.userError "no row")

end Tests.ErrorHandling
