/-
  Parameter Binding and Type Conversion Tests
-/
import Quarry
import Crucible

open Crucible
open Quarry

namespace Tests.Binding

testSuite "Parameter Binding"

test "positional binding integer" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  let stmt ← db.prepare "INSERT INTO t VALUES (?1)"
  bind stmt 1 (42 : Int)
  let _ ← FFI.stmtStep stmt
  let rows ← db.query "SELECT x FROM t"
  rows.size ≡ 1

test "positional binding multiple" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (a INTEGER, b TEXT, c REAL)"
  let stmt ← db.prepare "INSERT INTO t VALUES (?1, ?2, ?3)"
  bindAll stmt #[Value.integer 1, Value.text "hello", Value.real 3.14]
  let _ ← FFI.stmtStep stmt
  let rows ← db.query "SELECT * FROM t"
  rows.size ≡ 1

test "named binding colon style" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  let stmt ← db.prepare "INSERT INTO t VALUES (:val)"
  bindNamed stmt ":val" (Value.integer 99)
  let _ ← FFI.stmtStep stmt
  let rows ← db.query "SELECT x FROM t"
  rows.size ≡ 1

test "named binding at style" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x TEXT)"
  let stmt ← db.prepare "INSERT INTO t VALUES (@msg)"
  bindNamed stmt "@msg" (Value.text "test")
  let _ ← FFI.stmtStep stmt
  let rows ← db.query "SELECT x FROM t"
  rows.size ≡ 1

test "bindAllNamed" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (a INTEGER, b TEXT)"
  let stmt ← db.prepare "INSERT INTO t VALUES (:a, :b)"
  bindAllNamed stmt [
    (":a", Value.integer 42),
    (":b", Value.text "hello")
  ]
  let _ ← FFI.stmtStep stmt
  let rows ← db.query "SELECT * FROM t"
  rows.size ≡ 1

test "reset and rebind" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  let stmt ← db.prepare "INSERT INTO t VALUES (?1)"
  bind stmt 1 (1 : Int)
  let _ ← FFI.stmtStep stmt
  resetStmt stmt
  clearBindings stmt
  bind stmt 1 (2 : Int)
  let _ ← FFI.stmtStep stmt
  let rows ← db.query "SELECT * FROM t"
  rows.size ≡ 2

test "parameter count" := do
  let db ← Database.openMemory
  let stmt ← db.prepare "SELECT ?1, ?2, ?3"
  let count ← parameterCount stmt
  count ≡ 3

test "bind null value" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  let stmt ← db.prepare "INSERT INTO t VALUES (?1)"
  bindValue stmt 1 Value.null
  let _ ← FFI.stmtStep stmt
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.get? 0 with
    | some Value.null => ensure true "null bound"
    | _ => throw (IO.userError "expected null")
  | none => throw (IO.userError "no row")

end Tests.Binding

namespace Tests.TypeConversion

testSuite "Type Conversion"

test "FromSql Nat" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES (42)"
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.getAs (α := Nat) 0 with
    | .ok n => n ≡ 42
    | .error e => throw (IO.userError s!"failed: {e}")
  | none => throw (IO.userError "no row")

test "FromSql Float" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x REAL)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES (3.14)"
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.getAs (α := Float) 0 with
    | .ok f => ensure ((f - 3.14).abs < 0.01) "float matches"
    | .error e => throw (IO.userError s!"failed: {e}")
  | none => throw (IO.userError "no row")

test "FromSql Bool true" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES (1)"
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.getAs (α := Bool) 0 with
    | .ok b => b ≡ true
    | .error e => throw (IO.userError s!"failed: {e}")
  | none => throw (IO.userError "no row")

test "FromSql Bool false" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES (0)"
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.getAs (α := Bool) 0 with
    | .ok b => b ≡ false
    | .error e => throw (IO.userError s!"failed: {e}")
  | none => throw (IO.userError "no row")

test "FromSql ByteArray" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x BLOB)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES (X'DEADBEEF')"
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.getAs (α := ByteArray) 0 with
    | .ok b => b.size ≡ 4
    | .error e => throw (IO.userError s!"failed: {e}")
  | none => throw (IO.userError "no row")

test "FromSql Value passthrough" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES (42)"
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.getAs (α := Value) 0 with
    | .ok (Value.integer 42) => ensure true "passthrough works"
    | .ok v => throw (IO.userError s!"unexpected: {v}")
    | .error e => throw (IO.userError s!"failed: {e}")
  | none => throw (IO.userError "no row")

test "ToSql Nat via binding" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  let stmt ← db.prepare "INSERT INTO t VALUES (?1)"
  bind stmt 1 (100 : Nat)
  let _ ← FFI.stmtStep stmt
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.get? 0 with
    | some (Value.integer 100) => ensure true "Nat bound"
    | _ => throw (IO.userError "expected 100")
  | none => throw (IO.userError "no row")

test "ToSql Bool via binding" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  let stmt ← db.prepare "INSERT INTO t VALUES (?1)"
  bind stmt 1 true
  let _ ← FFI.stmtStep stmt
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.get? 0 with
    | some (Value.integer 1) => ensure true "Bool bound as 1"
    | _ => throw (IO.userError "expected 1")
  | none => throw (IO.userError "no row")

end Tests.TypeConversion
