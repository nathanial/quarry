/-
  Row, Value Extraction, and Utility Tests
-/
import Quarry
import Crucible

open Crucible
open Quarry

namespace Tests.Extract

testSuite "Value Extraction"

test "extract int" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x INTEGER)"
  db.exec "INSERT INTO t VALUES (42)"
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.getAs (α := Int) 0 with
    | .ok n => n ≡ 42
    | .error e => throw (IO.userError s!"extraction failed: {e}")
  | none => throw (IO.userError "no row")

test "extract string by name" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (name TEXT)"
  db.exec "INSERT INTO t VALUES ('Alice')"
  let rows ← db.query "SELECT name FROM t"
  match rows[0]? with
  | some row =>
    match row.getByNameAs (α := String) "name" with
    | .ok s => s ≡ "Alice"
    | .error e => throw (IO.userError s!"extraction failed: {e}")
  | none => throw (IO.userError "no row")

test "extract option for null" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x INTEGER)"
  db.exec "INSERT INTO t VALUES (NULL)"
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.getAs (α := Option Int) 0 with
    | .ok none => ensure true "null extracted as none"
    | .ok (some _) => throw (IO.userError "expected none")
    | .error e => throw (IO.userError s!"extraction failed: {e}")
  | none => throw (IO.userError "no row")

#generate_tests

end Tests.Extract

namespace Tests.Row

testSuite "Row Access"

test "column names" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)"
  db.exec "INSERT INTO users VALUES (1, 'Alice', 30)"
  let rows ← db.query "SELECT id, name, age FROM users"
  match rows[0]? with
  | some row =>
    let names := row.columnNames
    ensure (names.size == 3) "3 columns"
    ensure (names[0]? == some "id") "first column is id"
    ensure (names[1]? == some "name") "second column is name"
    ensure (names[2]? == some "age") "third column is age"
  | none => throw (IO.userError "no row")

test "access by name case insensitive" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (MyColumn TEXT)"
  db.exec "INSERT INTO t VALUES ('value')"
  let rows ← db.query "SELECT MyColumn FROM t"
  match rows[0]? with
  | some row =>
    match row.getByName? "mycolumn" with
    | some (Value.text "value") => ensure true "case insensitive access works"
    | _ => throw (IO.userError "column not found")
  | none => throw (IO.userError "no row")

#generate_tests

end Tests.Row

namespace Tests.ValueUtils

testSuite "Value Utilities"

test "asInt? on integer" := do
  let v := Value.integer 42
  match v.asInt? with
  | some 42 => ensure true "asInt? works"
  | _ => throw (IO.userError "expected 42")

test "asFloat? on real" := do
  let v := Value.real 3.14
  match v.asFloat? with
  | some f => ensure ((f - 3.14).abs < 0.01) "asFloat? works"
  | none => throw (IO.userError "expected float")

test "asFloat? coerces integer" := do
  let v := Value.integer 5
  match v.asFloat? with
  | some f => ensure ((f - 5.0).abs < 0.01) "coercion works"
  | none => throw (IO.userError "expected float")

test "asString? on text" := do
  let v := Value.text "hello"
  match v.asString? with
  | some "hello" => ensure true "asString? works"
  | _ => throw (IO.userError "expected hello")

test "isNull" := do
  ensure Value.null.isNull "null is null"
  ensure (!(Value.integer 0).isNull) "integer is not null"

test "BEq Value" := do
  ensure (Value.integer 42 == Value.integer 42) "integers equal"
  ensure (Value.text "a" == Value.text "a") "texts equal"
  ensure (Value.null == Value.null) "nulls equal"
  ensure (!(Value.integer 1 == Value.integer 2)) "different integers"

#generate_tests

end Tests.ValueUtils

namespace Tests.RowUtils

testSuite "Row Utilities"

test "row size" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (a INTEGER, b TEXT, c REAL)"
  db.exec "INSERT INTO t VALUES (1, 'hi', 3.14)"
  let rows ← db.query "SELECT * FROM t"
  match rows[0]? with
  | some row => row.size ≡ 3
  | none => throw (IO.userError "no row")

test "columnName by index" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE users (id INTEGER, name TEXT)"
  db.exec "INSERT INTO users VALUES (1, 'Alice')"
  let rows ← db.query "SELECT id, name FROM users"
  match rows[0]? with
  | some row =>
    match row.columnName 0, row.columnName 1 with
    | some "id", some "name" => ensure true "column names correct"
    | _, _ => throw (IO.userError "unexpected column names")
  | none => throw (IO.userError "no row")

test "getByNameAsOption" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x INTEGER)"
  db.exec "INSERT INTO t VALUES (NULL)"
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.getByNameAsOption (α := Int) "x" with
    | .ok none => ensure true "null as none"
    | .ok (some _) => throw (IO.userError "expected none")
    | .error e => throw (IO.userError s!"failed: {e}")
  | none => throw (IO.userError "no row")

#generate_tests

end Tests.RowUtils
