/-
  Quarry Test Suite
-/
import Quarry
import Crucible

open Crucible
open Quarry

namespace Tests.Database

testSuite "Database Operations"

test "open in-memory database" := do
  let _db ← Database.openMemory
  ensure true "database opened"

test "create table" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"
  ensure true "table created"

test "insert and query" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
  db.exec "INSERT INTO users (name) VALUES ('Alice')"
  db.exec "INSERT INTO users (name) VALUES ('Bob')"
  let rows ← db.query "SELECT * FROM users"
  rows.size ≡ 2

test "last insert rowid" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE items (id INTEGER PRIMARY KEY)"
  db.exec "INSERT INTO items DEFAULT VALUES"
  let rowid ← db.lastInsertRowid
  rowid ≡ 1

test "changes count" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE items (id INTEGER)"
  db.exec "INSERT INTO items VALUES (1), (2), (3)"
  let changes ← db.changes
  changes ≡ 3

test "query one" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x INTEGER)"
  db.exec "INSERT INTO t VALUES (42)"
  let row ← db.queryOne "SELECT x FROM t"
  match row with
  | some r =>
    match r.get? 0 with
    | some (Value.integer 42) => ensure true "found 42"
    | _ => throw (IO.userError "unexpected value")
  | none => throw (IO.userError "no row found")

#generate_tests

end Tests.Database

namespace Tests.Values

testSuite "Value Types"

test "integer value" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (v INTEGER)"
  db.exec "INSERT INTO t VALUES (42)"
  let rows ← db.query "SELECT v FROM t"
  match rows[0]? with
  | some row =>
    match row.get? 0 with
    | some (Value.integer 42) => ensure true "integer matches"
    | _ => throw (IO.userError "Expected integer 42")
  | none => throw (IO.userError "no row")

test "float value" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (v REAL)"
  db.exec "INSERT INTO t VALUES (3.14)"
  let rows ← db.query "SELECT v FROM t"
  match rows[0]? with
  | some row =>
    match row.get? 0 with
    | some (Value.real f) =>
      if (f - 3.14).abs < 0.001 then ensure true "float matches"
      else throw (IO.userError s!"Expected ~3.14, got {f}")
    | _ => throw (IO.userError "Expected float")
  | none => throw (IO.userError "no row")

test "text value" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (v TEXT)"
  db.exec "INSERT INTO t VALUES ('hello')"
  let rows ← db.query "SELECT v FROM t"
  match rows[0]? with
  | some row =>
    match row.get? 0 with
    | some (Value.text "hello") => ensure true "text matches"
    | _ => throw (IO.userError "Expected text 'hello'")
  | none => throw (IO.userError "no row")

test "null value" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (v TEXT)"
  db.exec "INSERT INTO t VALUES (NULL)"
  let rows ← db.query "SELECT v FROM t"
  match rows[0]? with
  | some row =>
    match row.get? 0 with
    | some Value.null => ensure true "null matches"
    | _ => throw (IO.userError "Expected null")
  | none => throw (IO.userError "no row")

test "blob value" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (v BLOB)"
  db.exec "INSERT INTO t VALUES (X'DEADBEEF')"
  let rows ← db.query "SELECT v FROM t"
  match rows[0]? with
  | some row =>
    match row.get? 0 with
    | some (Value.blob b) =>
      if b.size == 4 then ensure true "blob matches"
      else throw (IO.userError s!"Expected 4 bytes, got {b.size}")
    | _ => throw (IO.userError "Expected blob")
  | none => throw (IO.userError "no row")

#generate_tests

end Tests.Values

namespace Tests.Transactions

testSuite "Transactions"

test "commit transaction" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (id INTEGER)"
  db.transaction do
    db.exec "INSERT INTO t VALUES (1)"
    db.exec "INSERT INTO t VALUES (2)"
  let rows ← db.query "SELECT * FROM t"
  rows.size ≡ 2

test "rollback on error" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (id INTEGER)"
  try
    db.transaction do
      db.exec "INSERT INTO t VALUES (1)"
      throw (IO.userError "test error")
  catch _ =>
    pure ()
  let rows ← db.query "SELECT * FROM t"
  rows.size ≡ 0

test "savepoint nested transaction" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (id INTEGER)"
  db.transaction do
    db.exec "INSERT INTO t VALUES (1)"
    db.withSavepoint "sp1" do
      db.exec "INSERT INTO t VALUES (2)"
  let rows ← db.query "SELECT * FROM t"
  rows.size ≡ 2

test "savepoint rollback" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (id INTEGER)"
  db.transaction do
    db.exec "INSERT INTO t VALUES (1)"
    try
      db.withSavepoint "sp1" do
        db.exec "INSERT INTO t VALUES (2)"
        throw (IO.userError "rollback nested")
    catch _ =>
      pure ()
  let rows ← db.query "SELECT * FROM t"
  rows.size ≡ 1

#generate_tests

end Tests.Transactions

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

def main : IO UInt32 := do
  IO.println "Quarry Library Tests"
  IO.println "===================="
  runAllSuites
