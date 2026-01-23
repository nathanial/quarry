/-
  Database, Values, and Transaction Tests
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
  db.execSqlDdl "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"
  ensure true "table created"

test "insert and query" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
  let _ ← db.execSqlInsert "INSERT INTO users (name) VALUES ('Alice')"
  let _ ← db.execSqlInsert "INSERT INTO users (name) VALUES ('Bob')"
  let rows ← db.query "SELECT * FROM users"
  rows.size ≡ 2

test "last insert rowid" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE items (id INTEGER PRIMARY KEY)"
  -- DEFAULT VALUES not supported by Chisel, use explicit NULL
  let rowid ← db.execSqlInsert "INSERT INTO items (id) VALUES (NULL)"
  rowid ≡ 1

test "changes count" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE items (id INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO items VALUES (1), (2), (3)"
  let changes ← db.changes
  changes ≡ 3

test "query one" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES (42)"
  let row ← db.queryOne "SELECT x FROM t"
  match row with
  | some r =>
    match r.get? 0 with
    | some (Value.integer 42) => ensure true "found 42"
    | _ => throw (IO.userError "unexpected value")
  | none => throw (IO.userError "no row found")

end Tests.Database

namespace Tests.Values

testSuite "Value Types"

test "integer value" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (v INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES (42)"
  let rows ← db.query "SELECT v FROM t"
  match rows[0]? with
  | some row =>
    match row.get? 0 with
    | some (Value.integer 42) => ensure true "integer matches"
    | _ => throw (IO.userError "Expected integer 42")
  | none => throw (IO.userError "no row")

test "float value" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (v REAL)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES (3.14)"
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
  db.execSqlDdl "CREATE TABLE t (v TEXT)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES ('hello')"
  let rows ← db.query "SELECT v FROM t"
  match rows[0]? with
  | some row =>
    match row.get? 0 with
    | some (Value.text "hello") => ensure true "text matches"
    | _ => throw (IO.userError "Expected text 'hello'")
  | none => throw (IO.userError "no row")

test "null value" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (v TEXT)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES (NULL)"
  let rows ← db.query "SELECT v FROM t"
  match rows[0]? with
  | some row =>
    match row.get? 0 with
    | some Value.null => ensure true "null matches"
    | _ => throw (IO.userError "Expected null")
  | none => throw (IO.userError "no row")

test "blob value" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (v BLOB)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES (X'DEADBEEF')"
  let rows ← db.query "SELECT v FROM t"
  match rows[0]? with
  | some row =>
    match row.get? 0 with
    | some (Value.blob b) =>
      if b.size == 4 then ensure true "blob matches"
      else throw (IO.userError s!"Expected 4 bytes, got {b.size}")
    | _ => throw (IO.userError "Expected blob")
  | none => throw (IO.userError "no row")

end Tests.Values

namespace Tests.Transactions

testSuite "Transactions"

test "commit transaction" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (id INTEGER)"
  db.transaction do
    let _ ← db.execSqlInsert "INSERT INTO t VALUES (1)"
    let _ ← db.execSqlInsert "INSERT INTO t VALUES (2)"
  let rows ← db.query "SELECT * FROM t"
  rows.size ≡ 2

test "rollback on error" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (id INTEGER)"
  try
    db.transaction do
      let _ ← db.execSqlInsert "INSERT INTO t VALUES (1)"
      throw (IO.userError "test error")
  catch _ =>
    pure ()
  let rows ← db.query "SELECT * FROM t"
  rows.size ≡ 0

test "savepoint nested transaction" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (id INTEGER)"
  db.transaction do
    let _ ← db.execSqlInsert "INSERT INTO t VALUES (1)"
    db.withSavepoint "sp1" do
      let _ ← db.execSqlInsert "INSERT INTO t VALUES (2)"
  let rows ← db.query "SELECT * FROM t"
  rows.size ≡ 2

test "savepoint rollback" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (id INTEGER)"
  db.transaction do
    let _ ← db.execSqlInsert "INSERT INTO t VALUES (1)"
    try
      db.withSavepoint "sp1" do
        let _ ← db.execSqlInsert "INSERT INTO t VALUES (2)"
        throw (IO.userError "rollback nested")
    catch _ =>
      pure ()
  let rows ← db.query "SELECT * FROM t"
  rows.size ≡ 1

end Tests.Transactions

namespace Tests.TransactionVariants

testSuite "Transaction Variants"

test "read transaction" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES (1)"
  db.readTransaction do
    let rows ← db.query "SELECT * FROM t"
    rows.size ≡ 1

test "write transaction" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  db.writeTransaction do
    let _ ← db.execSqlInsert "INSERT INTO t VALUES (1)"
  let rows ← db.query "SELECT * FROM t"
  rows.size ≡ 1

test "exclusive transaction" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  db.exclusiveTransaction do
    let _ ← db.execSqlInsert "INSERT INTO t VALUES (1)"
  let rows ← db.query "SELECT * FROM t"
  rows.size ≡ 1

end Tests.TransactionVariants
