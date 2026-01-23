/-
  Serialize/Deserialize Tests
-/
import Crucible
import Quarry

open Crucible
open Quarry

namespace Tests.Serialize

testSuite "Serialize/Deserialize"

test "serialize empty database" := do
  let db ← Database.openMemory
  let bytes ← db.serialize
  -- Empty database still has SQLite header (at least 100 bytes)
  ensure (bytes.size > 0) "should have header"

test "serialize and deserialize roundtrip" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)"
  let _ ← db.execSqlInsert "INSERT INTO t (val) VALUES ('hello')"
  let _ ← db.execSqlInsert "INSERT INTO t (val) VALUES ('world')"

  let bytes ← db.serialize

  let db2 ← Database.deserialize bytes
  let rows ← db2.query "SELECT val FROM t ORDER BY id"
  rows.size ≡ 2

test "clone database" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)"
  let _ ← db.execSqlInsert "INSERT INTO t (val) VALUES ('original')"

  let clone ← db.clone

  -- Modify original
  let _ ← db.execSqlModify "UPDATE t SET val = 'modified'"

  -- Clone should still have original value
  let rows ← clone.query "SELECT val FROM t"
  match rows[0]?.bind (·.get? 0) with
  | some (Value.text v) => v ≡ "original"
  | _ => throw (IO.userError "expected text")

test "deserialize into existing connection" := do
  let db1 ← Database.openMemory
  db1.execSqlDdl "CREATE TABLE t (val TEXT)"
  let _ ← db1.execSqlInsert "INSERT INTO t VALUES ('from db1')"
  let bytes ← db1.serialize

  let db2 ← Database.openMemory
  db2.execSqlDdl "CREATE TABLE other (x INTEGER)"
  db2.deserializeInto bytes

  -- db2 should now have t, not other
  let rows ← db2.query "SELECT val FROM t"
  rows.size ≡ 1

test "readonly deserialize prevents writes" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (val TEXT)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES ('test')"
  let bytes ← db.serialize

  let db2 ← Database.deserialize bytes (readOnly := true)

  -- Reading should work
  let rows ← db2.query "SELECT val FROM t"
  rows.size ≡ 1

  -- Writing should fail
  try
    let _ ← db2.execSqlInsert "INSERT INTO t VALUES ('new')"
    throw (IO.userError "expected write to fail")
  catch _ =>
    ensure true "write correctly rejected"

test "serialize preserves data types" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (i INTEGER, r REAL, t TEXT, b BLOB)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES (42, 3.14, 'hello', X'DEADBEEF')"

  let bytes ← db.serialize
  let db2 ← Database.deserialize bytes

  let rows ← db2.query "SELECT * FROM t"
  match rows[0]? with
  | some row =>
    match row.get? 0 with
    | some (Value.integer n) => n ≡ 42
    | _ => throw (IO.userError "expected integer")
    match row.get? 1 with
    | some (Value.real r) => ensure (r > 3.0 && r < 4.0) "expected ~3.14"
    | _ => throw (IO.userError "expected real")
    match row.get? 2 with
    | some (Value.text s) => s ≡ "hello"
    | _ => throw (IO.userError "expected text")
    match row.get? 3 with
    | some (Value.blob b) => b.size ≡ 4
    | _ => throw (IO.userError "expected blob")
  | none => throw (IO.userError "expected row")

test "multiple tables roundtrip" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
  db.execSqlDdl "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT)"
  let _ ← db.execSqlInsert "INSERT INTO users VALUES (1, 'Alice')"
  let _ ← db.execSqlInsert "INSERT INTO users VALUES (2, 'Bob')"
  let _ ← db.execSqlInsert "INSERT INTO posts VALUES (1, 1, 'Hello')"
  let _ ← db.execSqlInsert "INSERT INTO posts VALUES (2, 2, 'World')"

  let bytes ← db.serialize
  let db2 ← Database.deserialize bytes

  let users ← db2.query "SELECT COUNT(*) FROM users"
  let posts ← db2.query "SELECT COUNT(*) FROM posts"

  match users[0]?.bind (·.get? 0) with
  | some (Value.integer n) => n ≡ 2
  | _ => throw (IO.userError "expected user count")

  match posts[0]?.bind (·.get? 0) with
  | some (Value.integer n) => n ≡ 2
  | _ => throw (IO.userError "expected post count")

end Tests.Serialize
