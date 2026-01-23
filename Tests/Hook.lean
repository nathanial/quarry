/-
  Update Hook Tests
  Tests for database update hook functionality
-/
import Crucible
import Quarry

open Crucible
open Quarry

namespace Tests.Hook

testSuite "Update Hook"

test "hook fires on insert" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)"

  let eventsRef ← IO.mkRef ([] : List (UpdateOp × String × Int))
  db.setUpdateHook fun op table rowid => do
    let events ← eventsRef.get
    eventsRef.set ((op, table, rowid) :: events)

  let _ ← db.execSqlInsert "INSERT INTO t (val) VALUES ('hello')"

  let events ← eventsRef.get
  events.length ≡ 1
  match events[0]? with
  | some (op, table, rowid) =>
    op ≡ UpdateOp.insert
    table ≡ "t"
    rowid ≡ 1
  | none => throw (IO.userError "expected event")

test "hook fires on update" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)"
  let _ ← db.execSqlInsert "INSERT INTO t (val) VALUES ('hello')"

  let eventsRef ← IO.mkRef ([] : List (UpdateOp × String × Int))
  db.setUpdateHook fun op table rowid => do
    let events ← eventsRef.get
    eventsRef.set ((op, table, rowid) :: events)

  let _ ← db.execSqlModify "UPDATE t SET val = 'world' WHERE id = 1"

  let events ← eventsRef.get
  events.length ≡ 1
  match events[0]? with
  | some (op, _, _) => op ≡ UpdateOp.update
  | none => throw (IO.userError "expected event")

test "hook fires on delete" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)"
  let _ ← db.execSqlInsert "INSERT INTO t (val) VALUES ('hello')"

  let eventsRef ← IO.mkRef ([] : List (UpdateOp × String × Int))
  db.setUpdateHook fun op table rowid => do
    let events ← eventsRef.get
    eventsRef.set ((op, table, rowid) :: events)

  let _ ← db.execSqlModify "DELETE FROM t WHERE id = 1"

  let events ← eventsRef.get
  events.length ≡ 1
  match events[0]? with
  | some (op, _, _) => op ≡ UpdateOp.delete
  | none => throw (IO.userError "expected event")

test "multiple operations fire multiple hooks" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)"

  let countRef ← IO.mkRef 0
  db.setUpdateHook fun _ _ _ => do
    let count ← countRef.get
    countRef.set (count + 1)

  let _ ← db.execSqlInsert "INSERT INTO t (val) VALUES ('a')"
  let _ ← db.execSqlInsert "INSERT INTO t (val) VALUES ('b')"
  let _ ← db.execSqlModify "UPDATE t SET val = 'c' WHERE id = 1"
  let _ ← db.execSqlModify "DELETE FROM t WHERE id = 2"

  let count ← countRef.get
  count ≡ 4

test "clear hook stops notifications" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)"

  let countRef ← IO.mkRef 0
  db.setUpdateHook fun _ _ _ => do
    let count ← countRef.get
    countRef.set (count + 1)

  let _ ← db.execSqlInsert "INSERT INTO t (val) VALUES ('a')"
  db.clearUpdateHook
  let _ ← db.execSqlInsert "INSERT INTO t (val) VALUES ('b')"

  let count ← countRef.get
  count ≡ 1

test "replacing hook cleans up old one" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)"

  let countRef1 ← IO.mkRef 0
  let countRef2 ← IO.mkRef 0

  db.setUpdateHook fun _ _ _ => do
    let count ← countRef1.get
    countRef1.set (count + 1)

  let _ ← db.execSqlInsert "INSERT INTO t (val) VALUES ('a')"

  db.setUpdateHook fun _ _ _ => do
    let count ← countRef2.get
    countRef2.set (count + 1)

  let _ ← db.execSqlInsert "INSERT INTO t (val) VALUES ('b')"

  let count1 ← countRef1.get
  let count2 ← countRef2.get
  count1 ≡ 1
  count2 ≡ 1

end Tests.Hook
