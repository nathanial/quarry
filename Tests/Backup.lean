/-
  Backup API Tests
-/
import Quarry
import Crucible

open Crucible
open Quarry

namespace Tests.Backup

testSuite "Backup API"

test "backup to file" := do
  -- Create source database with data
  let srcDb ← Database.openMemory
  srcDb.execSqlDdl "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
  let _ ← srcDb.execSqlInsert "INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Charlie')"

  -- Remove any existing test file first
  let tmpPath := "/tmp/quarry_backup_test.db"
  try IO.FS.removeFile tmpPath catch _ => pure ()

  -- Backup to a temp file
  srcDb.backupToFile tmpPath

  -- Open the backup and verify data
  let destDb ← Database.openFile tmpPath
  let rows ← destDb.query "SELECT name FROM users ORDER BY id"
  rows.size ≡ 3
  match rows[0]?.bind (·.get? 0), rows[1]?.bind (·.get? 0), rows[2]?.bind (·.get? 0) with
  | some (Value.text "Alice"), some (Value.text "Bob"), some (Value.text "Charlie") =>
    ensure true "backup data correct"
  | _, _, _ => throw (IO.userError "backup data mismatch")

test "backup between memory databases" := do
  -- Create source with data
  let srcDb ← Database.openMemory
  srcDb.execSqlDdl "CREATE TABLE items (id INTEGER, value TEXT)"
  let _ ← srcDb.execSqlInsert "INSERT INTO items VALUES (1, 'one'), (2, 'two')"

  -- Create empty destination
  let destDb ← Database.openMemory

  -- Backup
  srcDb.backupTo destDb

  -- Verify
  let rows ← destDb.query "SELECT value FROM items ORDER BY id"
  rows.size ≡ 2

test "incremental backup with progress" := do
  -- Create source with some data
  let srcDb ← Database.openMemory
  srcDb.execSqlDdl "CREATE TABLE data (id INTEGER PRIMARY KEY, content BLOB)"
  -- Insert some rows to have multiple pages
  for _ in [:100] do
    let _ ← srcDb.execSqlInsert "INSERT INTO data (content) VALUES (randomblob(1000))"

  let destDb ← Database.openMemory
  let backup ← srcDb.backupInit destDb

  -- Step through incrementally, tracking steps
  -- Note: pageCount/remaining are only reliable after first step
  let mut steps := 0
  while true do
    let more ← backup.step 5  -- Copy 5 pages at a time
    steps := steps + 1
    if !more then break  -- false = SQLITE_DONE
    if steps > 1000 then  -- Safety limit
      throw (IO.userError "too many steps")

  ensure (steps > 0) "took at least one step"

  -- Finish
  backup.finish

  -- Verify destination has data
  let rows ← destDb.query "SELECT COUNT(*) FROM data"
  match rows[0]?.bind (·.get? 0) with
  | some (Value.integer 100) => ensure true "all rows backed up"
  | _ => throw (IO.userError "row count mismatch")

test "backup progress percentage" := do
  let srcDb ← Database.openMemory
  srcDb.execSqlDdl "CREATE TABLE t (x INTEGER)"
  for i in [:50] do
    let _ ← srcDb.execSqlInsert s!"INSERT INTO t VALUES ({i})"

  let destDb ← Database.openMemory
  let backup ← srcDb.backupInit destDb

  -- Before starting, check we can get progress
  let initialProgress ← backup.progress
  ensure (initialProgress >= 0.0) "progress is non-negative"

  -- Run backup
  backup.runAll

test "backup empty database" := do
  let srcDb ← Database.openMemory
  let destDb ← Database.openMemory

  -- Backup empty database should succeed
  srcDb.backupTo destDb
  ensure true "empty backup succeeded"

test "backup preserves schema" := do
  let srcDb ← Database.openMemory
  srcDb.execSqlDdl "CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT NOT NULL)"
  srcDb.execSqlDdl "CREATE TABLE t2 (x REAL, y BLOB)"
  srcDb.execSqlDdl "CREATE INDEX idx_t1_b ON t1(b)"

  let destDb ← Database.openMemory
  srcDb.backupTo destDb

  -- Verify schema exists by inserting data
  let _ ← destDb.execSqlInsert "INSERT INTO t1 (a, b) VALUES (1, 'test')"
  let _ ← destDb.execSqlInsert "INSERT INTO t2 (x, y) VALUES (3.14, X'DEADBEEF')"

  let rows ← destDb.query "SELECT b FROM t1"
  match rows[0]?.bind (·.get? 0) with
  | some (Value.text "test") => ensure true "schema preserved"
  | _ => throw (IO.userError "schema not preserved")

end Tests.Backup
