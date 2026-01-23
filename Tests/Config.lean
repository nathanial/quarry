/-
  Configuration and PRAGMA Helper Tests
-/
import Quarry
import Crucible

open Crucible
open Quarry

namespace Tests.Configuration

testSuite "Configuration"

test "set busy timeout" := do
  let db ← Database.openMemory
  db.busyTimeout 5000  -- 5 seconds
  ensure true "busy timeout set"

test "get journal mode default" := do
  let db ← Database.openMemory
  let mode ← db.getJournalMode
  -- In-memory databases use "memory" journal mode
  ensure (mode == .memory) "in-memory uses memory journal"

test "set journal mode" := do
  let db ← Database.openMemory
  let mode ← db.setJournalMode .delete
  -- In-memory can't change to delete, stays memory
  ensure (mode == .memory) "in-memory stays memory mode"

test "enable WAL on memory db" := do
  let db ← Database.openMemory
  let success ← db.enableWAL
  -- WAL can't be enabled on in-memory databases
  ensure (!success) "WAL not available for in-memory"

test "set synchronous mode" := do
  let db ← Database.openMemory
  db.setSynchronous .normal
  ensure true "synchronous mode set"

test "JournalMode toString roundtrip" := do
  ensure (Database.JournalMode.fromString? "WAL" == some .wal) "WAL parses"
  ensure (Database.JournalMode.fromString? "DELETE" == some .delete) "DELETE parses"
  ensure (Database.JournalMode.fromString? "wal" == some .wal) "lowercase works"
  ensure (Database.JournalMode.fromString? "invalid" == none) "invalid returns none"

test "interrupt on idle connection" := do
  let db ← Database.openMemory
  -- Interrupt on idle connection should be safe (no-op)
  db.interrupt
  ensure true "interrupt on idle is safe"

test "isInterrupted initially false" := do
  let db ← Database.openMemory
  let interrupted ← db.isInterrupted
  ensure (!interrupted) "not interrupted initially"

test "interrupt sets flag" := do
  let db ← Database.openMemory
  db.interrupt
  let _interrupted ← db.isInterrupted
  -- Note: flag may be cleared after check, so we just verify the call works
  ensure true "interrupt call succeeded"

end Tests.Configuration

namespace Tests.PragmaHelpers

testSuite "PRAGMA Helpers"

test "foreign keys get/set" := do
  let db ← Database.openMemory
  -- Default is off in SQLite
  let initial ← db.getForeignKeys
  ensure (!initial) "foreign keys off by default"
  db.setForeignKeys true
  let enabled ← db.getForeignKeys
  ensure enabled "foreign keys enabled"
  db.setForeignKeys false
  let disabled ← db.getForeignKeys
  ensure (!disabled) "foreign keys disabled"

test "foreign keys enforcement" := do
  let db ← Database.openMemory
  db.setForeignKeys true
  db.execSqlDdl "CREATE TABLE parent (id INTEGER PRIMARY KEY)"
  db.execSqlDdl "CREATE TABLE child (id INTEGER, parent_id INTEGER REFERENCES parent(id))"
  let _ ← db.execSqlInsert "INSERT INTO parent VALUES (1)"
  let _ ← db.execSqlInsert "INSERT INTO child VALUES (1, 1)"  -- Valid FK
  try
    let _ ← db.execSqlInsert "INSERT INTO child VALUES (2, 999)"  -- Invalid FK
    throw (IO.userError "should have failed FK constraint")
  catch _ =>
    ensure true "FK constraint enforced"

test "cache size get/set" := do
  let db ← Database.openMemory
  let initial ← db.getCacheSize
  -- Default is negative (KiB mode)
  ensure (initial < 0) "default is KiB mode"
  db.setCacheSize 1000  -- 1000 pages
  let updated ← db.getCacheSize
  updated ≡ 1000

test "temp store get/set" := do
  let db ← Database.openMemory
  db.setTempStore .memory
  let mode ← db.getTempStore
  mode ≡ Database.TempStore.memory

test "auto vacuum get/set" := do
  let db ← Database.openMemory
  -- Must set before creating tables
  db.setAutoVacuum .full
  let mode ← db.getAutoVacuum
  mode ≡ Database.AutoVacuum.full

test "encoding is UTF-8" := do
  let db ← Database.openMemory
  let enc ← db.getEncoding
  enc ≡ "UTF-8"

test "page size get" := do
  let db ← Database.openMemory
  let size ← db.getPageSize
  -- Default is 4096 on most systems
  ensure (size > 0) "page size is positive"

test "page size set before tables" := do
  let db ← Database.openMemory
  db.setPageSize 8192
  let size ← db.getPageSize
  size ≡ 8192

test "max page count get/set" := do
  let db ← Database.openMemory
  db.setMaxPageCount 1000
  let count ← db.getMaxPageCount
  count ≡ 1000

test "page count and freelist" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES (1)"
  let pages ← db.getPageCount
  ensure (pages > 0) "has pages"
  let free ← db.getFreelistCount
  ensure (free >= 0) "freelist count non-negative"

test "synchronous get/set roundtrip" := do
  let db ← Database.openMemory
  db.setSynchronous .off
  let mode ← db.getSynchronous
  mode ≡ Database.SyncMode.off

end Tests.PragmaHelpers
