/-
  Virtual Table Tests
  Tests for SQLite virtual table implementations
-/
import Crucible
import Quarry

open Crucible
open Quarry

namespace Tests.VirtualTable

testSuite "Virtual Tables - ArrayTable"

test "create array virtual table" := do
  let db ← Database.openMemory
  let _vtab ← db.createArrayVTable "users" {
    columns := #[
      { name := "id", sqlType := "INTEGER" },
      { name := "name", sqlType := "TEXT" }
    ]
  }
  -- Table exists if we can query it
  let rows ← db.query "SELECT * FROM users"
  rows.size ≡ 0

test "insert via Lean API" := do
  let db ← Database.openMemory
  let vtab ← db.createArrayVTable "users" {
    columns := #[
      { name := "id", sqlType := "INTEGER" },
      { name := "name", sqlType := "TEXT" }
    ]
  }
  let rowid ← vtab.insert #[.integer 1, .text "Alice"]
  rowid ≡ 1

  let rows ← db.query "SELECT * FROM users"
  rows.size ≡ 1
  match rows[0]? with
  | some row =>
    match row.get? 0, row.get? 1 with
    | some (.integer 1), some (.text "Alice") => ensure true "values match"
    | _, _ => throw (IO.userError "unexpected values")
  | none => throw (IO.userError "no row")

test "insert via SQL" := do
  let db ← Database.openMemory
  let _vtab ← db.createArrayVTable "users" {
    columns := #[
      { name := "id", sqlType := "INTEGER" },
      { name := "name", sqlType := "TEXT" }
    ]
  }
  db.execRaw "INSERT INTO users VALUES (1, 'Bob')"

  let rows ← db.query "SELECT * FROM users"
  rows.size ≡ 1
  match rows[0]? with
  | some row =>
    match row.get? 0, row.get? 1 with
    | some (.integer 1), some (.text "Bob") => ensure true "values match"
    | _, _ => throw (IO.userError "unexpected values")
  | none => throw (IO.userError "no row")

test "multiple inserts" := do
  let db ← Database.openMemory
  let vtab ← db.createArrayVTable "items" {
    columns := #[
      { name := "name", sqlType := "TEXT" },
      { name := "qty", sqlType := "INTEGER" }
    ]
  }
  let _ ← vtab.insert #[.text "Apples", .integer 10]
  let _ ← vtab.insert #[.text "Bananas", .integer 5]
  db.execRaw "INSERT INTO items VALUES ('Cherries', 20)"

  let rows ← db.query "SELECT * FROM items ORDER BY qty"
  rows.size ≡ 3
  match rows[0]? with
  | some row =>
    match row.get? 0 with
    | some (.text "Bananas") => ensure true "first row is Bananas"
    | _ => throw (IO.userError "expected Bananas first")
  | none => throw (IO.userError "no row")

test "delete via SQL" := do
  let db ← Database.openMemory
  let vtab ← db.createArrayVTable "items" {
    columns := #[{ name := "val", sqlType := "INTEGER" }]
  }
  let r1 ← vtab.insert #[.integer 100]
  let _ ← vtab.insert #[.integer 200]
  let _ ← vtab.insert #[.integer 300]

  db.execRaw s!"DELETE FROM items WHERE rowid = {r1}"

  let rows ← db.query "SELECT * FROM items ORDER BY val"
  rows.size ≡ 2

test "delete via Lean API" := do
  let db ← Database.openMemory
  let vtab ← db.createArrayVTable "items" {
    columns := #[{ name := "val", sqlType := "INTEGER" }]
  }
  let _ ← vtab.insert #[.integer 1]
  let r2 ← vtab.insert #[.integer 2]
  let _ ← vtab.insert #[.integer 3]

  let deleted ← vtab.delete r2
  deleted ≡ true

  let rows ← db.query "SELECT * FROM items ORDER BY val"
  rows.size ≡ 2

test "update via SQL" := do
  let db ← Database.openMemory
  let vtab ← db.createArrayVTable "users" {
    columns := #[
      { name := "name", sqlType := "TEXT" },
      { name := "age", sqlType := "INTEGER" }
    ]
  }
  let rowid ← vtab.insert #[.text "Alice", .integer 30]

  db.execRaw s!"UPDATE users SET age = 31 WHERE rowid = {rowid}"

  let rows ← db.query "SELECT * FROM users"
  rows.size ≡ 1
  match rows[0]? with
  | some row =>
    match row.get? 0, row.get? 1 with
    | some (.text "Alice"), some (.integer 31) => ensure true "update worked"
    | _, _ => throw (IO.userError "unexpected values after update")
  | none => throw (IO.userError "no row")

test "all column types" := do
  let db ← Database.openMemory
  let vtab ← db.createArrayVTable "mixed" {
    columns := #[
      { name := "i", sqlType := "INTEGER" },
      { name := "r", sqlType := "REAL" },
      { name := "t", sqlType := "TEXT" },
      { name := "b", sqlType := "BLOB" }
    ]
  }
  let bytes := ByteArray.mk #[0xDE, 0xAD, 0xBE, 0xEF]
  let _ ← vtab.insert #[.integer 42, .real 3.14, .text "hello", .blob bytes]

  let rows ← db.query "SELECT * FROM mixed"
  rows.size ≡ 1
  match rows[0]? with
  | some row =>
    match row.get? 0, row.get? 2, row.get? 3 with
    | some (.integer 42), some (.text "hello"), some (.blob b) =>
      if b == bytes then ensure true "blob matches"
      else throw (IO.userError "blob mismatch")
    | _, _, _ => throw (IO.userError "unexpected values")
  | none => throw (IO.userError "no row")

test "null values" := do
  let db ← Database.openMemory
  let vtab ← db.createArrayVTable "nullable" {
    columns := #[
      { name := "a", sqlType := "TEXT" },
      { name := "b", sqlType := "INTEGER" }
    ]
  }
  let _ ← vtab.insert #[.text "present", .null]

  let rows ← db.query "SELECT * FROM nullable"
  rows.size ≡ 1
  match rows[0]? with
  | some row =>
    match row.get? 0, row.get? 1 with
    | some (.text "present"), some .null => ensure true "null preserved"
    | _, _ => throw (IO.userError "unexpected values")
  | none => throw (IO.userError "no row")

test "clear all rows" := do
  let db ← Database.openMemory
  let vtab ← db.createArrayVTable "items" {
    columns := #[{ name := "val", sqlType := "INTEGER" }]
  }
  let _ ← vtab.insert #[.integer 1]
  let _ ← vtab.insert #[.integer 2]
  let _ ← vtab.insert #[.integer 3]

  vtab.clear
  let count ← vtab.size
  count ≡ 0

test "size tracking" := do
  let db ← Database.openMemory
  let vtab ← db.createArrayVTable "items" {
    columns := #[{ name := "val", sqlType := "INTEGER" }]
  }
  let s0 ← vtab.size
  s0 ≡ 0

  let _ ← vtab.insert #[.integer 1]
  let s1 ← vtab.size
  s1 ≡ 1

  let _ ← vtab.insert #[.integer 2]
  let s2 ← vtab.size
  s2 ≡ 2

test "allRows helper" := do
  let db ← Database.openMemory
  let vtab ← db.createArrayVTable "items" {
    columns := #[{ name := "val", sqlType := "INTEGER" }]
  }
  let _ ← vtab.insert #[.integer 10]
  let _ ← vtab.insert #[.integer 20]

  let rows ← vtab.allRows
  rows.size ≡ 2
  match rows[0]?, rows[1]? with
  | some r0, some r1 =>
    match r0.values[0]?, r1.values[0]? with
    | some (Value.integer 10), some (Value.integer 20) => ensure true "values match"
    | _, _ => throw (IO.userError "unexpected values")
  | _, _ => throw (IO.userError "missing rows")

test "join with regular table" := do
  let db ← Database.openMemory

  -- Create regular table
  db.execSqlDdl "CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT)"
  let _ ← db.execSqlInsert "INSERT INTO categories VALUES (1, 'Fruit')"
  let _ ← db.execSqlInsert "INSERT INTO categories VALUES (2, 'Vegetable')"

  -- Create virtual table
  let vtab ← db.createArrayVTable "products" {
    columns := #[
      { name := "name", sqlType := "TEXT" },
      { name := "category_id", sqlType := "INTEGER" }
    ]
  }
  let _ ← vtab.insert #[.text "Apple", .integer 1]
  let _ ← vtab.insert #[.text "Carrot", .integer 2]

  -- Join them
  let rows ← db.query "SELECT p.name, c.name FROM products p JOIN categories c ON p.category_id = c.id ORDER BY p.name"
  rows.size ≡ 2


end Tests.VirtualTable

namespace Tests.Generator

testSuite "Virtual Tables - Generator"

test "basic generator" := do
  let db ← Database.openMemory

  -- Register a simple counting generator
  db.registerGenerator "counter" (σ := Nat × Nat) {
    schema := { columns := #[{ name := "value", sqlType := "INTEGER" }] }
    init := fun _ => pure (1, 5)  -- count from 1 to 5
    hasMore := fun (current, limit) => current <= limit
    current := fun (current, _) => #[.integer current]
    advance := fun (current, limit) => (current + 1, limit)
  }

  let rows ← db.query "SELECT * FROM counter"
  rows.size ≡ 5
  match rows[0]?, rows[4]? with
  | some r0, some r4 =>
    match r0.get? 0, r4.get? 0 with
    | some (.integer 1), some (.integer 5) => ensure true "values match"
    | _, _ => throw (IO.userError "unexpected values")
  | _, _ => throw (IO.userError "missing rows")

test "generator with multiple columns" := do
  let db ← Database.openMemory

  -- Generate pairs
  db.registerGenerator "pairs" (σ := Nat) {
    schema := { columns := #[
      { name := "idx", sqlType := "INTEGER" },
      { name := "squared", sqlType := "INTEGER" }
    ]}
    init := fun _ => pure 1
    hasMore := fun n => n <= 4
    current := fun n => #[.integer n, .integer (n * n)]
    advance := fun n => n + 1
  }

  let rows ← db.query "SELECT * FROM pairs"
  rows.size ≡ 4
  match rows[3]? with
  | some row =>
    match row.get? 0, row.get? 1 with
    | some (.integer 4), some (.integer 16) => ensure true "4^2 = 16"
    | _, _ => throw (IO.userError "unexpected values")
  | none => throw (IO.userError "no row")

test "empty generator" := do
  let db ← Database.openMemory

  db.registerGenerator "empty" (σ := Unit) {
    schema := { columns := #[{ name := "val", sqlType := "INTEGER" }] }
    init := fun _ => pure ()
    hasMore := fun _ => false
    current := fun _ => #[.integer 0]
    advance := fun _ => ()
  }

  let rows ← db.query "SELECT * FROM empty"
  rows.size ≡ 0

test "generator with text values" := do
  let db ← Database.openMemory

  let names := #["Alice", "Bob", "Charlie"]
  db.registerGenerator "names" (σ := Nat) {
    schema := { columns := #[{ name := "name", sqlType := "TEXT" }] }
    init := fun _ => pure 0
    hasMore := fun idx => idx < names.size
    current := fun idx => #[.text (names[idx]?.getD "")]
    advance := fun idx => idx + 1
  }

  let rows ← db.query "SELECT * FROM names"
  rows.size ≡ 3
  match rows[1]? with
  | some row =>
    match row.get? 0 with
    | some (.text "Bob") => ensure true "Bob found"
    | _ => throw (IO.userError "expected Bob")
  | none => throw (IO.userError "no row")

test "generator is read-only" := do
  let db ← Database.openMemory

  db.registerGenerator "readonly" (σ := Nat) {
    schema := { columns := #[{ name := "val", sqlType := "INTEGER" }] }
    init := fun _ => pure 1
    hasMore := fun n => n <= 3
    current := fun n => #[.integer n]
    advance := fun n => n + 1
  }

  -- Attempting to INSERT should fail
  try
    db.execRaw "INSERT INTO readonly VALUES (999)"
    throw (IO.userError "Should have failed")
  catch _ =>
    ensure true "insert correctly rejected"

test "generator join with array table" := do
  let db ← Database.openMemory

  -- Create array table
  let vtab ← db.createArrayVTable "multipliers" {
    columns := #[
      { name := "factor", sqlType := "INTEGER" }
    ]
  }
  let _ ← vtab.insert #[.integer 2]
  let _ ← vtab.insert #[.integer 3]

  -- Create generator
  db.registerGenerator "nums" (σ := Nat) {
    schema := { columns := #[{ name := "value", sqlType := "INTEGER" }] }
    init := fun _ => pure 1
    hasMore := fun n => n <= 3
    current := fun n => #[.integer n]
    advance := fun n => n + 1
  }

  -- Cross join
  let rows ← db.query "SELECT n.value, m.factor, n.value * m.factor as result FROM nums n, multipliers m ORDER BY result"
  rows.size ≡ 6  -- 3 nums × 2 multipliers

test "generator with custom rowid" := do
  let db ← Database.openMemory

  db.registerGenerator "custom_rowid" (σ := Int) {
    schema := { columns := #[{ name := "val", sqlType := "INTEGER" }] }
    init := fun _ => pure 100
    hasMore := fun n => n <= 102
    current := fun n => #[.integer n]
    advance := fun n => n + 1
    rowid := some fun n => n * 10  -- Custom rowid: 1000, 1010, 1020
  }

  let rows ← db.query "SELECT rowid, val FROM custom_rowid"
  rows.size ≡ 3
  match rows[0]?, rows[2]? with
  | some r0, some r2 =>
    match r0.get? 0, r0.get? 1, r2.get? 0, r2.get? 1 with
    | some (.integer 1000), some (.integer 100), some (.integer 1020), some (.integer 102) =>
      ensure true "custom rowids work"
    | _, _, _, _ => throw (IO.userError "unexpected values")
  | _, _ => throw (IO.userError "missing rows")

end Tests.Generator
