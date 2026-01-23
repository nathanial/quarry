/-
  Tests for Chisel-Quarry Integration

  These tests use the unified execSql API which parses SQL via Statement.parse
  and executes the appropriate statement type.
-/
import Quarry
import Chisel
import Crucible
import Staple

open Crucible
open Quarry
open Staple (String.containsSubstr)

namespace Tests.Chisel

-- ============================================================================
-- Type Conversion Tests
-- ============================================================================

testSuite "Chisel Type Conversion"

test "Literal.null to Value.null" := do
  let v := Quarry.Chisel.literalToValue .null
  match v with
  | .null => ensure true "null conversion"
  | _ => throw (IO.userError "expected null")

test "Literal.bool true to Value.integer 1" := do
  let v := Quarry.Chisel.literalToValue (.bool true)
  match v with
  | .integer 1 => ensure true "true -> 1"
  | _ => throw (IO.userError "expected integer 1")

test "Literal.bool false to Value.integer 0" := do
  let v := Quarry.Chisel.literalToValue (.bool false)
  match v with
  | .integer 0 => ensure true "false -> 0"
  | _ => throw (IO.userError "expected integer 0")

test "Literal.int to Value.integer" := do
  let v := Quarry.Chisel.literalToValue (.int 42)
  match v with
  | .integer 42 => ensure true "int conversion"
  | _ => throw (IO.userError "expected integer 42")

test "Literal.float to Value.real" := do
  let v := Quarry.Chisel.literalToValue (.float 3.14)
  match v with
  | .real f => shouldSatisfy ((f - 3.14).abs < 0.001) "float ~3.14"
  | _ => throw (IO.userError "expected real")

test "Literal.string to Value.text" := do
  let v := Quarry.Chisel.literalToValue (.string "hello")
  match v with
  | .text "hello" => ensure true "string conversion"
  | _ => throw (IO.userError "expected text")

test "Literal.blob to Value.blob" := do
  let v := Quarry.Chisel.literalToValue (.blob (ByteArray.mk #[0xDE, 0xAD]))
  match v with
  | .blob b => b.size ≡ 2
  | _ => throw (IO.userError "expected blob")

test "Value.null roundtrip" := do
  let lit := Quarry.Chisel.valueToLiteral .null
  match lit with
  | .null => ensure true "null roundtrip"
  | _ => throw (IO.userError "expected null")

test "Value.integer roundtrip" := do
  let lit := Quarry.Chisel.valueToLiteral (.integer 99)
  match lit with
  | .int 99 => ensure true "integer roundtrip"
  | _ => throw (IO.userError "expected int 99")

test "Value.real roundtrip" := do
  let lit := Quarry.Chisel.valueToLiteral (.real 2.718)
  match lit with
  | .float f => shouldSatisfy ((f - 2.718).abs < 0.001) "real roundtrip"
  | _ => throw (IO.userError "expected float")

test "Value.text roundtrip" := do
  let lit := Quarry.Chisel.valueToLiteral (.text "world")
  match lit with
  | .string "world" => ensure true "text roundtrip"
  | _ => throw (IO.userError "expected string")

end Tests.Chisel

-- ============================================================================
-- Unified SQL Execution Tests (SELECT)
-- ============================================================================

namespace Tests.ChiselSelect

testSuite "Chisel SELECT Execution"

test "execSqlSelect simple query" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
  let _ ← db.execSqlInsert "INSERT INTO users (name) VALUES ('Alice'), ('Bob')"

  let rows ← db.execSqlSelect "SELECT name FROM users"
  rows.size ≡ 2

test "execSqlSelect with WHERE" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO items (value) VALUES (10), (20), (30)"

  let rows ← db.execSqlSelect "SELECT value FROM items WHERE value > 15"
  rows.size ≡ 2

test "execSqlSelect returns first row" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES (1), (2), (3)"

  let rows ← db.execSqlSelect "SELECT * FROM t ORDER BY x ASC LIMIT 1"
  match rows[0]? with
  | some r =>
    match r.get? 0 with
    | some (.integer 1) => ensure true "first row is 1"
    | _ => throw (IO.userError "expected integer 1")
  | none => throw (IO.userError "expected a row")

test "execSqlSelect returns empty for no matches" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"

  let rows ← db.execSqlSelect "SELECT * FROM t"
  rows.size ≡ 0

test "execSqlSelect with JOIN" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
  db.execSqlDdl "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')"
  let _ ← db.execSqlInsert "INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)"

  let rows ← db.execSqlSelect
    "SELECT users.name, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id"
  rows.size ≡ 3

test "execSqlSelect with GROUP BY and aggregate" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE sales (category TEXT, amount INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO sales VALUES ('A', 10), ('A', 20), ('B', 30)"

  let rows ← db.execSqlSelect "SELECT category, SUM(amount) FROM sales GROUP BY category"
  rows.size ≡ 2

test "execSqlSelect with DISTINCT" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE tags (name TEXT)"
  let _ ← db.execSqlInsert "INSERT INTO tags VALUES ('foo'), ('foo'), ('bar')"

  let rows ← db.execSqlSelect "SELECT DISTINCT name FROM tags"
  rows.size ≡ 2

test "execSqlSelect with ORDER BY DESC" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE nums (n INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO nums VALUES (3), (1), (2)"

  let rows ← db.execSqlSelect "SELECT n FROM nums ORDER BY n DESC"
  match rows[0]? with
  | some r =>
    match r.get? 0 with
    | some (Quarry.Value.integer 3) => ensure true "first is 3"
    | _ => throw (IO.userError "expected 3 first")
  | none => throw (IO.userError "expected a row")

test "execSqlSelect with LIMIT and OFFSET" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE items (n INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO items VALUES (1), (2), (3), (4), (5)"

  let rows ← db.execSqlSelect "SELECT n FROM items ORDER BY n LIMIT 2 OFFSET 2"
  rows.size ≡ 2
  match rows[0]? with
  | some r =>
    match r.get? 0 with
    | some (Quarry.Value.integer 3) => ensure true "starts at 3"
    | _ => throw (IO.userError "expected 3")
  | none => throw (IO.userError "expected a row")

test "execSqlSelect with subquery" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES (1), (2), (3)"

  let rows ← db.execSqlSelect "SELECT * FROM t WHERE x > (SELECT MIN(x) FROM t)"
  rows.size ≡ 2

end Tests.ChiselSelect

-- ============================================================================
-- Unified SQL Execution Tests (INSERT)
-- ============================================================================

namespace Tests.ChiselInsert

testSuite "Chisel INSERT Execution"

test "execSqlInsert single row" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"

  let rowid ← db.execSqlInsert "INSERT INTO users (name) VALUES ('Charlie')"
  rowid ≡ 1

test "execSqlInsert multiple rows" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE items (name TEXT)"

  let _ ← db.execSqlInsert "INSERT INTO items (name) VALUES ('Item1'), ('Item2'), ('Item3')"
  let rows ← db.query "SELECT * FROM items"
  rows.size ≡ 3

test "execSqlInsert returns rowid" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"

  let rowid1 ← db.execSqlInsert "INSERT INTO users (name) VALUES ('Dave')"
  let rowid2 ← db.execSqlInsert "INSERT INTO users (name) VALUES ('Eve')"
  rowid1 ≡ 1
  rowid2 ≡ 2

test "execSqlInsert with NULL value" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (a TEXT, b TEXT)"

  let _ ← db.execSqlInsert "INSERT INTO t (a, b) VALUES ('x', NULL)"
  let rows ← db.query "SELECT * FROM t WHERE b IS NULL"
  rows.size ≡ 1

test "execSqlInsert with numeric values" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE nums (i INTEGER, f REAL)"

  let _ ← db.execSqlInsert "INSERT INTO nums (i, f) VALUES (42, 3.14)"
  let rows ← db.query "SELECT * FROM nums"
  rows.size ≡ 1

test "execSqlInsert with boolean expressions" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE flags (active INTEGER)"

  let _ ← db.execSqlInsert "INSERT INTO flags (active) VALUES (1)"
  let _ ← db.execSqlInsert "INSERT INTO flags (active) VALUES (0)"
  let rows ← db.query "SELECT * FROM flags WHERE active = 1"
  rows.size ≡ 1

end Tests.ChiselInsert

-- ============================================================================
-- Unified SQL Execution Tests (UPDATE)
-- ============================================================================

namespace Tests.ChiselUpdate

testSuite "Chisel UPDATE Execution"

test "execSqlModify UPDATE single row" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
  let _ ← db.execSqlInsert "INSERT INTO users (name) VALUES ('Alice')"

  let count ← db.execSqlModify "UPDATE users SET name = 'Alicia' WHERE id = 1"
  count ≡ 1

  let rows ← db.query "SELECT name FROM users WHERE name = 'Alicia'"
  rows.size ≡ 1

test "execSqlModify UPDATE all rows" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE items (value INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO items VALUES (1), (2), (3)"

  let count ← db.execSqlModify "UPDATE items SET value = value + 10"
  count ≡ 3

  let rows ← db.query "SELECT * FROM items WHERE value > 10"
  rows.size ≡ 3

test "execSqlModify UPDATE with expression" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE products (price REAL)"
  let _ ← db.execSqlInsert "INSERT INTO products VALUES (100.0), (200.0)"

  let count ← db.execSqlModify "UPDATE products SET price = price * 1.1"
  count ≡ 2

test "execSqlModify UPDATE returns count" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE items (active INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO items VALUES (0), (0), (1)"

  let count ← db.execSqlModify "UPDATE items SET active = 1 WHERE active = 0"
  count ≡ 2

test "execSqlModify UPDATE no matches" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES (1)"

  let count ← db.execSqlModify "UPDATE t SET x = 99 WHERE x = 999"
  count ≡ 0

end Tests.ChiselUpdate

-- ============================================================================
-- Unified SQL Execution Tests (DELETE)
-- ============================================================================

namespace Tests.ChiselDelete

testSuite "Chisel DELETE Execution"

test "execSqlModify DELETE with WHERE" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
  let _ ← db.execSqlInsert "INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Charlie')"

  let count ← db.execSqlModify "DELETE FROM users WHERE name = 'Bob'"
  count ≡ 1

  let rows ← db.query "SELECT * FROM users"
  rows.size ≡ 2

test "execSqlModify DELETE all rows" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE items (x INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO items VALUES (1), (2), (3)"

  let count ← db.execSqlModify "DELETE FROM items"
  count ≡ 3

  let rows ← db.query "SELECT * FROM items"
  rows.size ≡ 0

test "execSqlModify DELETE returns count" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE items (category TEXT)"
  let _ ← db.execSqlInsert "INSERT INTO items VALUES ('A'), ('A'), ('B')"

  let count ← db.execSqlModify "DELETE FROM items WHERE category = 'A'"
  count ≡ 2

test "execSqlModify DELETE no matches" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES (1)"

  let count ← db.execSqlModify "DELETE FROM t WHERE x = 999"
  count ≡ 0

test "execSqlModify DELETE with complex WHERE" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE products (name TEXT, price INTEGER, active INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO products VALUES ('A', 10, 1), ('B', 20, 0), ('C', 30, 0)"

  let count ← db.execSqlModify "DELETE FROM products WHERE active = 0 AND price > 15"
  count ≡ 2

end Tests.ChiselDelete

-- ============================================================================
-- Unified SQL Execution Tests (DDL)
-- ============================================================================

namespace Tests.ChiselDDL

testSuite "Chisel DDL Execution"

test "execSqlDdl CREATE TABLE basic" := do
  let db ← Database.openMemory

  db.execSqlDdl "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL)"

  let rows ← db.query "SELECT name FROM sqlite_master WHERE type='table' AND name='products'"
  rows.size ≡ 1

test "execSqlDdl CREATE TABLE IF NOT EXISTS" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"

  db.execSqlDdl "CREATE TABLE IF NOT EXISTS t (y TEXT)"
  ensure true "no error on duplicate"

test "execSqlDdl DROP TABLE" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"

  db.execSqlDdl "DROP TABLE t"

  let rows ← db.query "SELECT name FROM sqlite_master WHERE type='table' AND name='t'"
  rows.size ≡ 0

test "execSqlDdl DROP TABLE IF EXISTS" := do
  let db ← Database.openMemory

  db.execSqlDdl "DROP TABLE IF EXISTS nonexistent"
  ensure true "no error on missing table"

test "execSqlDdl CREATE INDEX" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE users (id INTEGER, email TEXT)"

  db.execSqlDdl "CREATE INDEX idx_email ON users (email)"

  let rows ← db.query "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_email'"
  rows.size ≡ 1

test "execSqlDdl CREATE UNIQUE INDEX" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE users (id INTEGER, email TEXT)"

  db.execSqlDdl "CREATE UNIQUE INDEX idx_unique_email ON users (email)"

  let rows ← db.query "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_unique_email'"
  rows.size ≡ 1

test "execSqlDdl DROP INDEX" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  db.execSqlDdl "CREATE INDEX idx_x ON t (x)"

  db.execSqlDdl "DROP INDEX idx_x"

  let rows ← db.query "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_x'"
  rows.size ≡ 0

test "execSqlDdl ALTER TABLE ADD COLUMN" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE users (id INTEGER)"

  db.execSqlDdl "ALTER TABLE users ADD COLUMN email TEXT"

  let rows ← db.query "PRAGMA table_info(users)"
  rows.size ≡ 2

test "execSqlDdl CREATE TABLE with constraints" := do
  let db ← Database.openMemory

  db.execSqlDdl "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, amount REAL)"

  let rows ← db.query "SELECT name FROM sqlite_master WHERE type='table' AND name='orders'"
  rows.size ≡ 1

test "execSqlDdl CREATE TABLE with UNIQUE" := do
  let db ← Database.openMemory

  db.execSqlDdl "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE)"

  -- Insert should work
  let _ ← db.execSqlInsert "INSERT INTO users (email) VALUES ('test@test.com')"
  let rows ← db.query "SELECT * FROM users"
  rows.size ≡ 1

end Tests.ChiselDDL

-- ============================================================================
-- Parameterized Query Tests
-- ============================================================================

namespace Tests.ChiselParams

testSuite "Chisel Parameter Binding"

test "bindPositional expression" := do
  match Chisel.Parser.Expr.parse "x = ? AND y = ?" with
  | .error e => throw (IO.userError s!"parse error: {e}")
  | .ok expr =>
    match Chisel.Parser.bindPositional expr [Chisel.Literal.int 1, Chisel.Literal.int 2] with
    | .error e => throw (IO.userError s!"bind error: {e}")
    | .ok bound =>
      let sql := Chisel.renderExpr Quarry.sqliteContext bound
      shouldSatisfy (sql.length > 0) "expression rendered"

test "bindNamed expression" := do
  match Chisel.Parser.Expr.parse "value > :min" with
  | .error e => throw (IO.userError s!"parse error: {e}")
  | .ok expr =>
    match Chisel.Parser.bindNamed expr [("min", Chisel.Literal.int 15)] with
    | .error e => throw (IO.userError s!"bind error: {e}")
    | .ok bound =>
      let sql := Chisel.renderExpr Quarry.sqliteContext bound
      shouldSatisfy (sql.length > 0) "expression rendered"

test "bindIndexed expression" := do
  match Chisel.Parser.Expr.parse "a = $1 OR b = $2" with
  | .error e => throw (IO.userError s!"parse error: {e}")
  | .ok expr =>
    match Chisel.Parser.bindIndexed expr #[Chisel.Literal.int 10, Chisel.Literal.int 20] with
    | .error e => throw (IO.userError s!"bind error: {e}")
    | .ok bound =>
      let sql := Chisel.renderExpr Quarry.sqliteContext bound
      shouldSatisfy (sql.length > 0) "expression rendered"

end Tests.ChiselParams

-- ============================================================================
-- execSql Unified API Tests
-- ============================================================================

namespace Tests.ChiselUnified

testSuite "Chisel Unified execSql API"

test "execSql SELECT returns rows" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES (1), (2)"

  match ← db.execSql "SELECT * FROM t" with
  | .rows data => data.size ≡ 2
  | _ => throw (IO.userError "expected rows result")

test "execSql INSERT returns rowid" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (id INTEGER PRIMARY KEY, x TEXT)"

  match ← db.execSql "INSERT INTO t (x) VALUES ('test')" with
  | .rowid id => id ≡ 1
  | _ => throw (IO.userError "expected rowid result")

test "execSql UPDATE returns changes" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES (1), (2), (3)"

  match ← db.execSql "UPDATE t SET x = x + 10" with
  | .changes n => n ≡ 3
  | _ => throw (IO.userError "expected changes result")

test "execSql DELETE returns changes" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES (1), (2)"

  match ← db.execSql "DELETE FROM t WHERE x = 1" with
  | .changes n => n ≡ 1
  | _ => throw (IO.userError "expected changes result")

test "execSql CREATE TABLE returns ok" := do
  let db ← Database.openMemory

  match ← db.execSql "CREATE TABLE newt (id INTEGER PRIMARY KEY)" with
  | .ok => ensure true "DDL succeeded"
  | _ => throw (IO.userError "expected ok result")

test "execSql DROP TABLE returns ok" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"

  match ← db.execSql "DROP TABLE t" with
  | .ok => ensure true "DDL succeeded"
  | _ => throw (IO.userError "expected ok result")

test "execSql parse error" := do
  let db ← Database.openMemory

  try
    let _ ← db.execSql "NOT VALID SQL AT ALL !!!"
    throw (IO.userError "expected parse error")
  catch e =>
    let msg := toString e
    shouldSatisfy (String.containsSubstr msg "Parse error") "got parse error"

end Tests.ChiselUnified
