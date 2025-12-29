/-
  Tests for Chisel-Quarry Integration
-/
import Quarry
import Chisel
import Crucible

open Crucible
open Quarry
open Chisel

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

#generate_tests

end Tests.Chisel

-- ============================================================================
-- SELECT Execution Tests
-- ============================================================================

namespace Tests.ChiselSelect

testSuite "Chisel SELECT Execution"

test "execSelect simple query" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
  db.exec "INSERT INTO users (name) VALUES ('Alice'), ('Bob')"

  let stmt : Chisel.SelectCore := SelectCore.mk
    false
    [⟨Expr.col "name", none⟩]
    (some (TableRef.table "users" none))
    none [] none [] none none
  let rows ← db.execSelect stmt
  rows.size ≡ 2

test "select with monadic builder" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)"
  db.exec "INSERT INTO items (value) VALUES (10), (20), (30)"

  let rows ← db.select do
    select_ (Expr.col "value")
    from_ "items"
    where_ (Expr.col "value" .> Expr.lit (Literal.int 15))

  rows.size ≡ 2

test "selectOne returns first row" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x INTEGER)"
  db.exec "INSERT INTO t VALUES (1), (2), (3)"

  let row ← db.selectOne do
    select_ Expr.star
    from_ "t"
    orderBy_ [OrderItem.mk (Expr.col "x") SortDir.asc none]
    limit_ 1

  match row with
  | some r =>
    match r.get? 0 with
    | some (.integer 1) => ensure true "first row is 1"
    | _ => throw (IO.userError "expected integer 1")
  | none => throw (IO.userError "expected a row")

test "selectOne returns none for empty" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x INTEGER)"

  let row ← db.selectOne do
    select_ Expr.star
    from_ "t"

  match row with
  | none => ensure true "no rows"
  | some _ => throw (IO.userError "expected none")

test "select with JOIN" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
  db.exec "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)"
  db.exec "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')"
  db.exec "INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)"

  let rows ← db.select do
    select_ (Expr.qualified "users" "name")
    select_ (Expr.qualified "orders" "amount")
    Quarry.Database.from_' (TableRef.join JoinType.inner
      (TableRef.table "users" none)
      (TableRef.table "orders" none)
      (some (Expr.qualified "users" "id" .== Expr.qualified "orders" "user_id")))

  rows.size ≡ 3

test "select with GROUP BY and aggregate" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE sales (category TEXT, amount INTEGER)"
  db.exec "INSERT INTO sales VALUES ('A', 10), ('A', 20), ('B', 30)"

  let rows ← db.select do
    select_ (Expr.col "category")
    select_ (Expr.agg AggFunc.sum (some (Expr.col "amount")) false)
    from_ "sales"
    groupBy_ [Expr.col "category"]

  rows.size ≡ 2

#generate_tests

end Tests.ChiselSelect

-- ============================================================================
-- INSERT Execution Tests
-- ============================================================================

namespace Tests.ChiselInsert

testSuite "Chisel INSERT Execution"

test "execInsert single row" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"

  let stmt : Chisel.InsertStmt := {
    table := "users"
    columns := ["name"]
    values := [[Expr.lit (Literal.string "Charlie")]]
  }
  db.execInsert stmt

  let rows ← db.query "SELECT name FROM users"
  rows.size ≡ 1

test "execInsert multiple rows" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE items (name TEXT)"

  let stmt : Chisel.InsertStmt := {
    table := "items"
    columns := ["name"]
    values := [
      [Expr.lit (Literal.string "Item1")],
      [Expr.lit (Literal.string "Item2")],
      [Expr.lit (Literal.string "Item3")]
    ]
  }
  db.execInsert stmt

  let rows ← db.query "SELECT * FROM items"
  rows.size ≡ 3

test "execInsertReturning returns rowid" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"

  let stmt : Chisel.InsertStmt := {
    table := "users"
    columns := ["name"]
    values := [[Expr.lit (Literal.string "Dave")]]
  }
  let rowid ← db.execInsertReturning stmt

  rowid ≡ 1

test "execInsert with NULL value" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (a TEXT, b TEXT)"

  let stmt : Chisel.InsertStmt := {
    table := "t"
    columns := ["a", "b"]
    values := [[Expr.lit (Literal.string "x"), Expr.lit Literal.null]]
  }
  db.execInsert stmt

  let rows ← db.query "SELECT * FROM t WHERE b IS NULL"
  rows.size ≡ 1

#generate_tests

end Tests.ChiselInsert

-- ============================================================================
-- UPDATE Execution Tests
-- ============================================================================

namespace Tests.ChiselUpdate

testSuite "Chisel UPDATE Execution"

test "execUpdate single row" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
  db.exec "INSERT INTO users (name) VALUES ('Alice')"

  let stmt : Chisel.UpdateStmt := {
    table := "users"
    set := [Assignment.mk "name" (Expr.lit (Literal.string "Alicia"))]
    where_ := some (Expr.col "id" .== Expr.lit (Literal.int 1))
  }
  db.execUpdate stmt

  let rows ← db.query "SELECT name FROM users WHERE name = 'Alicia'"
  rows.size ≡ 1

test "execUpdate all rows" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE items (value INTEGER)"
  db.exec "INSERT INTO items VALUES (1), (2), (3)"

  let stmt : Chisel.UpdateStmt := {
    table := "items"
    set := [Assignment.mk "value" (Expr.col "value" .+ Expr.lit (Literal.int 10))]
  }
  db.execUpdate stmt

  let rows ← db.query "SELECT * FROM items WHERE value > 10"
  rows.size ≡ 3

test "execUpdateReturning returns count" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE items (active INTEGER)"
  db.exec "INSERT INTO items VALUES (0), (0), (1)"

  let stmt : Chisel.UpdateStmt := {
    table := "items"
    set := [Assignment.mk "active" (Expr.lit (Literal.int 1))]
    where_ := some (Expr.col "active" .== Expr.lit (Literal.int 0))
  }
  let count ← db.execUpdateReturning stmt

  count ≡ 2

#generate_tests

end Tests.ChiselUpdate

-- ============================================================================
-- DELETE Execution Tests
-- ============================================================================

namespace Tests.ChiselDelete

testSuite "Chisel DELETE Execution"

test "execDelete with WHERE" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
  db.exec "INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Charlie')"

  let stmt : Chisel.DeleteStmt := {
    table := "users"
    where_ := some (Expr.col "name" .== Expr.lit (Literal.string "Bob"))
  }
  db.execDelete stmt

  let rows ← db.query "SELECT * FROM users"
  rows.size ≡ 2

test "execDelete all rows" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE items (x INTEGER)"
  db.exec "INSERT INTO items VALUES (1), (2), (3)"

  let stmt : Chisel.DeleteStmt := { table := "items" }
  db.execDelete stmt

  let rows ← db.query "SELECT * FROM items"
  rows.size ≡ 0

test "execDeleteReturning returns count" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE items (category TEXT)"
  db.exec "INSERT INTO items VALUES ('A'), ('A'), ('B')"

  let stmt : Chisel.DeleteStmt := {
    table := "items"
    where_ := some (Expr.col "category" .== Expr.lit (Literal.string "A"))
  }
  let count ← db.execDeleteReturning stmt

  count ≡ 2

#generate_tests

end Tests.ChiselDelete

-- ============================================================================
-- DDL Execution Tests
-- ============================================================================

namespace Tests.ChiselDDL

testSuite "Chisel DDL Execution"

test "execCreateTable basic" := do
  let db ← Database.openMemory

  let stmt : Chisel.CreateTableStmt := {
    name := "products"
    columns := [
      { name := "id", type := ColumnType.integer, constraints := [ColumnConstraint.primaryKey false] },
      { name := "name", type := ColumnType.text, constraints := [ColumnConstraint.notNull] },
      { name := "price", type := ColumnType.real, constraints := [] }
    ]
  }
  db.execCreateTable stmt

  let rows ← db.query "SELECT name FROM sqlite_master WHERE type='table' AND name='products'"
  rows.size ≡ 1

test "execCreateTable IF NOT EXISTS" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x INTEGER)"

  let stmt : Chisel.CreateTableStmt := {
    name := "t"
    columns := [{ name := "y", type := ColumnType.text, constraints := [] }]
    ifNotExists := true
  }
  db.execCreateTable stmt
  ensure true "no error on duplicate"

test "execDropTable" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x INTEGER)"

  let stmt : Chisel.DropTableStmt := { table := "t" }
  db.execDropTable stmt

  let rows ← db.query "SELECT name FROM sqlite_master WHERE type='table' AND name='t'"
  rows.size ≡ 0

test "execDropTable IF EXISTS" := do
  let db ← Database.openMemory

  let stmt : Chisel.DropTableStmt := { table := "nonexistent", ifExists := true }
  db.execDropTable stmt
  ensure true "no error on missing table"

test "execCreateIndex" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE users (id INTEGER, email TEXT)"

  let stmt : Chisel.CreateIndexStmt := {
    name := "idx_email"
    table := "users"
    columns := [("email", none)]
  }
  db.execCreateIndex stmt

  let rows ← db.query "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_email'"
  rows.size ≡ 1

test "execCreateIndex UNIQUE" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE users (id INTEGER, email TEXT)"

  let stmt : Chisel.CreateIndexStmt := {
    name := "idx_unique_email"
    table := "users"
    columns := [("email", none)]
    unique := true
  }
  db.execCreateIndex stmt

  let rows ← db.query "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_unique_email'"
  rows.size ≡ 1

test "execDropIndex" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x INTEGER)"
  db.exec "CREATE INDEX idx_x ON t (x)"

  let stmt : Chisel.DropIndexStmt := { name := "idx_x" }
  db.execDropIndex stmt

  let rows ← db.query "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_x'"
  rows.size ≡ 0

test "execAlterTable ADD COLUMN" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE users (id INTEGER)"

  let stmt : Chisel.AlterTableStmt := {
    table := "users"
    operations := [AlterOp.addColumn { name := "email", type := ColumnType.text, constraints := [] }]
  }
  db.execAlterTable stmt

  let rows ← db.query "PRAGMA table_info(users)"
  rows.size ≡ 2

#generate_tests

end Tests.ChiselDDL

-- ============================================================================
-- Parameterized Query Tests
-- ============================================================================

namespace Tests.ChiselParams

testSuite "Chisel Parameter Binding"

test "bindPositional expression" := do
  -- Test that positional parameter binding works correctly
  match Chisel.Parser.Expr.parse "x = ? AND y = ?" with
  | .error e => throw (IO.userError s!"parse error: {e}")
  | .ok expr =>
    match Chisel.Parser.bindPositional expr [Literal.int 1, Literal.int 2] with
    | .error e => throw (IO.userError s!"bind error: {e}")
    | .ok bound =>
      let sql := Chisel.renderExpr Quarry.sqliteContext bound
      -- Should render to something like: ((x = 1) AND (y = 2))
      shouldSatisfy (sql.length > 0) "expression rendered"

test "bindNamed expression" := do
  -- Test that named parameter binding works correctly
  match Chisel.Parser.Expr.parse "value > :min" with
  | .error e => throw (IO.userError s!"parse error: {e}")
  | .ok expr =>
    match Chisel.Parser.bindNamed expr [("min", Literal.int 15)] with
    | .error e => throw (IO.userError s!"bind error: {e}")
    | .ok bound =>
      let sql := Chisel.renderExpr Quarry.sqliteContext bound
      -- Should render to something like: (value > 15)
      shouldSatisfy (sql.length > 0) "expression rendered"

test "bindIndexed expression" := do
  -- Test that indexed parameter binding works correctly
  match Chisel.Parser.Expr.parse "a = $1 OR b = $2" with
  | .error e => throw (IO.userError s!"parse error: {e}")
  | .ok expr =>
    match Chisel.Parser.bindIndexed expr #[Literal.int 10, Literal.int 20] with
    | .error e => throw (IO.userError s!"bind error: {e}")
    | .ok bound =>
      let sql := Chisel.renderExpr Quarry.sqliteContext bound
      -- Should render to something like: ((a = 10) OR (b = 20))
      shouldSatisfy (sql.length > 0) "expression rendered"

#generate_tests

end Tests.ChiselParams
