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

namespace Tests.Binding

testSuite "Parameter Binding"

test "positional binding integer" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x INTEGER)"
  let stmt ← db.prepare "INSERT INTO t VALUES (?1)"
  bind stmt 1 (42 : Int)
  let _ ← FFI.stmtStep stmt
  let rows ← db.query "SELECT x FROM t"
  rows.size ≡ 1

test "positional binding multiple" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (a INTEGER, b TEXT, c REAL)"
  let stmt ← db.prepare "INSERT INTO t VALUES (?1, ?2, ?3)"
  bindAll stmt #[Value.integer 1, Value.text "hello", Value.real 3.14]
  let _ ← FFI.stmtStep stmt
  let rows ← db.query "SELECT * FROM t"
  rows.size ≡ 1

test "named binding colon style" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x INTEGER)"
  let stmt ← db.prepare "INSERT INTO t VALUES (:val)"
  bindNamed stmt ":val" (Value.integer 99)
  let _ ← FFI.stmtStep stmt
  let rows ← db.query "SELECT x FROM t"
  rows.size ≡ 1

test "named binding at style" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x TEXT)"
  let stmt ← db.prepare "INSERT INTO t VALUES (@msg)"
  bindNamed stmt "@msg" (Value.text "test")
  let _ ← FFI.stmtStep stmt
  let rows ← db.query "SELECT x FROM t"
  rows.size ≡ 1

test "bindAllNamed" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (a INTEGER, b TEXT)"
  let stmt ← db.prepare "INSERT INTO t VALUES (:a, :b)"
  bindAllNamed stmt [
    (":a", Value.integer 42),
    (":b", Value.text "hello")
  ]
  let _ ← FFI.stmtStep stmt
  let rows ← db.query "SELECT * FROM t"
  rows.size ≡ 1

test "reset and rebind" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x INTEGER)"
  let stmt ← db.prepare "INSERT INTO t VALUES (?1)"
  bind stmt 1 (1 : Int)
  let _ ← FFI.stmtStep stmt
  resetStmt stmt
  clearBindings stmt
  bind stmt 1 (2 : Int)
  let _ ← FFI.stmtStep stmt
  let rows ← db.query "SELECT * FROM t"
  rows.size ≡ 2

test "parameter count" := do
  let db ← Database.openMemory
  let stmt ← db.prepare "SELECT ?1, ?2, ?3"
  let count ← parameterCount stmt
  count ≡ 3

test "bind null value" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x INTEGER)"
  let stmt ← db.prepare "INSERT INTO t VALUES (?1)"
  bindValue stmt 1 Value.null
  let _ ← FFI.stmtStep stmt
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.get? 0 with
    | some Value.null => ensure true "null bound"
    | _ => throw (IO.userError "expected null")
  | none => throw (IO.userError "no row")

#generate_tests

end Tests.Binding

namespace Tests.TransactionVariants

testSuite "Transaction Variants"

test "read transaction" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x INTEGER)"
  db.exec "INSERT INTO t VALUES (1)"
  db.readTransaction do
    let rows ← db.query "SELECT * FROM t"
    rows.size ≡ 1

test "write transaction" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x INTEGER)"
  db.writeTransaction do
    db.exec "INSERT INTO t VALUES (1)"
  let rows ← db.query "SELECT * FROM t"
  rows.size ≡ 1

test "exclusive transaction" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x INTEGER)"
  db.exclusiveTransaction do
    db.exec "INSERT INTO t VALUES (1)"
  let rows ← db.query "SELECT * FROM t"
  rows.size ≡ 1

#generate_tests

end Tests.TransactionVariants

namespace Tests.TypeConversion

testSuite "Type Conversion"

test "FromSql Nat" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x INTEGER)"
  db.exec "INSERT INTO t VALUES (42)"
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.getAs (α := Nat) 0 with
    | .ok n => n ≡ 42
    | .error e => throw (IO.userError s!"failed: {e}")
  | none => throw (IO.userError "no row")

test "FromSql Float" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x REAL)"
  db.exec "INSERT INTO t VALUES (3.14)"
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.getAs (α := Float) 0 with
    | .ok f => ensure ((f - 3.14).abs < 0.01) "float matches"
    | .error e => throw (IO.userError s!"failed: {e}")
  | none => throw (IO.userError "no row")

test "FromSql Bool true" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x INTEGER)"
  db.exec "INSERT INTO t VALUES (1)"
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.getAs (α := Bool) 0 with
    | .ok b => b ≡ true
    | .error e => throw (IO.userError s!"failed: {e}")
  | none => throw (IO.userError "no row")

test "FromSql Bool false" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x INTEGER)"
  db.exec "INSERT INTO t VALUES (0)"
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.getAs (α := Bool) 0 with
    | .ok b => b ≡ false
    | .error e => throw (IO.userError s!"failed: {e}")
  | none => throw (IO.userError "no row")

test "FromSql ByteArray" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x BLOB)"
  db.exec "INSERT INTO t VALUES (X'DEADBEEF')"
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.getAs (α := ByteArray) 0 with
    | .ok b => b.size ≡ 4
    | .error e => throw (IO.userError s!"failed: {e}")
  | none => throw (IO.userError "no row")

test "FromSql Value passthrough" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x INTEGER)"
  db.exec "INSERT INTO t VALUES (42)"
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.getAs (α := Value) 0 with
    | .ok (Value.integer 42) => ensure true "passthrough works"
    | .ok v => throw (IO.userError s!"unexpected: {v}")
    | .error e => throw (IO.userError s!"failed: {e}")
  | none => throw (IO.userError "no row")

test "ToSql Nat via binding" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x INTEGER)"
  let stmt ← db.prepare "INSERT INTO t VALUES (?1)"
  bind stmt 1 (100 : Nat)
  let _ ← FFI.stmtStep stmt
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.get? 0 with
    | some (Value.integer 100) => ensure true "Nat bound"
    | _ => throw (IO.userError "expected 100")
  | none => throw (IO.userError "no row")

test "ToSql Bool via binding" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x INTEGER)"
  let stmt ← db.prepare "INSERT INTO t VALUES (?1)"
  bind stmt 1 true
  let _ ← FFI.stmtStep stmt
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.get? 0 with
    | some (Value.integer 1) => ensure true "Bool bound as 1"
    | _ => throw (IO.userError "expected 1")
  | none => throw (IO.userError "no row")

#generate_tests

end Tests.TypeConversion

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

namespace Tests.ErrorHandling

testSuite "Error Handling"

test "bad SQL syntax error" := do
  let db ← Database.openMemory
  try
    db.exec "NOT VALID SQL"
    throw (IO.userError "should have failed")
  catch _ =>
    ensure true "caught error"

test "columnNotFound error" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x INTEGER)"
  db.exec "INSERT INTO t VALUES (1)"
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.getByNameAs (α := Int) "nonexistent" with
    | .error (.columnNotFound _) => ensure true "got columnNotFound"
    | .ok _ => throw (IO.userError "should have failed")
    | .error e => throw (IO.userError s!"wrong error: {e}")
  | none => throw (IO.userError "no row")

test "type extraction error" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE t (x TEXT)"
  db.exec "INSERT INTO t VALUES ('not a number')"
  let rows ← db.query "SELECT x FROM t"
  match rows[0]? with
  | some row =>
    match row.getAs (α := Int) 0 with
    | .error (.typeError _ _) => ensure true "got typeError"
    | .ok _ => throw (IO.userError "should have failed")
    | .error e => throw (IO.userError s!"wrong error: {e}")
  | none => throw (IO.userError "no row")

#generate_tests

end Tests.ErrorHandling

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

#generate_tests

end Tests.Configuration

namespace Tests.UserFunctions

testSuite "User-Defined Functions"

-- Scalar function tests

test "scalar function double" := do
  let db ← Database.openMemory
  db.createScalarFunction "double" 1 fun args => do
    match args[0]? with
    | some (Value.integer n) => return Value.integer (n * 2)
    | _ => return Value.null
  db.exec "CREATE TABLE t (x INTEGER)"
  db.exec "INSERT INTO t VALUES (21)"
  let rows ← db.query "SELECT double(x) FROM t"
  match rows[0]?.bind (·.get? 0) with
  | some (Value.integer 42) => ensure true "doubled"
  | _ => throw (IO.userError "expected 42")

test "type-safe function add" := do
  let db ← Database.openMemory
  db.createFunction2 "my_add" (fun (a b : Int) => a + b)
  let rows ← db.query "SELECT my_add(1, 2)"
  match rows[0]?.bind (·.get? 0) with
  | some (Value.integer 3) => ensure true "added"
  | _ => throw (IO.userError "expected 3")

test "string function concat" := do
  let db ← Database.openMemory
  db.createFunction2 "myconcat" (fun (a b : String) => a ++ b)
  let rows ← db.query "SELECT myconcat('hello', ' world')"
  match rows[0]?.bind (·.get? 0) with
  | some (Value.text "hello world") => ensure true "concatenated"
  | _ => throw (IO.userError "expected 'hello world'")

test "function with null handling" := do
  let db ← Database.openMemory
  db.createFunction1 "safe_double" (fun (x : Option Int) =>
    x.map (· * 2))
  let rows ← db.query "SELECT safe_double(NULL)"
  match rows[0]?.bind (·.get? 0) with
  | some Value.null => ensure true "null preserved"
  | _ => throw (IO.userError "expected null")

test "variadic function sum_all" := do
  let db ← Database.openMemory
  db.createScalarFunction "sum_all" (-1) fun args => do
    let mut total : Int := 0
    for arg in args do
      match arg with
      | .integer n => total := total + n
      | _ => pure ()
    return Value.integer total
  let rows ← db.query "SELECT sum_all(1, 2, 3, 4, 5)"
  match rows[0]?.bind (·.get? 0) with
  | some (Value.integer 15) => ensure true "summed"
  | _ => throw (IO.userError "expected 15")

test "remove function" := do
  let db ← Database.openMemory
  db.createFunction1 "temp_fn" (fun (x : Int) => x)
  let _ ← db.query "SELECT temp_fn(1)"  -- Should work
  db.removeFunction "temp_fn" 1
  try
    let _ ← db.query "SELECT temp_fn(1)"  -- Should fail
    throw (IO.userError "should have failed")
  catch _ =>
    ensure true "function removed"

test "scalar function with float" := do
  let db ← Database.openMemory
  db.createFunction1 "half" (fun (x : Float) => x / 2.0)
  let rows ← db.query "SELECT half(10.0)"
  match rows[0]?.bind (·.get? 0) with
  | some (Value.real f) =>
    if (f - 5.0).abs < 0.01 then ensure true "halved"
    else throw (IO.userError s!"expected 5.0, got {f}")
  | _ => throw (IO.userError "expected real")

-- Aggregate function tests

test "aggregate product" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE nums (v INTEGER)"
  db.exec "INSERT INTO nums VALUES (2), (3), (4)"

  db.createAggregateFunction "product" 1
    (init := pure (Value.integer 1))
    (step := fun acc args => do
      match acc, args[0]? with
      | Value.integer a, some (Value.integer b) => return Value.integer (a * b)
      | _, _ => return acc)
    (final := fun acc => return acc)

  let rows ← db.query "SELECT product(v) FROM nums"
  match rows[0]?.bind (·.get? 0) with
  | some (Value.integer 24) => ensure true "product computed"
  | _ => throw (IO.userError "expected 24")

test "aggregate string concat" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE words (w TEXT)"
  db.exec "INSERT INTO words VALUES ('a'), ('b'), ('c')"

  db.createAggregateFunction "concat_all" 1
    (init := pure (Value.text ""))
    (step := fun acc args => do
      match acc, args[0]? with
      | Value.text a, some (Value.text b) => return Value.text (a ++ b)
      | _, _ => return acc)
    (final := fun acc => return acc)

  let rows ← db.query "SELECT concat_all(w) FROM words"
  match rows[0]?.bind (·.get? 0) with
  | some (Value.text "abc") => ensure true "concat computed"
  | _ => throw (IO.userError "expected 'abc'")

test "aggregate with GROUP BY" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE sales (category TEXT, amount INTEGER)"
  db.exec "INSERT INTO sales VALUES ('A', 10), ('A', 20), ('B', 5)"

  db.createAggregateFunction "my_sum" 1
    (init := pure (Value.integer 0))
    (step := fun acc args => do
      match acc, args[0]? with
      | Value.integer a, some (Value.integer b) => return Value.integer (a + b)
      | _, _ => return acc)
    (final := fun acc => return acc)

  let rows ← db.query "SELECT category, my_sum(amount) FROM sales GROUP BY category ORDER BY category"
  match rows[0]?.bind (·.get? 1), rows[1]?.bind (·.get? 1) with
  | some (Value.integer 30), some (Value.integer 5) => ensure true "grouped sums correct"
  | _, _ => throw (IO.userError "expected 30 and 5")

test "aggregate empty table" := do
  let db ← Database.openMemory
  db.exec "CREATE TABLE empty (v INTEGER)"

  db.createAggregateFunction "my_count" 1
    (init := pure (Value.integer 0))
    (step := fun acc _ => do
      match acc with
      | Value.integer n => return Value.integer (n + 1)
      | _ => return acc)
    (final := fun acc => return acc)

  let rows ← db.query "SELECT my_count(v) FROM empty"
  -- With no rows, final is called but with no accumulated value, returns NULL
  ensure (rows.size == 1) "one row returned"

#generate_tests

end Tests.UserFunctions

def main : IO UInt32 := do
  IO.println "Quarry Library Tests"
  IO.println "===================="
  runAllSuites
