/-
  User-Defined Function Tests
-/
import Quarry
import Crucible

open Crucible
open Quarry

namespace Tests.UserFunctions

testSuite "User-Defined Functions"

-- Scalar function tests

test "scalar function double" := do
  let db ← Database.openMemory
  db.createScalarFunction "double" 1 fun args => do
    match args[0]? with
    | some (Value.integer n) => return Value.integer (n * 2)
    | _ => return Value.null
  db.execSqlDdl "CREATE TABLE t (x INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO t VALUES (21)"
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
  db.execSqlDdl "CREATE TABLE nums (v INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO nums VALUES (2), (3), (4)"

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
  db.execSqlDdl "CREATE TABLE words (w TEXT)"
  let _ ← db.execSqlInsert "INSERT INTO words VALUES ('a'), ('b'), ('c')"

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
  db.execSqlDdl "CREATE TABLE sales (category TEXT, amount INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO sales VALUES ('A', 10), ('A', 20), ('B', 5)"

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
  db.execSqlDdl "CREATE TABLE empty (v INTEGER)"

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

-- Additional coverage tests

test "createFunction3 three arguments" := do
  let db ← Database.openMemory
  db.createFunction3 "clamp" (fun (lo hi val : Int) =>
    if val < lo then lo else if val > hi then hi else val)
  let rows ← db.query "SELECT clamp(0, 10, 15), clamp(0, 10, 5), clamp(0, 10, -5)"
  match rows[0]?.bind (·.get? 0), rows[0]?.bind (·.get? 1), rows[0]?.bind (·.get? 2) with
  | some (Value.integer 10), some (Value.integer 5), some (Value.integer 0) =>
    ensure true "clamp works"
  | _, _, _ => throw (IO.userError "clamp failed")

test "createIOFunction1 with IO" := do
  let db ← Database.openMemory
  let counterRef ← IO.mkRef (0 : Int)
  db.createIOFunction1 "incr_counter" (fun (n : Int) => do
    let old ← counterRef.get
    counterRef.set (old + n)
    return old)
  let _ ← db.query "SELECT incr_counter(5)"
  let _ ← db.query "SELECT incr_counter(3)"
  let final ← counterRef.get
  final ≡ 8

test "Bool argument and result" := do
  let db ← Database.openMemory
  db.createFunction1 "negate" (fun (b : Bool) => !b)
  let rows ← db.query "SELECT negate(1), negate(0)"
  match rows[0]?.bind (·.get? 0), rows[0]?.bind (·.get? 1) with
  | some (Value.integer 0), some (Value.integer 1) => ensure true "bool negation works"
  | _, _ => throw (IO.userError "expected 0 and 1")

test "ByteArray argument and result" := do
  let db ← Database.openMemory
  db.createFunction1 "blob_len" (fun (b : ByteArray) => (b.size : Int))
  db.execSqlDdl "CREATE TABLE blobs (data BLOB)"
  let _ ← db.execSqlInsert "INSERT INTO blobs VALUES (X'DEADBEEF')"
  let rows ← db.query "SELECT blob_len(data) FROM blobs"
  match rows[0]?.bind (·.get? 0) with
  | some (Value.integer 4) => ensure true "blob length correct"
  | _ => throw (IO.userError "expected 4")

test "raw Value passthrough" := do
  let db ← Database.openMemory
  -- Function that accepts any Value and returns its type name
  db.createFunction1 "type_name" (fun (v : Value) =>
    match v with
    | .null => "null"
    | .integer _ => "integer"
    | .real _ => "real"
    | .text _ => "text"
    | .blob _ => "blob")
  let rows ← db.query "SELECT type_name(42), type_name('hi'), type_name(NULL)"
  match rows[0]?.bind (·.get? 0), rows[0]?.bind (·.get? 1), rows[0]?.bind (·.get? 2) with
  | some (Value.text "integer"), some (Value.text "text"), some (Value.text "null") =>
    ensure true "type detection works"
  | _, _, _ => throw (IO.userError "type detection failed")

test "multi-argument aggregate" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE pairs (a INTEGER, b INTEGER)"
  let _ ← db.execSqlInsert "INSERT INTO pairs VALUES (1, 2), (3, 4), (5, 6)"

  -- Aggregate that sums products: (1*2) + (3*4) + (5*6) = 2 + 12 + 30 = 44
  db.createAggregateFunction "sum_products" 2
    (init := pure (Value.integer 0))
    (step := fun acc args => do
      match acc, args[0]?, args[1]? with
      | Value.integer sum, some (Value.integer a), some (Value.integer b) =>
        return Value.integer (sum + a * b)
      | _, _, _ => return acc)
    (final := fun acc => return acc)

  let rows ← db.query "SELECT sum_products(a, b) FROM pairs"
  match rows[0]?.bind (·.get? 0) with
  | some (Value.integer 44) => ensure true "sum of products correct"
  | _ => throw (IO.userError "expected 44")

test "function error returns null" := do
  let db ← Database.openMemory
  -- Function expects integer but we'll pass text - should return null due to type mismatch
  db.createFunction1 "strict_double" (fun (x : Int) => x * 2)
  let rows ← db.query "SELECT strict_double('not a number')"
  match rows[0]?.bind (·.get? 0) with
  | some Value.null => ensure true "type mismatch returns null"
  | _ => throw (IO.userError "expected null on type mismatch")

end Tests.UserFunctions
