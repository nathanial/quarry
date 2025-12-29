/-
  Quarry.Chisel.Execute
  Execute Chisel queries via Quarry
-/
import Chisel
import Quarry.Database
import Quarry.Chisel.Convert

namespace Quarry

/-- Default render context for SQLite -/
def sqliteContext : Chisel.RenderContext where
  dialect := .sqlite
  paramStyle := .question

namespace Database

-- ============================================================================
-- SELECT Execution
-- ============================================================================

/-- Execute a Chisel SELECT statement -/
def execSelect (db : Database) (stmt : Chisel.SelectCore)
    (ctx : Chisel.RenderContext := sqliteContext) : IO (Array Row) :=
  db.query (Chisel.renderSelect ctx stmt)

/-- Execute SELECT using monadic builder -/
def select (db : Database) (build : Chisel.SelectM Unit)
    (ctx : Chisel.RenderContext := sqliteContext) : IO (Array Row) :=
  db.execSelect (Chisel.SelectM.build build) ctx

/-- Execute SELECT, return first row -/
def selectOne (db : Database) (build : Chisel.SelectM Unit)
    (ctx : Chisel.RenderContext := sqliteContext) : IO (Option Row) := do
  let rows ← db.select build ctx
  return rows[0]?

/-- Set FROM with explicit TableRef -/
def from_' (ref : Chisel.TableRef) : Chisel.SelectM Unit :=
  modify fun s => { s with stmt := s.stmt.setFrom (some ref) }

-- ============================================================================
-- INSERT Execution
-- ============================================================================

/-- Execute a Chisel INSERT statement -/
def execInsert (db : Database) (stmt : Chisel.InsertStmt)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Unit :=
  db.execRaw (Chisel.renderInsert ctx stmt)

/-- Execute INSERT and return last inserted rowid -/
def execInsertReturning (db : Database) (stmt : Chisel.InsertStmt)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Int := do
  db.execRaw (Chisel.renderInsert ctx stmt)
  db.lastInsertRowid

-- ============================================================================
-- UPDATE Execution
-- ============================================================================

/-- Execute a Chisel UPDATE statement -/
def execUpdate (db : Database) (stmt : Chisel.UpdateStmt)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Unit :=
  db.execRaw (Chisel.renderUpdate ctx stmt)

/-- Execute UPDATE and return number of affected rows -/
def execUpdateReturning (db : Database) (stmt : Chisel.UpdateStmt)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Int := do
  db.execRaw (Chisel.renderUpdate ctx stmt)
  db.changes

-- ============================================================================
-- DELETE Execution
-- ============================================================================

/-- Execute a Chisel DELETE statement -/
def execDelete (db : Database) (stmt : Chisel.DeleteStmt)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Unit :=
  db.execRaw (Chisel.renderDelete ctx stmt)

/-- Execute DELETE and return number of affected rows -/
def execDeleteReturning (db : Database) (stmt : Chisel.DeleteStmt)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Int := do
  db.execRaw (Chisel.renderDelete ctx stmt)
  db.changes

-- ============================================================================
-- DDL Execution
-- ============================================================================

/-- Execute a CREATE TABLE statement -/
def execCreateTable (db : Database) (stmt : Chisel.CreateTableStmt)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Unit :=
  db.execRaw (Chisel.renderCreateTable ctx stmt)

/-- Execute a CREATE INDEX statement -/
def execCreateIndex (db : Database) (stmt : Chisel.CreateIndexStmt)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Unit :=
  db.execRaw (Chisel.renderCreateIndex ctx stmt)

/-- Execute a DROP TABLE statement -/
def execDropTable (db : Database) (stmt : Chisel.DropTableStmt)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Unit :=
  db.execRaw (Chisel.renderDropTable ctx stmt)

/-- Execute a DROP INDEX statement -/
def execDropIndex (db : Database) (stmt : Chisel.DropIndexStmt)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Unit :=
  db.execRaw (Chisel.renderDropIndex ctx stmt)

/-- Execute an ALTER TABLE statement -/
def execAlterTable (db : Database) (stmt : Chisel.AlterTableStmt)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Unit :=
  db.execRaw (Chisel.renderAlterTable ctx stmt)

-- ============================================================================
-- Parameterized Query Execution
-- ============================================================================

/-- Execute a SQL query with positional parameter binding -/
def queryWithParams (db : Database) (sql : String)
    (params : List Chisel.Literal) (ctx : Chisel.RenderContext := sqliteContext) : IO (Array Row) := do
  match Chisel.Parser.Expr.parse sql with
  | .error e => throw (IO.userError s!"Parse error: {e}")
  | .ok expr =>
    match Chisel.Parser.bindPositional expr params with
    | .error e => throw (IO.userError s!"Bind error: {e}")
    | .ok bound => db.query (Chisel.renderExpr ctx bound)

/-- Execute a SQL query with named parameter binding -/
def queryWithNamedParams (db : Database) (sql : String)
    (params : List (String × Chisel.Literal)) (ctx : Chisel.RenderContext := sqliteContext) : IO (Array Row) := do
  match Chisel.Parser.Expr.parse sql with
  | .error e => throw (IO.userError s!"Parse error: {e}")
  | .ok expr =>
    match Chisel.Parser.bindNamed expr params with
    | .error e => throw (IO.userError s!"Bind error: {e}")
    | .ok bound => db.query (Chisel.renderExpr ctx bound)

/-- Execute a SQL query with indexed parameter binding ($1, $2, etc) -/
def queryWithIndexedParams (db : Database) (sql : String)
    (params : Array Chisel.Literal) (ctx : Chisel.RenderContext := sqliteContext) : IO (Array Row) := do
  match Chisel.Parser.Expr.parse sql with
  | .error e => throw (IO.userError s!"Parse error: {e}")
  | .ok expr =>
    match Chisel.Parser.bindIndexed expr params with
    | .error e => throw (IO.userError s!"Bind error: {e}")
    | .ok bound => db.query (Chisel.renderExpr ctx bound)

-- ============================================================================
-- Unified SQL Execution (Single Entrypoint)
-- ============================================================================

/-- Result type for unified SQL execution -/
inductive ExecResult where
  | rows (data : Array Row)     -- SELECT returns rows
  | rowid (id : Int)            -- INSERT returns last rowid
  | changes (count : Int)       -- UPDATE/DELETE return affected rows
  | ok                          -- DDL returns success
  deriving Inhabited

/-- Parse and execute any SQL statement via Statement.parse -/
def execSql (db : Database) (sql : String)
    (ctx : Chisel.RenderContext := sqliteContext) : IO ExecResult := do
  match Chisel.Parser.Statement.parse sql with
  | .error e => throw (IO.userError s!"Parse error: {e}")
  | .ok stmt =>
    match stmt with
    | .select s =>
      let rows ← db.execSelect s ctx
      return .rows rows
    | .insert s =>
      db.execInsert s ctx
      let id ← db.lastInsertRowid
      return .rowid id
    | .update s =>
      db.execUpdate s ctx
      let n ← db.changes
      return .changes n
    | .delete s =>
      db.execDelete s ctx
      let n ← db.changes
      return .changes n
    | .createTable s =>
      db.execCreateTable s ctx
      return .ok
    | .createIndex s =>
      db.execCreateIndex s ctx
      return .ok
    | .dropTable s =>
      db.execDropTable s ctx
      return .ok
    | .dropIndex s =>
      db.execDropIndex s ctx
      return .ok
    | .alterTable s =>
      db.execAlterTable s ctx
      return .ok

/-- Parse and execute SQL, expecting SELECT (returns rows) -/
def execSqlSelect (db : Database) (sql : String)
    (ctx : Chisel.RenderContext := sqliteContext) : IO (Array Row) := do
  match ← db.execSql sql ctx with
  | .rows data => return data
  | _ => throw (IO.userError "Expected SELECT statement")

/-- Parse and execute SQL, expecting INSERT (returns rowid) -/
def execSqlInsert (db : Database) (sql : String)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Int := do
  match ← db.execSql sql ctx with
  | .rowid id => return id
  | _ => throw (IO.userError "Expected INSERT statement")

/-- Parse and execute SQL, expecting UPDATE/DELETE (returns affected rows) -/
def execSqlModify (db : Database) (sql : String)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Int := do
  match ← db.execSql sql ctx with
  | .changes n => return n
  | _ => throw (IO.userError "Expected UPDATE or DELETE statement")

/-- Parse and execute SQL, expecting DDL (CREATE/DROP/ALTER) -/
def execSqlDdl (db : Database) (sql : String)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Unit := do
  match ← db.execSql sql ctx with
  | .ok => return ()
  | _ => throw (IO.userError "Expected DDL statement")

end Database

end Quarry
