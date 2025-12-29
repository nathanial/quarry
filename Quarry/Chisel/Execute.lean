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
  db.exec (Chisel.renderInsert ctx stmt)

/-- Execute INSERT and return last inserted rowid -/
def execInsertReturning (db : Database) (stmt : Chisel.InsertStmt)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Int := do
  db.exec (Chisel.renderInsert ctx stmt)
  db.lastInsertRowid

-- ============================================================================
-- UPDATE Execution
-- ============================================================================

/-- Execute a Chisel UPDATE statement -/
def execUpdate (db : Database) (stmt : Chisel.UpdateStmt)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Unit :=
  db.exec (Chisel.renderUpdate ctx stmt)

/-- Execute UPDATE and return number of affected rows -/
def execUpdateReturning (db : Database) (stmt : Chisel.UpdateStmt)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Int := do
  db.exec (Chisel.renderUpdate ctx stmt)
  db.changes

-- ============================================================================
-- DELETE Execution
-- ============================================================================

/-- Execute a Chisel DELETE statement -/
def execDelete (db : Database) (stmt : Chisel.DeleteStmt)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Unit :=
  db.exec (Chisel.renderDelete ctx stmt)

/-- Execute DELETE and return number of affected rows -/
def execDeleteReturning (db : Database) (stmt : Chisel.DeleteStmt)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Int := do
  db.exec (Chisel.renderDelete ctx stmt)
  db.changes

-- ============================================================================
-- DDL Execution
-- ============================================================================

/-- Execute a CREATE TABLE statement -/
def execCreateTable (db : Database) (stmt : Chisel.CreateTableStmt)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Unit :=
  db.exec (Chisel.renderCreateTable ctx stmt)

/-- Execute a CREATE INDEX statement -/
def execCreateIndex (db : Database) (stmt : Chisel.CreateIndexStmt)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Unit :=
  db.exec (Chisel.renderCreateIndex ctx stmt)

/-- Execute a DROP TABLE statement -/
def execDropTable (db : Database) (stmt : Chisel.DropTableStmt)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Unit :=
  db.exec (Chisel.renderDropTable ctx stmt)

/-- Execute a DROP INDEX statement -/
def execDropIndex (db : Database) (stmt : Chisel.DropIndexStmt)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Unit :=
  db.exec (Chisel.renderDropIndex ctx stmt)

/-- Execute an ALTER TABLE statement -/
def execAlterTable (db : Database) (stmt : Chisel.AlterTableStmt)
    (ctx : Chisel.RenderContext := sqliteContext) : IO Unit :=
  db.exec (Chisel.renderAlterTable ctx stmt)

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

end Database

end Quarry
