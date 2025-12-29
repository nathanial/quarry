/-
  Quarry.Transaction
  Transaction management utilities
-/
import Quarry.Database

namespace Quarry

/-- Savepoint for nested transactions -/
structure Savepoint where
  name : String
  db : Database

namespace Savepoint

/-- Release the savepoint (commit nested transaction) -/
def release (sp : Savepoint) : IO Unit :=
  sp.db.execRaw s!"RELEASE SAVEPOINT {sp.name}"

/-- Rollback to savepoint -/
def rollback (sp : Savepoint) : IO Unit :=
  sp.db.execRaw s!"ROLLBACK TO SAVEPOINT {sp.name}"

end Savepoint

namespace Database

/-- Create a savepoint for nested transaction -/
def savepoint (db : Database) (name : String) : IO Savepoint := do
  db.execRaw s!"SAVEPOINT {name}"
  return ⟨name, db⟩

/-- Run a nested transaction with automatic rollback on error -/
def withSavepoint (db : Database) (name : String) (f : IO α) : IO α := do
  let sp ← db.savepoint name
  try
    let result ← f
    sp.release
    return result
  catch e =>
    sp.rollback
    throw e

/-- Run read-only transaction (BEGIN DEFERRED) -/
def readTransaction (db : Database) (f : IO α) : IO α := do
  db.execRaw "BEGIN DEFERRED"
  try
    let result ← f
    db.execRaw "COMMIT"
    return result
  catch e =>
    db.execRaw "ROLLBACK"
    throw e

/-- Run immediate write transaction (BEGIN IMMEDIATE) -/
def writeTransaction (db : Database) (f : IO α) : IO α := do
  db.execRaw "BEGIN IMMEDIATE"
  try
    let result ← f
    db.execRaw "COMMIT"
    return result
  catch e =>
    db.execRaw "ROLLBACK"
    throw e

/-- Run exclusive transaction (BEGIN EXCLUSIVE) -/
def exclusiveTransaction (db : Database) (f : IO α) : IO α := do
  db.execRaw "BEGIN EXCLUSIVE"
  try
    let result ← f
    db.execRaw "COMMIT"
    return result
  catch e =>
    db.execRaw "ROLLBACK"
    throw e

end Database

end Quarry
