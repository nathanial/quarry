/-
  Quarry - SQLite Library for Lean 4

  A SQLite library using the amalgamated source with no system dependencies.

  ## Quick Start

  ```lean
  import Quarry

  def main : IO Unit := do
    -- Open in-memory database
    let db ← Quarry.Database.openMemory

    -- Create a table
    db.exec "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"

    -- Insert data
    db.exec "INSERT INTO users (name, age) VALUES ('Alice', 30)"
    db.exec "INSERT INTO users (name, age) VALUES ('Bob', 25)"

    -- Query data
    let rows ← db.query "SELECT * FROM users"
    for row in rows do
      IO.println row

    -- Use transactions
    db.transaction do
      db.exec "UPDATE users SET age = age + 1"
  ```
-/

import Quarry.Core.Error
import Quarry.Core.Value
import Quarry.Core.Row
import Quarry.Core.Column
import Quarry.FFI.Types
import Quarry.FFI.Database
import Quarry.FFI.Statement
import Quarry.FFI.Backup
import Quarry.FFI.Blob
import Quarry.Database
import Quarry.Backup
import Quarry.Blob
import Quarry.Bind
import Quarry.Extract
import Quarry.Transaction
import Quarry.Function
import Quarry.VirtualTable
import Quarry.VirtualTable.Array
import Quarry.VirtualTable.Generator
import Quarry.Hook
import Quarry.Serialize
import Quarry.Chisel.Convert
import Quarry.Chisel.Execute
