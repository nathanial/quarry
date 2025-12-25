/-
  Quarry.FFI.Statement
  Low-level FFI bindings for sqlite3_stmt operations
-/
import Quarry.FFI.Types

namespace Quarry.FFI

-- Statement lifecycle
@[extern "quarry_stmt_prepare"]
opaque stmtPrepare (db : @& Database) (sql : @& String) : IO Statement

@[extern "quarry_stmt_finalize"]
opaque stmtFinalize (stmt : @& Statement) : IO Unit

@[extern "quarry_stmt_reset"]
opaque stmtReset (stmt : @& Statement) : IO Unit

@[extern "quarry_stmt_clear_bindings"]
opaque stmtClearBindings (stmt : @& Statement) : IO Unit

-- Execution
@[extern "quarry_stmt_step"]
opaque stmtStep (stmt : @& Statement) : IO Int

-- Positional binding (?1, ?2, ...)
@[extern "quarry_stmt_bind_null"]
opaque stmtBindNull (stmt : @& Statement) (idx : UInt32) : IO Unit

@[extern "quarry_stmt_bind_int"]
opaque stmtBindInt (stmt : @& Statement) (idx : UInt32) (value : Int) : IO Unit

@[extern "quarry_stmt_bind_double"]
opaque stmtBindDouble (stmt : @& Statement) (idx : UInt32) (value : Float) : IO Unit

@[extern "quarry_stmt_bind_text"]
opaque stmtBindText (stmt : @& Statement) (idx : UInt32) (value : @& String) : IO Unit

@[extern "quarry_stmt_bind_blob"]
opaque stmtBindBlob (stmt : @& Statement) (idx : UInt32) (value : @& ByteArray) : IO Unit

-- Named binding (:name, @name, $name)
@[extern "quarry_stmt_bind_parameter_index"]
opaque stmtBindParameterIndex (stmt : @& Statement) (name : @& String) : IO Int

@[extern "quarry_stmt_bind_parameter_count"]
opaque stmtBindParameterCount (stmt : @& Statement) : IO UInt32

-- Column access
@[extern "quarry_stmt_column_count"]
opaque stmtColumnCount (stmt : @& Statement) : IO UInt32

@[extern "quarry_stmt_column_type"]
opaque stmtColumnType (stmt : @& Statement) (idx : UInt32) : IO Int

@[extern "quarry_stmt_column_name"]
opaque stmtColumnName (stmt : @& Statement) (idx : UInt32) : IO String

-- Column metadata (source table/column information)
@[extern "quarry_stmt_column_database_name"]
opaque stmtColumnDatabaseName (stmt : @& Statement) (idx : UInt32) : IO (Option String)

@[extern "quarry_stmt_column_table_name"]
opaque stmtColumnTableName (stmt : @& Statement) (idx : UInt32) : IO (Option String)

@[extern "quarry_stmt_column_origin_name"]
opaque stmtColumnOriginName (stmt : @& Statement) (idx : UInt32) : IO (Option String)

@[extern "quarry_stmt_column_int"]
opaque stmtColumnInt (stmt : @& Statement) (idx : UInt32) : IO Int

@[extern "quarry_stmt_column_double"]
opaque stmtColumnDouble (stmt : @& Statement) (idx : UInt32) : IO Float

@[extern "quarry_stmt_column_text"]
opaque stmtColumnText (stmt : @& Statement) (idx : UInt32) : IO String

@[extern "quarry_stmt_column_blob"]
opaque stmtColumnBlob (stmt : @& Statement) (idx : UInt32) : IO ByteArray

@[extern "quarry_stmt_column_bytes"]
opaque stmtColumnBytes (stmt : @& Statement) (idx : UInt32) : IO Int

-- SQL text
@[extern "quarry_stmt_sql"]
opaque stmtSql (stmt : @& Statement) : IO String

end Quarry.FFI
