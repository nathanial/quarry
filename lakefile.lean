import Lake
open Lake DSL System

package quarry where
  version := v!"0.1.0"
  precompileModules := true

require crucible from git "https://github.com/nathanial/crucible" @ "v0.0.3"
require staple from git "https://github.com/nathanial/staple" @ "v0.0.2"
require chisel from git "https://github.com/nathanial/chisel" @ "v0.0.3"

@[default_target]
lean_lib Quarry where
  roots := #[`Quarry]

lean_lib Tests where
  roots := #[`Tests]

@[test_driver]
lean_exe quarry_tests where
  root := `Tests.Main

-- Compile sqlite3.c (amalgamation)
target sqlite3_o pkg : FilePath := do
  let oFile := pkg.buildDir / "native" / "sqlite3.o"
  let srcFile := pkg.dir / "native" / "sqlite" / "sqlite3.c"
  let includeDir := pkg.dir / "native" / "sqlite"
  buildO oFile (← inputTextFile srcFile) #[
    "-I", includeDir.toString,
    "-DSQLITE_THREADSAFE=0",
    "-DSQLITE_OMIT_LOAD_EXTENSION",
    "-DSQLITE_DEFAULT_MEMSTATUS=0",
    "-DSQLITE_ENABLE_COLUMN_METADATA",  -- Required for column origin info
    "-DSQLITE_ENABLE_FTS5",             -- Full-text search
    "-DSQLITE_ENABLE_RTREE",            -- R-Tree spatial indexing
    "-fPIC",
    "-O2"
  ] #[] "cc" getLeanTrace

-- FFI bridge
target quarry_ffi_o pkg : FilePath := do
  let oFile := pkg.buildDir / "native" / "quarry_ffi.o"
  let srcFile := pkg.dir / "native" / "src" / "quarry_ffi.c"
  let sqliteInclude := pkg.dir / "native" / "sqlite"
  let leanIncludeDir ← getLeanIncludeDir
  buildO oFile (← inputTextFile srcFile) #[
    "-I", leanIncludeDir.toString,
    "-I", sqliteInclude.toString,
    "-fPIC",
    "-O2"
  ] #[] "cc" getLeanTrace

extern_lib quarry_native pkg := do
  let name := nameToStaticLib "quarry_native"
  let sqlite3O ← sqlite3_o.fetch
  let ffiO ← quarry_ffi_o.fetch
  buildStaticLib (pkg.buildDir / "lib" / name) #[sqlite3O, ffiO]
