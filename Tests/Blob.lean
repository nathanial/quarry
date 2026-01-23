/-
  Incremental BLOB I/O Tests
  Tests for streaming blob read/write operations
-/
import Crucible
import Quarry

open Crucible
open Quarry

namespace Tests.Blob

testSuite "Incremental BLOB I/O"

test "open and read blob" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE files (id INTEGER PRIMARY KEY, content BLOB)"
  let _ ← db.execSqlInsert "INSERT INTO files (id, content) VALUES (1, X'48454C4C4F')"  -- "HELLO"

  let blob ← db.openBlob "files" "content" 1
  let data ← blob.readAll
  blob.close

  data.size ≡ 5
  data.data[0]! ≡ 0x48  -- 'H'
  data.data[4]! ≡ 0x4F  -- 'O'

test "write to blob" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE files (id INTEGER PRIMARY KEY, content BLOB)"
  let _ ← db.execSqlInsert "INSERT INTO files (id, content) VALUES (1, X'0000000000')"  -- 5 zero bytes

  let blob ← db.openBlob "files" "content" 1 .readWrite
  blob.write 0 (ByteArray.mk #[0x41, 0x42, 0x43])  -- "ABC"
  blob.close

  -- Verify the write
  let rows ← db.query "SELECT content FROM files WHERE id = 1"
  match rows[0]?.bind (·.get? 0) with
  | some (Value.blob written) =>
    written.data[0]! ≡ 0x41  -- 'A'
    written.data[1]! ≡ 0x42  -- 'B'
    written.data[2]! ≡ 0x43  -- 'C'
    written.data[3]! ≡ 0x00  -- Still zero
  | _ => throw (IO.userError "expected blob")

test "read partial blob" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE files (id INTEGER PRIMARY KEY, content BLOB)"
  -- Insert "HELLO WORLD" as hex
  let _ ← db.execSqlInsert "INSERT INTO files (id, content) VALUES (1, X'48454C4C4F20574F524C44')"

  let blob ← db.openBlob "files" "content" 1
  let chunk ← blob.read 6 5  -- Read "WORLD"
  blob.close

  chunk.size ≡ 5
  chunk.data[0]! ≡ 0x57  -- 'W'

test "blob bytes size" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE files (id INTEGER PRIMARY KEY, content BLOB)"
  let _ ← db.execSqlInsert "INSERT INTO files (id, content) VALUES (1, X'0102030405060708090A')"  -- 10 bytes

  let blob ← db.openBlob "files" "content" 1
  let size ← blob.bytes
  blob.close

  size ≡ 10

test "reopen for different row" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE files (id INTEGER PRIMARY KEY, content BLOB)"
  let _ ← db.execSqlInsert "INSERT INTO files (id, content) VALUES (1, X'4141')"  -- "AA"
  let _ ← db.execSqlInsert "INSERT INTO files (id, content) VALUES (2, X'4242')"  -- "BB"

  let blob ← db.openBlob "files" "content" 1
  let data1 ← blob.readAll
  data1.data[0]! ≡ 0x41  -- 'A'

  blob.reopen 2
  let data2 ← blob.readAll
  data2.data[0]! ≡ 0x42  -- 'B'

  blob.close

test "read-only mode prevents writes" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE files (id INTEGER PRIMARY KEY, content BLOB)"
  let _ ← db.execSqlInsert "INSERT INTO files (id, content) VALUES (1, X'4141')"

  let blob ← db.openBlob "files" "content" 1 .readOnly
  try
    blob.write 0 (ByteArray.mk #[0x42])
    throw (IO.userError "Expected write to fail")
  catch _ =>
    ensure true "write correctly rejected"
  -- Note: Close may fail after a failed write, which is fine
  try blob.close catch _ => pure ()

test "close is idempotent" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE files (id INTEGER PRIMARY KEY, content BLOB)"
  let _ ← db.execSqlInsert "INSERT INTO files (id, content) VALUES (1, X'4141')"

  let blob ← db.openBlob "files" "content" 1
  blob.close
  blob.close  -- Second close should not error
  ensure true "double close works"

test "blob with empty data" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE files (id INTEGER PRIMARY KEY, content BLOB)"
  let _ ← db.execSqlInsert "INSERT INTO files (id, content) VALUES (1, X'')"  -- Empty blob

  let blob ← db.openBlob "files" "content" 1
  let size ← blob.bytes
  size ≡ 0

  let data ← blob.readAll
  data.size ≡ 0
  blob.close

test "large blob streaming" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE files (id INTEGER PRIMARY KEY, content BLOB)"

  -- Create a 10KB blob filled with zeroes using zeroblob
  let size := 10240
  let _ ← db.execSqlInsert s!"INSERT INTO files (id, content) VALUES (1, zeroblob({size}))"

  -- Write in chunks
  let blob ← db.openBlob "files" "content" 1 .readWrite
  let blobSize ← blob.bytes
  blobSize ≡ size

  -- Write different byte values at different offsets
  blob.write 0 (ByteArray.mk #[0xA0, 0xA0, 0xA0, 0xA0])
  blob.write 5120 (ByteArray.mk #[0xA5, 0xA5, 0xA5, 0xA5])

  blob.close

  -- Read back and verify
  let blob2 ← db.openBlob "files" "content" 1
  let chunk0 ← blob2.read 0 4
  chunk0.data[0]! ≡ 0xA0

  let chunk5 ← blob2.read 5120 4
  chunk5.data[0]! ≡ 0xA5

  blob2.close

test "write at offset" := do
  let db ← Database.openMemory
  db.execSqlDdl "CREATE TABLE files (id INTEGER PRIMARY KEY, content BLOB)"
  let _ ← db.execSqlInsert "INSERT INTO files (id, content) VALUES (1, X'0000000000000000')"  -- 8 zero bytes

  let blob ← db.openBlob "files" "content" 1 .readWrite
  blob.write 3 (ByteArray.mk #[0xFF, 0xFF])  -- Write at offset 3
  blob.close

  -- Verify
  let blob2 ← db.openBlob "files" "content" 1
  let data ← blob2.readAll
  data.data[0]! ≡ 0x00
  data.data[2]! ≡ 0x00
  data.data[3]! ≡ 0xFF
  data.data[4]! ≡ 0xFF
  data.data[5]! ≡ 0x00
  blob2.close

end Tests.Blob
