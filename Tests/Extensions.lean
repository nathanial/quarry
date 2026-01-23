/-
  SQLite Extension Tests
  Tests for FTS5 (Full-Text Search) and R-Tree (Spatial Indexing)
-/
import Crucible
import Quarry
import Staple

open Crucible
open Quarry
open Staple (String.containsSubstr)

namespace Tests.FTS5

testSuite "Full-Text Search (FTS5)"

test "create FTS5 table" := do
  let db ← Database.openMemory
  db.execRaw "CREATE VIRTUAL TABLE docs USING fts5(title, body)"
  -- Table exists if we can query it
  let rows ← db.query "SELECT * FROM docs"
  rows.size ≡ 0

test "insert and search" := do
  let db ← Database.openMemory
  db.execRaw "CREATE VIRTUAL TABLE docs USING fts5(title, body)"
  db.execRaw "INSERT INTO docs VALUES ('First Post', 'Hello world, this is my first blog post')"
  db.execRaw "INSERT INTO docs VALUES ('Second Post', 'Another day, another post about programming')"
  db.execRaw "INSERT INTO docs VALUES ('Recipe', 'How to make chocolate cake')"

  -- Search for 'post'
  let rows ← db.query "SELECT title FROM docs WHERE docs MATCH 'post'"
  rows.size ≡ 2

test "phrase search" := do
  let db ← Database.openMemory
  db.execRaw "CREATE VIRTUAL TABLE docs USING fts5(content)"
  db.execRaw "INSERT INTO docs VALUES ('the quick brown fox')"
  db.execRaw "INSERT INTO docs VALUES ('quick fox jumps')"
  db.execRaw "INSERT INTO docs VALUES ('the lazy dog')"

  -- Phrase search with quotes
  let rows ← db.query "SELECT * FROM docs WHERE docs MATCH '\"quick brown\"'"
  rows.size ≡ 1

test "prefix search" := do
  let db ← Database.openMemory
  db.execRaw "CREATE VIRTUAL TABLE docs USING fts5(word)"
  db.execRaw "INSERT INTO docs VALUES ('programming')"
  db.execRaw "INSERT INTO docs VALUES ('program')"
  db.execRaw "INSERT INTO docs VALUES ('progress')"
  db.execRaw "INSERT INTO docs VALUES ('project')"

  -- Prefix search with *
  let rows ← db.query "SELECT * FROM docs WHERE docs MATCH 'prog*'"
  rows.size ≡ 3

test "boolean operators" := do
  let db ← Database.openMemory
  db.execRaw "CREATE VIRTUAL TABLE docs USING fts5(content)"
  db.execRaw "INSERT INTO docs VALUES ('apple banana')"
  db.execRaw "INSERT INTO docs VALUES ('apple orange')"
  db.execRaw "INSERT INTO docs VALUES ('banana orange')"

  -- AND (default)
  let andRows ← db.query "SELECT * FROM docs WHERE docs MATCH 'apple banana'"
  andRows.size ≡ 1

  -- OR
  let orRows ← db.query "SELECT * FROM docs WHERE docs MATCH 'apple OR orange'"
  orRows.size ≡ 3

  -- NOT
  let notRows ← db.query "SELECT * FROM docs WHERE docs MATCH 'apple NOT banana'"
  notRows.size ≡ 1

test "column filter" := do
  let db ← Database.openMemory
  db.execRaw "CREATE VIRTUAL TABLE docs USING fts5(title, body)"
  db.execRaw "INSERT INTO docs VALUES ('Apple News', 'Read about oranges today')"
  db.execRaw "INSERT INTO docs VALUES ('Orange News', 'Read about apples today')"

  -- Search only in title column
  let rows ← db.query "SELECT * FROM docs WHERE docs MATCH 'title:apple'"
  rows.size ≡ 1

test "ranking with bm25" := do
  let db ← Database.openMemory
  db.execRaw "CREATE VIRTUAL TABLE docs USING fts5(content)"
  db.execRaw "INSERT INTO docs VALUES ('apple apple apple')"
  db.execRaw "INSERT INTO docs VALUES ('apple')"
  db.execRaw "INSERT INTO docs VALUES ('apple apple')"

  -- Order by relevance (bm25 returns negative scores, more negative = more relevant)
  let rows ← db.query "SELECT content, bm25(docs) as score FROM docs WHERE docs MATCH 'apple' ORDER BY score"
  rows.size ≡ 3
  -- First row should have the most apples
  match rows[0]? with
  | some row =>
    match row.get? 0 with
    | some (.text content) => ensure (String.containsSubstr content "apple apple apple") "most relevant first"
    | _ => throw (IO.userError "unexpected value")
  | none => throw (IO.userError "no row")

test "highlight function" := do
  let db ← Database.openMemory
  db.execRaw "CREATE VIRTUAL TABLE docs USING fts5(content)"
  db.execRaw "INSERT INTO docs VALUES ('The quick brown fox jumps over')"

  let rows ← db.query "SELECT highlight(docs, 0, '<b>', '</b>') FROM docs WHERE docs MATCH 'quick'"
  rows.size ≡ 1
  match rows[0]? with
  | some row =>
    match row.get? 0 with
    | some (.text highlighted) =>
      ensure (String.containsSubstr highlighted "<b>quick</b>") "should contain highlighted term"
    | _ => throw (IO.userError "unexpected value")
  | none => throw (IO.userError "no row")

test "snippet function" := do
  let db ← Database.openMemory
  db.execRaw "CREATE VIRTUAL TABLE docs USING fts5(content)"
  db.execRaw "INSERT INTO docs VALUES ('This is a very long document with many words and somewhere in the middle we mention programming which is the term we will search for')"

  let rows ← db.query "SELECT snippet(docs, 0, '[', ']', '...', 10) FROM docs WHERE docs MATCH 'programming'"
  rows.size ≡ 1
  match rows[0]? with
  | some row =>
    match row.get? 0 with
    | some (.text snippet) =>
      ensure (String.containsSubstr snippet "[programming]") "should contain marked term"
    | _ => throw (IO.userError "unexpected value")
  | none => throw (IO.userError "no row")

test "delete from FTS table" := do
  let db ← Database.openMemory
  db.execRaw "CREATE VIRTUAL TABLE docs USING fts5(content)"
  db.execRaw "INSERT INTO docs VALUES ('first')"
  db.execRaw "INSERT INTO docs VALUES ('second')"

  let before ← db.query "SELECT * FROM docs"
  before.size ≡ 2

  db.execRaw "DELETE FROM docs WHERE content = 'first'"

  let after ← db.query "SELECT * FROM docs"
  after.size ≡ 1

test "update FTS table" := do
  let db ← Database.openMemory
  db.execRaw "CREATE VIRTUAL TABLE docs USING fts5(content)"
  db.execRaw "INSERT INTO docs VALUES ('original')"

  db.execRaw "UPDATE docs SET content = 'modified' WHERE content = 'original'"

  let rows ← db.query "SELECT * FROM docs WHERE docs MATCH 'modified'"
  rows.size ≡ 1

end Tests.FTS5

namespace Tests.RTree

testSuite "R-Tree (Spatial Indexing)"

test "create R-Tree table" := do
  let db ← Database.openMemory
  -- R-Tree with 2D coordinates (minX, maxX, minY, maxY)
  db.execRaw "CREATE VIRTUAL TABLE spatial USING rtree(id, minX, maxX, minY, maxY)"
  let rows ← db.query "SELECT * FROM spatial"
  rows.size ≡ 0

test "insert and query points" := do
  let db ← Database.openMemory
  db.execRaw "CREATE VIRTUAL TABLE locations USING rtree(id, minX, maxX, minY, maxY)"

  -- Insert some points (for points, minX=maxX and minY=maxY)
  db.execRaw "INSERT INTO locations VALUES (1, 10.0, 10.0, 20.0, 20.0)"  -- Point at (10, 20)
  db.execRaw "INSERT INTO locations VALUES (2, 30.0, 30.0, 40.0, 40.0)"  -- Point at (30, 40)
  db.execRaw "INSERT INTO locations VALUES (3, 50.0, 50.0, 60.0, 60.0)"  -- Point at (50, 60)

  let rows ← db.query "SELECT id FROM locations"
  rows.size ≡ 3

test "bounding box query" := do
  let db ← Database.openMemory
  db.execRaw "CREATE VIRTUAL TABLE geo USING rtree(id, minX, maxX, minY, maxY)"

  -- Insert rectangles
  db.execRaw "INSERT INTO geo VALUES (1, 0, 10, 0, 10)"    -- Box at origin
  db.execRaw "INSERT INTO geo VALUES (2, 20, 30, 20, 30)"  -- Box at (20,20)
  db.execRaw "INSERT INTO geo VALUES (3, 5, 15, 5, 15)"    -- Overlaps with box 1

  -- Query for boxes that intersect with (0,0)-(12,12)
  let rows ← db.query "SELECT id FROM geo WHERE minX <= 12 AND maxX >= 0 AND minY <= 12 AND maxY >= 0"
  rows.size ≡ 2  -- Boxes 1 and 3

test "contains query" := do
  let db ← Database.openMemory
  db.execRaw "CREATE VIRTUAL TABLE areas USING rtree(id, minX, maxX, minY, maxY)"

  -- Insert areas
  db.execRaw "INSERT INTO areas VALUES (1, 0, 100, 0, 100)"   -- Large area
  db.execRaw "INSERT INTO areas VALUES (2, 25, 75, 25, 75)"   -- Medium area inside large
  db.execRaw "INSERT INTO areas VALUES (3, 40, 60, 40, 60)"   -- Small area inside medium
  db.execRaw "INSERT INTO areas VALUES (4, 200, 300, 200, 300)" -- Separate area

  -- Find areas that contain point (50, 50)
  let rows ← db.query "SELECT id FROM areas WHERE minX <= 50 AND maxX >= 50 AND minY <= 50 AND maxY >= 50 ORDER BY id"
  rows.size ≡ 3  -- Areas 1, 2, and 3 contain (50, 50)

test "3D R-Tree" := do
  let db ← Database.openMemory
  -- R-Tree with 3D coordinates
  db.execRaw "CREATE VIRTUAL TABLE space3d USING rtree(id, minX, maxX, minY, maxY, minZ, maxZ)"

  db.execRaw "INSERT INTO space3d VALUES (1, 0, 10, 0, 10, 0, 10)"
  db.execRaw "INSERT INTO space3d VALUES (2, 5, 15, 5, 15, 5, 15)"

  let rows ← db.query "SELECT id FROM space3d"
  rows.size ≡ 2

test "nearest neighbor simulation" := do
  let db ← Database.openMemory
  db.execRaw "CREATE VIRTUAL TABLE points USING rtree(id, minX, maxX, minY, maxY)"

  -- Insert some points
  db.execRaw "INSERT INTO points VALUES (1, 0, 0, 0, 0)"      -- Origin
  db.execRaw "INSERT INTO points VALUES (2, 10, 10, 10, 10)"  -- (10, 10)
  db.execRaw "INSERT INTO points VALUES (3, 3, 3, 4, 4)"      -- (3, 4) - closest to (5,5)
  db.execRaw "INSERT INTO points VALUES (4, 100, 100, 100, 100)" -- Far away

  -- Find points within distance 10 of (5, 5), ordered by distance
  -- Using simple distance approximation
  let rows ← db.query "
    SELECT id,
           (minX - 5) * (minX - 5) + (minY - 5) * (minY - 5) as dist_sq
    FROM points
    WHERE minX >= -5 AND maxX <= 15 AND minY >= -5 AND maxY <= 15
    ORDER BY dist_sq
    LIMIT 3
  "
  rows.size ≡ 3

test "delete from R-Tree" := do
  let db ← Database.openMemory
  db.execRaw "CREATE VIRTUAL TABLE boxes USING rtree(id, minX, maxX, minY, maxY)"

  db.execRaw "INSERT INTO boxes VALUES (1, 0, 10, 0, 10)"
  db.execRaw "INSERT INTO boxes VALUES (2, 20, 30, 20, 30)"

  let before ← db.query "SELECT * FROM boxes"
  before.size ≡ 2

  db.execRaw "DELETE FROM boxes WHERE id = 1"

  let after ← db.query "SELECT * FROM boxes"
  after.size ≡ 1

test "update R-Tree" := do
  let db ← Database.openMemory
  db.execRaw "CREATE VIRTUAL TABLE boxes USING rtree(id, minX, maxX, minY, maxY)"

  db.execRaw "INSERT INTO boxes VALUES (1, 0, 10, 0, 10)"

  -- Move the box
  db.execRaw "UPDATE boxes SET minX = 50, maxX = 60, minY = 50, maxY = 60 WHERE id = 1"

  -- Should no longer be at origin
  let atOrigin ← db.query "SELECT id FROM boxes WHERE minX <= 5 AND maxX >= 5 AND minY <= 5 AND maxY >= 5"
  atOrigin.size ≡ 0

  -- Should be at new location
  let atNew ← db.query "SELECT id FROM boxes WHERE minX <= 55 AND maxX >= 55 AND minY <= 55 AND maxY >= 55"
  atNew.size ≡ 1

test "R-Tree with auxiliary columns" := do
  let db ← Database.openMemory
  -- R-Tree with auxiliary data column (prefixed with +)
  db.execRaw "CREATE VIRTUAL TABLE places USING rtree(id, minX, maxX, minY, maxY, +name TEXT, +category TEXT)"

  db.execRaw "INSERT INTO places VALUES (1, -122.4, -122.4, 37.8, 37.8, 'San Francisco', 'city')"
  db.execRaw "INSERT INTO places VALUES (2, -118.2, -118.2, 34.0, 34.0, 'Los Angeles', 'city')"
  db.execRaw "INSERT INTO places VALUES (3, -73.9, -73.9, 40.7, 40.7, 'New York', 'city')"

  -- Query with auxiliary columns
  let rows ← db.query "SELECT name, category FROM places WHERE minX < -100"
  rows.size ≡ 2  -- SF and LA are west of -100

test "join R-Tree with regular table" := do
  let db ← Database.openMemory

  -- Create R-Tree for spatial data
  db.execRaw "CREATE VIRTUAL TABLE geo USING rtree(id, minX, maxX, minY, maxY)"

  -- Create regular table for metadata
  db.execSqlDdl "CREATE TABLE metadata (id INTEGER PRIMARY KEY, name TEXT, population INTEGER)"

  -- Insert data
  db.execRaw "INSERT INTO geo VALUES (1, 0, 10, 0, 10)"
  db.execRaw "INSERT INTO geo VALUES (2, 20, 30, 20, 30)"
  let _ ← db.execSqlInsert "INSERT INTO metadata VALUES (1, 'Area A', 1000)"
  let _ ← db.execSqlInsert "INSERT INTO metadata VALUES (2, 'Area B', 2000)"

  -- Join query
  let rows ← db.query "
    SELECT m.name, m.population
    FROM geo g
    JOIN metadata m ON g.id = m.id
    WHERE g.minX <= 5 AND g.maxX >= 5
  "
  rows.size ≡ 1
  match rows[0]? with
  | some row =>
    match row.get? 0 with
    | some (.text "Area A") => ensure true "correct area"
    | _ => throw (IO.userError "unexpected value")
  | none => throw (IO.userError "no row")

end Tests.RTree
