#!/bin/bash
# Build Quarry with vendored SQLite amalgamation

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

SQLITE_VERSION="3470200"
SQLITE_YEAR="2024"
SQLITE_DIR="native/sqlite"

# Download SQLite amalgamation if not present
if [ ! -f "$SQLITE_DIR/sqlite3.c" ]; then
    echo "Downloading SQLite amalgamation..."
    mkdir -p "$SQLITE_DIR"
    SQLITE_URL="https://www.sqlite.org/${SQLITE_YEAR}/sqlite-amalgamation-${SQLITE_VERSION}.zip"

    curl -L -o /tmp/sqlite.zip "$SQLITE_URL"
    unzip -j /tmp/sqlite.zip -d "$SQLITE_DIR"
    rm /tmp/sqlite.zip

    echo "SQLite amalgamation downloaded!"
fi

# Build the specified target
TARGET="${1:-Quarry}"

echo "Building $TARGET..."
lake build "$TARGET"

if [ "$TARGET" = "Quarry" ]; then
    echo "Building quarry_native..."
    lake build quarry_native
fi

echo "Build complete!"
