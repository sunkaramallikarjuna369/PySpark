#!/bin/bash
# File Operations Demo
set -e

echo "=== FILE OPERATIONS DEMO ==="

# Create temp directory
TEMP_DIR=$(mktemp -d)
echo "Working in: $TEMP_DIR"

# Create sample files
echo "id,name,value" > "$TEMP_DIR/data.csv"
echo "1,Alice,100" >> "$TEMP_DIR/data.csv"
echo "2,Bob,200" >> "$TEMP_DIR/data.csv"

# Read file
echo "File contents:"
cat "$TEMP_DIR/data.csv"

# Count lines
echo "Line count: $(wc -l < "$TEMP_DIR/data.csv")"

# Copy file
cp "$TEMP_DIR/data.csv" "$TEMP_DIR/backup.csv"
echo "Backup created"

# List files
echo "Files in temp dir:"
ls -la "$TEMP_DIR"

# Cleanup
rm -rf "$TEMP_DIR"
echo "Cleaned up temp directory"

echo "=== DEMO COMPLETE ==="
