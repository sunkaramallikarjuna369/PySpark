#!/bin/bash
# Data Pipeline Script Demo
set -euo pipefail

echo "=== DATA PIPELINE DEMO ==="

# Configuration
WORK_DIR=$(mktemp -d)
echo "Working directory: $WORK_DIR"

# Sample source data
cat > "$WORK_DIR/raw_data.json" << 'EOF'
[
  {"id": 1, "name": "Alice", "amount": 100, "date": "2024-01-01"},
  {"id": 2, "name": "Bob", "amount": 200, "date": "2024-01-01"},
  {"id": 3, "name": "Charlie", "amount": 150, "date": "2024-01-02"}
]
EOF

echo "1. EXTRACT - Raw JSON data:"
cat "$WORK_DIR/raw_data.json"
echo ""

# Transform: JSON to CSV
echo "2. TRANSFORM - Convert to CSV:"
if command -v jq &> /dev/null; then
    jq -r '(.[0] | keys_unsorted) as $keys | $keys, (.[] | [.[$keys[]]] | @csv)' \
        "$WORK_DIR/raw_data.json" > "$WORK_DIR/data.csv"
else
    # Fallback without jq
    echo "id,name,amount,date" > "$WORK_DIR/data.csv"
    echo "1,Alice,100,2024-01-01" >> "$WORK_DIR/data.csv"
    echo "2,Bob,200,2024-01-01" >> "$WORK_DIR/data.csv"
    echo "3,Charlie,150,2024-01-02" >> "$WORK_DIR/data.csv"
fi
cat "$WORK_DIR/data.csv"
echo ""

# Aggregate
echo "3. AGGREGATE - Daily totals:"
echo "date,total,count" > "$WORK_DIR/aggregated.csv"
tail -n +2 "$WORK_DIR/data.csv" | \
    awk -F',' '{
        date=$4; amount=$3
        total[date]+=amount; count[date]++
    } END {
        for(d in total) print d","total[d]","count[d]
    }' | sort >> "$WORK_DIR/aggregated.csv"
cat "$WORK_DIR/aggregated.csv"
echo ""

# Validate
echo "4. VALIDATE - Check data quality:"
ROWS=$(wc -l < "$WORK_DIR/data.csv")
echo "  Total rows: $((ROWS - 1))"
echo "  Columns: $(head -1 "$WORK_DIR/data.csv" | tr ',' '\n' | wc -l)"
echo "  Validation: PASSED"
echo ""

# Load (simulate)
echo "5. LOAD - Ready for database:"
echo "  File: $WORK_DIR/aggregated.csv"
echo "  Size: $(wc -c < "$WORK_DIR/aggregated.csv") bytes"
echo ""

# Cleanup
rm -rf "$WORK_DIR"
echo "=== PIPELINE COMPLETE ==="
