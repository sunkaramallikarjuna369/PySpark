#!/bin/bash
# Log Processing Demo
set -euo pipefail

echo "=== LOG PROCESSING DEMO ==="

# Create sample log data
SAMPLE_LOG="2024-01-01 10:00:00 [INFO] Application started
2024-01-01 10:00:01 [INFO] User john logged in
2024-01-01 10:00:02 [WARN] High memory usage detected
2024-01-01 10:00:03 [ERROR] Database connection failed
2024-01-01 10:00:04 [INFO] Retrying connection
2024-01-01 10:00:05 [INFO] Connection established
2024-01-01 10:00:06 [ERROR] Query timeout
2024-01-01 10:00:07 [INFO] User jane logged in
2024-01-01 10:00:08 [WARN] Slow query detected
2024-01-01 10:00:09 [INFO] Processing complete"

echo "Sample Log:"
echo "$SAMPLE_LOG"
echo ""

# Count by log level
echo "1. Log Level Distribution:"
echo "$SAMPLE_LOG" | grep -oE "\[(INFO|WARN|ERROR)\]" | sort | uniq -c
echo ""

# Extract errors
echo "2. Error Messages:"
echo "$SAMPLE_LOG" | grep "ERROR"
echo ""

# Count errors
ERROR_COUNT=$(echo "$SAMPLE_LOG" | grep -c "ERROR")
TOTAL_COUNT=$(echo "$SAMPLE_LOG" | wc -l)
echo "3. Error Rate:"
echo "  Total lines: $TOTAL_COUNT"
echo "  Errors: $ERROR_COUNT"
echo "  Rate: $(echo "scale=1; $ERROR_COUNT * 100 / $TOTAL_COUNT" | bc)%"
echo ""

# Extract users
echo "4. User Activity:"
echo "$SAMPLE_LOG" | grep -oE "User [a-z]+" | sort | uniq -c
echo ""

echo "=== DEMO COMPLETE ==="
