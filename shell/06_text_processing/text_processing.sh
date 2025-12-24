#!/bin/bash
# Text Processing Demo
set -e

echo "=== TEXT PROCESSING DEMO ==="

# Sample data
DATA="id,name,value,status
1,Alice,100,active
2,Bob,200,inactive
3,Charlie,150,active
4,Diana,300,active
5,Eve,250,inactive"

echo "Sample Data:"
echo "$DATA"
echo ""

# grep - filter rows
echo "Active users (grep):"
echo "$DATA" | grep "active"
echo ""

# awk - select columns
echo "Names only (awk):"
echo "$DATA" | awk -F',' 'NR>1 {print $2}'
echo ""

# awk - calculate sum
echo "Sum of values (awk):"
echo "$DATA" | awk -F',' 'NR>1 {sum+=$3} END {print "Total:", sum}'
echo ""

# sed - replace text
echo "Replace active with ACTIVE (sed):"
echo "$DATA" | sed 's/active/ACTIVE/g'
echo ""

# cut + sort + uniq - unique values
echo "Unique statuses:"
echo "$DATA" | tail -n +2 | cut -d',' -f4 | sort | uniq -c
echo ""

echo "=== DEMO COMPLETE ==="
