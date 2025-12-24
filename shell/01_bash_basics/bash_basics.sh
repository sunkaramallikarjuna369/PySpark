#!/bin/bash
# ============================================
# Bash Basics - Shell Scripting Learning Hub
# ============================================
# This script demonstrates fundamental bash concepts
# for data engineering workflows.

set -e  # Exit on error
set -u  # Exit on undefined variable

# ============================================
# SCRIPT INFORMATION
# ============================================
SCRIPT_NAME="Bash Basics Demo"
SCRIPT_VERSION="1.0"
SCRIPT_DATE=$(date +"%Y-%m-%d %H:%M:%S")

echo "============================================"
echo "$SCRIPT_NAME v$SCRIPT_VERSION"
echo "Started: $SCRIPT_DATE"
echo "============================================"

# ============================================
# 1. BASIC COMMANDS DEMONSTRATION
# ============================================
echo ""
echo "1. BASIC COMMANDS"
echo "----------------------------------------"

# Current directory
echo "Current directory: $(pwd)"

# List files
echo "Files in current directory:"
ls -la 2>/dev/null | head -5 || echo "  (directory listing)"

# System information
echo "System: $(uname -s)"
echo "User: $(whoami)"
echo "Hostname: $(hostname)"

# ============================================
# 2. VARIABLES
# ============================================
echo ""
echo "2. VARIABLES"
echo "----------------------------------------"

# String variables
NAME="Data Pipeline"
ENVIRONMENT="production"
echo "Name: $NAME"
echo "Environment: $ENVIRONMENT"

# Numeric variables
COUNT=10
DOUBLED=$((COUNT * 2))
echo "Count: $COUNT, Doubled: $DOUBLED"

# Command substitution
FILE_COUNT=$(ls -1 2>/dev/null | wc -l)
echo "Files in directory: $FILE_COUNT"

# Default values
UNSET_VAR=${UNDEFINED_VAR:-"default_value"}
echo "Default value example: $UNSET_VAR"

# ============================================
# 3. ARRAYS
# ============================================
echo ""
echo "3. ARRAYS"
echo "----------------------------------------"

# Indexed array
FRUITS=("apple" "banana" "cherry" "date")
echo "First fruit: ${FRUITS[0]}"
echo "All fruits: ${FRUITS[@]}"
echo "Number of fruits: ${#FRUITS[@]}"

# Associative array (bash 4+)
declare -A CONFIG
CONFIG[host]="localhost"
CONFIG[port]="5432"
CONFIG[database]="mydb"
echo "Database config: ${CONFIG[host]}:${CONFIG[port]}/${CONFIG[database]}"

# ============================================
# 4. STRING OPERATIONS
# ============================================
echo ""
echo "4. STRING OPERATIONS"
echo "----------------------------------------"

STRING="Hello, Data Engineering World!"

# Length
echo "String length: ${#STRING}"

# Substring
echo "Substring (0-5): ${STRING:0:5}"

# Replace
echo "Replace 'World' with 'Universe': ${STRING/World/Universe}"

# Uppercase/Lowercase (bash 4+)
echo "Uppercase: ${STRING^^}"
echo "Lowercase: ${STRING,,}"

# ============================================
# 5. ARITHMETIC
# ============================================
echo ""
echo "5. ARITHMETIC"
echo "----------------------------------------"

A=15
B=4

echo "A = $A, B = $B"
echo "Addition: $((A + B))"
echo "Subtraction: $((A - B))"
echo "Multiplication: $((A * B))"
echo "Division: $((A / B))"
echo "Modulo: $((A % B))"
echo "Power: $((A ** 2))"

# Floating point with bc
if command -v bc &> /dev/null; then
    RESULT=$(echo "scale=2; $A / $B" | bc)
    echo "Float division: $RESULT"
fi

# ============================================
# 6. CONDITIONALS
# ============================================
echo ""
echo "6. CONDITIONALS"
echo "----------------------------------------"

NUMBER=42

# Numeric comparison
if [ $NUMBER -gt 40 ]; then
    echo "$NUMBER is greater than 40"
fi

# String comparison
STATUS="active"
if [ "$STATUS" = "active" ]; then
    echo "Status is active"
fi

# File tests
SCRIPT_PATH="$0"
if [ -f "$SCRIPT_PATH" ]; then
    echo "Script file exists"
fi

if [ -x "$SCRIPT_PATH" ]; then
    echo "Script is executable"
fi

# ============================================
# 7. LOOPS
# ============================================
echo ""
echo "7. LOOPS"
echo "----------------------------------------"

# For loop with range
echo "Counting 1-5:"
for i in {1..5}; do
    echo "  $i"
done

# For loop with array
echo "Iterating fruits:"
for fruit in "${FRUITS[@]}"; do
    echo "  - $fruit"
done

# While loop
echo "While loop countdown:"
COUNTER=3
while [ $COUNTER -gt 0 ]; do
    echo "  $COUNTER..."
    COUNTER=$((COUNTER - 1))
done
echo "  Done!"

# ============================================
# 8. FUNCTIONS
# ============================================
echo ""
echo "8. FUNCTIONS"
echo "----------------------------------------"

# Simple function
greet() {
    local name="$1"
    echo "Hello, $name!"
}

greet "Data Engineer"

# Function with return value
calculate_sum() {
    local a=$1
    local b=$2
    echo $((a + b))
}

RESULT=$(calculate_sum 10 20)
echo "Sum of 10 + 20 = $RESULT"

# Function with multiple returns
get_file_info() {
    local file="$1"
    if [ -f "$file" ]; then
        echo "exists"
    else
        echo "not_found"
    fi
}

STATUS=$(get_file_info "$0")
echo "Script status: $STATUS"

# ============================================
# 9. WORKING WITH DATA FILES
# ============================================
echo ""
echo "9. WORKING WITH DATA FILES"
echo "----------------------------------------"

# Create sample data
SAMPLE_DATA="id,name,value
1,Alice,100
2,Bob,200
3,Charlie,150
4,Diana,300
5,Eve,250"

echo "Sample CSV data:"
echo "$SAMPLE_DATA"

# Process with pipeline
echo ""
echo "Processing pipeline (sum of values):"
echo "$SAMPLE_DATA" | tail -n +2 | cut -d',' -f3 | paste -sd+ | bc 2>/dev/null || echo "  Total: 1000"

# Count records
RECORD_COUNT=$(echo "$SAMPLE_DATA" | tail -n +2 | wc -l)
echo "Record count: $RECORD_COUNT"

# ============================================
# 10. ERROR HANDLING
# ============================================
echo ""
echo "10. ERROR HANDLING"
echo "----------------------------------------"

# Function with error handling
safe_divide() {
    local a=$1
    local b=$2
    
    if [ "$b" -eq 0 ]; then
        echo "Error: Division by zero" >&2
        return 1
    fi
    
    echo $((a / b))
    return 0
}

if RESULT=$(safe_divide 10 2); then
    echo "10 / 2 = $RESULT"
fi

if ! RESULT=$(safe_divide 10 0 2>/dev/null); then
    echo "Division by zero handled gracefully"
fi

# ============================================
# SUMMARY
# ============================================
echo ""
echo "============================================"
echo "BASH BASICS DEMONSTRATION COMPLETE"
echo "============================================"
echo "Topics covered:"
echo "  1. Basic commands"
echo "  2. Variables"
echo "  3. Arrays"
echo "  4. String operations"
echo "  5. Arithmetic"
echo "  6. Conditionals"
echo "  7. Loops"
echo "  8. Functions"
echo "  9. Data file processing"
echo "  10. Error handling"
echo ""
echo "Finished: $(date +"%Y-%m-%d %H:%M:%S")"
