#!/bin/bash
# ============================================
# Variables & Data Types - Shell Scripting Hub
# ============================================
# Comprehensive demonstration of bash variables

set -e
set -u

echo "============================================"
echo "VARIABLES & DATA TYPES DEMONSTRATION"
echo "============================================"

# ============================================
# 1. BASIC VARIABLES
# ============================================
echo ""
echo "1. BASIC VARIABLES"
echo "----------------------------------------"

# String variables
NAME="Data Engineer"
COMPANY="Tech Corp"
echo "Name: $NAME"
echo "Company: $COMPANY"

# Numeric variables
AGE=30
SALARY=85000
echo "Age: $AGE"
echo "Salary: $SALARY"

# Command substitution
CURRENT_DATE=$(date +"%Y-%m-%d")
CURRENT_TIME=$(date +"%H:%M:%S")
echo "Date: $CURRENT_DATE"
echo "Time: $CURRENT_TIME"

# ============================================
# 2. STRING OPERATIONS
# ============================================
echo ""
echo "2. STRING OPERATIONS"
echo "----------------------------------------"

TEXT="Hello, Data Engineering World!"

echo "Original: $TEXT"
echo "Length: ${#TEXT}"
echo "Substring [0:5]: ${TEXT:0:5}"
echo "Substring [7:]: ${TEXT:7}"
echo "Replace 'World' with 'Universe': ${TEXT/World/Universe}"
echo "Uppercase: ${TEXT^^}"
echo "Lowercase: ${TEXT,,}"

# ============================================
# 3. ARITHMETIC
# ============================================
echo ""
echo "3. ARITHMETIC OPERATIONS"
echo "----------------------------------------"

A=25
B=7

echo "A = $A, B = $B"
echo "Addition: $((A + B))"
echo "Subtraction: $((A - B))"
echo "Multiplication: $((A * B))"
echo "Division: $((A / B))"
echo "Modulo: $((A % B))"

# Floating point
if command -v bc &> /dev/null; then
    FLOAT=$(echo "scale=4; $A / $B" | bc)
    echo "Float division: $FLOAT"
fi

# ============================================
# 4. INDEXED ARRAYS
# ============================================
echo ""
echo "4. INDEXED ARRAYS"
echo "----------------------------------------"

COLORS=("red" "green" "blue" "yellow" "purple")

echo "All colors: ${COLORS[@]}"
echo "First color: ${COLORS[0]}"
echo "Third color: ${COLORS[2]}"
echo "Count: ${#COLORS[@]}"
echo "Indices: ${!COLORS[@]}"

# Iterate
echo "Iterating:"
for color in "${COLORS[@]}"; do
    echo "  - $color"
done

# ============================================
# 5. ASSOCIATIVE ARRAYS
# ============================================
echo ""
echo "5. ASSOCIATIVE ARRAYS"
echo "----------------------------------------"

declare -A DATABASE
DATABASE[host]="localhost"
DATABASE[port]="5432"
DATABASE[name]="analytics"
DATABASE[user]="admin"

echo "Database configuration:"
for key in "${!DATABASE[@]}"; do
    echo "  $key = ${DATABASE[$key]}"
done

# ============================================
# 6. DEFAULT VALUES
# ============================================
echo ""
echo "6. DEFAULT VALUES"
echo "----------------------------------------"

# Using defaults
echo "UNDEFINED with default: ${UNDEFINED_VAR:-'default_value'}"
echo "NAME with default: ${NAME:-'default_value'}"

# Assign if unset
NEW_VAR="${UNSET_VAR:=assigned_value}"
echo "Assigned value: $NEW_VAR"

# ============================================
# 7. SPECIAL VARIABLES
# ============================================
echo ""
echo "7. SPECIAL VARIABLES"
echo "----------------------------------------"

echo "Script name: $0"
echo "Process ID: $$"
echo "User: $USER"
echo "Home: $HOME"
echo "Shell: $SHELL"
echo "Random: $RANDOM"

# ============================================
# 8. PRACTICAL EXAMPLE
# ============================================
echo ""
echo "8. PRACTICAL EXAMPLE - Config Parser"
echo "----------------------------------------"

# Simulated config
CONFIG_DATA="
host=localhost
port=5432
database=mydb
timeout=30
"

declare -A CONFIG

while IFS='=' read -r key value; do
    # Skip empty lines and comments
    [[ -z "$key" || "$key" =~ ^# ]] && continue
    CONFIG[$key]="$value"
done <<< "$CONFIG_DATA"

echo "Parsed configuration:"
for key in "${!CONFIG[@]}"; do
    echo "  $key = ${CONFIG[$key]}"
done

# ============================================
# SUMMARY
# ============================================
echo ""
echo "============================================"
echo "DEMONSTRATION COMPLETE"
echo "============================================"
