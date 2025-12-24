#!/bin/bash
# Functions Demo
set -e

echo "=== FUNCTIONS DEMO ==="

# Basic function
greet() {
    local name="${1:-World}"
    echo "Hello, $name!"
}

greet
greet "Data Engineer"

# Function with return value
add() {
    local a=$1
    local b=$2
    echo $((a + b))
}

RESULT=$(add 10 20)
echo "10 + 20 = $RESULT"

# Function with validation
validate_number() {
    local num="$1"
    if [[ "$num" =~ ^[0-9]+$ ]]; then
        return 0
    else
        return 1
    fi
}

if validate_number "42"; then
    echo "42 is a valid number"
fi

echo "=== DEMO COMPLETE ==="
