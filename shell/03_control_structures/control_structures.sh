#!/bin/bash
# Control Structures Demo
set -e

echo "=== CONTROL STRUCTURES DEMO ==="

# If-else
AGE=25
if [ $AGE -ge 18 ]; then
    echo "Adult (age: $AGE)"
else
    echo "Minor"
fi

# For loop
echo "Counting 1-5:"
for i in {1..5}; do
    echo "  $i"
done

# While loop
COUNT=3
while [ $COUNT -gt 0 ]; do
    echo "Countdown: $COUNT"
    ((COUNT--))
done

# Case statement
FRUIT="apple"
case "$FRUIT" in
    apple) echo "It's an apple" ;;
    banana) echo "It's a banana" ;;
    *) echo "Unknown fruit" ;;
esac

echo "=== DEMO COMPLETE ==="
