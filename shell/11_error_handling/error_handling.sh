#!/bin/bash
# Error Handling Demo
set -euo pipefail

echo "=== ERROR HANDLING DEMO ==="

# Logging functions
log_info() { echo "[INFO] $*"; }
log_error() { echo "[ERROR] $*" >&2; }

# Cleanup trap
TEMP_FILE=""
cleanup() {
    log_info "Cleanup: Removing temp files"
    [ -n "$TEMP_FILE" ] && rm -f "$TEMP_FILE"
}
trap cleanup EXIT

# Create temp file
TEMP_FILE=$(mktemp)
log_info "Created temp file: $TEMP_FILE"

# Retry function
retry() {
    local max=$1
    local delay=$2
    shift 2
    local cmd="$*"
    
    for ((i=1; i<=max; i++)); do
        log_info "Attempt $i of $max"
        if eval "$cmd"; then
            log_info "Success!"
            return 0
        fi
        [ $i -lt $max ] && sleep "$delay"
    done
    log_error "All attempts failed"
    return 1
}

# Demo: Successful command
echo ""
echo "1. Successful command:"
if ls /tmp > /dev/null 2>&1; then
    log_info "Command succeeded (exit code: 0)"
fi

# Demo: Failed command with handling
echo ""
echo "2. Failed command with handling:"
set +e
ls /nonexistent_directory 2>/dev/null
EXIT_CODE=$?
set -e
log_info "Command failed (exit code: $EXIT_CODE) - handled gracefully"

# Demo: Retry logic
echo ""
echo "3. Retry logic (simulated):"
COUNTER=0
simulate_flaky() {
    ((COUNTER++))
    if [ $COUNTER -lt 3 ]; then
        return 1
    fi
    return 0
}
retry 3 1 simulate_flaky

echo ""
echo "=== DEMO COMPLETE ==="
