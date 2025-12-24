#!/usr/bin/env bash
#===============================================================================
# SCRIPT: best_practices.sh
# DESCRIPTION: Demonstrates shell scripting best practices
#===============================================================================

set -euo pipefail
IFS=$'\n\t'

readonly SCRIPT_NAME=$(basename "${BASH_SOURCE[0]}")
readonly VERSION="1.0.0"

# Logging
log_info() { echo "[INFO] $*"; }
log_error() { echo "[ERROR] $*" >&2; }

# Cleanup trap
cleanup() {
    log_info "Cleanup complete"
}
trap cleanup EXIT

# Help
show_help() {
    cat << EOF
Usage: $SCRIPT_NAME [OPTIONS]

Options:
    -h, --help      Show help
    -v, --version   Show version

Example:
    $SCRIPT_NAME --help
EOF
}

# Argument parsing
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -h|--help) show_help; exit 0 ;;
            -v|--version) echo "$VERSION"; exit 0 ;;
            *) log_error "Unknown: $1"; exit 1 ;;
        esac
    done
}

# Main
main() {
    parse_args "$@"
    
    echo "=== BEST PRACTICES DEMO ==="
    echo ""
    
    echo "1. Script Structure:"
    echo "   - Shebang: #!/usr/bin/env bash"
    echo "   - Strict mode: set -euo pipefail"
    echo "   - Constants with readonly"
    echo "   - Functions for organization"
    echo ""
    
    echo "2. Security:"
    echo "   - Quote all variables"
    echo "   - Use [[ ]] for tests"
    echo "   - Validate input"
    echo "   - Secure temp files"
    echo ""
    
    echo "3. Performance:"
    echo "   - Use built-in operations"
    echo "   - Minimize subshells"
    echo "   - Process in parallel"
    echo ""
    
    echo "4. Maintainability:"
    echo "   - Clear documentation"
    echo "   - Consistent naming"
    echo "   - Error handling"
    echo "   - Logging"
    echo ""
    
    echo "=== DEMO COMPLETE ==="
}

main "$@"
