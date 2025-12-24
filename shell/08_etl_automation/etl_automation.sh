#!/bin/bash
# ETL Automation Demo
set -euo pipefail

echo "=== ETL AUTOMATION DEMO ==="

# Simulate job execution
run_job() {
    local job_name="$1"
    local duration="${2:-1}"
    echo "  Running: $job_name"
    sleep "$duration"
    echo "  Completed: $job_name"
}

# Sequential workflow
echo "1. Sequential Workflow:"
run_job "extract" 1
run_job "transform" 1
run_job "load" 1
echo ""

# Parallel jobs
echo "2. Parallel Jobs:"
run_job "job_a" 1 &
run_job "job_b" 1 &
run_job "job_c" 1 &
wait
echo "  All parallel jobs completed"
echo ""

# Dependency check simulation
echo "3. Dependency Management:"
check_dep() {
    echo "  Checking dependency: $1 -> OK"
    return 0
}
check_dep "extract_customers"
check_dep "extract_products"
echo "  Dependencies met, running transform..."
run_job "transform_all" 1
echo ""

echo "=== DEMO COMPLETE ==="
