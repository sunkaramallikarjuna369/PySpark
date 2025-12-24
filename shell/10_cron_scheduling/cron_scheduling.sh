#!/bin/bash
# Cron Scheduling Demo
set -euo pipefail

echo "=== CRON SCHEDULING DEMO ==="

# Display cron syntax
echo "1. Cron Syntax:"
echo "   ┌───────────── minute (0-59)"
echo "   │ ┌───────────── hour (0-23)"
echo "   │ │ ┌───────────── day of month (1-31)"
echo "   │ │ │ ┌───────────── month (1-12)"
echo "   │ │ │ │ ┌───────────── day of week (0-7)"
echo "   │ │ │ │ │"
echo "   * * * * * command"
echo ""

# Common schedules
echo "2. Common Schedules:"
echo "   */5 * * * *     - Every 5 minutes"
echo "   0 * * * *       - Every hour"
echo "   0 0 * * *       - Daily at midnight"
echo "   0 0 * * 0       - Weekly on Sunday"
echo "   0 0 1 * *       - Monthly on 1st"
echo ""

# Simulate cron job listing
echo "3. Sample Crontab:"
echo "   # Data Pipeline Jobs"
echo "   0 2 * * * /scripts/etl_daily.sh"
echo "   */15 * * * * /scripts/health_check.sh"
echo "   0 */6 * * * /scripts/backup_db.sh"
echo "   0 3 * * 0 /scripts/weekly_cleanup.sh"
echo ""

# Next run calculation
echo "4. Next Run Times (simulated):"
CURRENT_HOUR=$(date +%H)
CURRENT_MIN=$(date +%M)
echo "   Current time: $(date '+%H:%M')"
echo "   Every hour job: Next at $(printf '%02d' $((CURRENT_HOUR + 1))):00"
echo "   Daily midnight: Next at 00:00"
echo ""

echo "=== DEMO COMPLETE ==="
