#!/bin/bash

# Stop if another instance is running
pidof -o %PPID -x "$0" >/dev/null && 
logger -p err "Error: Script $0 already running, exiting!" && 
exit 1

echo "[MetaFusion] Starting scheduled processing..."
docker exec kometa python /config/scripts/MetaFusion/metafusion.py
echo "[MetaFusion] Processing completed."

LOGFILE="/mnt/user/appdata/kometa/scripts/MetaFusion/logs/metafusion.log"

# Extract the METAFUSION SUMMARY REPORT block 
SUMMARY=$(tail -n 24 "$LOGFILE")

# Remove timestamps, log level, borders and adjust width
SUMMARY=$(echo "$SUMMARY" | sed -E 's/^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3} - [A-Z]+ - //')
SUMMARY=$(echo "$SUMMARY" | sed -E 's/^ *\| ?//; s/ ?\| *$//')
SUMMARY=$(echo "$SUMMARY" | sed -E 's/^={41,}$/========================================/')
echo "[MetaFusion] Summary report updated for email notification..."

# Email summary report
/usr/local/emhttp/webGui/scripts/notify -s "MetaFusion Summary Report" \
   -d "Libraries Processing Completed" -m "$SUMMARY"
echo "[MetaFusion] Summary report emailed..."