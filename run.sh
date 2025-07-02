#!/bin/bash

echo "[KOMETA SCRIPT] Starting MetaFusion script..."
docker exec kometa python /config/scripts/MetaFusion/metafusion.py
echo "[KOMETA SCRIPT] MetaFusion script completed."

LOGFILE="/mnt/user/appdata/kometa/scripts/MetaFusion/logs/metafusion.log"

# Extract the METAFUSION SUMMARY REPORT block 
SUMMARY=$(tail -n 32 "$LOGFILE")

# Remove timestamps and log level
SUMMARY=$(echo "$SUMMARY" | sed -E 's/^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3} - [A-Z]+ - //')

# Remove leading and trailing borders
SUMMARY=$(echo "$SUMMARY" | sed -E 's/^ *\| ?//; s/ ?\| *$//')

# Email summary report
/usr/local/emhttp/webGui/scripts/notify -s "MetaFusion Summary Report" \
   -d "Libraries Processing Completed" -m "$SUMMARY"

echo "[KOMETA SCRIPT] MetaFusion summary report extracted."