#!/bin/bash

echo "[KOMETA SCRIPT] Starting MetaFusion script..."
docker exec kometa python /config/scripts/MetaFusion/metafusion.py
echo "[KOMETA SCRIPT] MetaFusion script completed."

# Extract the MetaFusion summary report from the log
SUMMARY=$(awk '/^=+$/ {p = !p; s = ""} p {s = s $0 ORS} END {print s}' /mnt/user/appdata/kometa/scripts/MetaFusion/logs/metafusion.log)

# If summary is empty, fallback to last 11 lines
if [ -z "$SUMMARY" ]; then
  SUMMARY=$(tail -n 11 /mnt/user/appdata/kometa/scripts/MetaFusion/logs/metafusion.log)
fi

# Send the summary as an email notification
/usr/local/emhttp/webGui/scripts/notify -s "MetaFusion Summary Report" \
  -d "Libraries Processing Completed" -m "$SUMMARY"

echo "[KOMETA SCRIPT] MetaFusion summary report emailed."