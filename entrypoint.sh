#!/bin/bash
set -e

SCHEDULE="${SCHEDULE:-$(python3 -c 'import sys; from helper.config import load_config_file; print(load_config_file()["settings"].get("schedule", "0 3 * * *"))')}"

CRON_FILE="/etc/cron.d/metafusion-cron"
> "$CRON_FILE"

if [[ "$SCHEDULE" =~ ^([0-9]{2}:[0-9]{2},?)+$ ]]; then
  IFS=',' read -ra TIMES <<< "$SCHEDULE"
  for t in "${TIMES[@]}"; do
    HOUR="${t%:*}"
    MIN="${t#*:}"
    echo "$MIN $HOUR * * * python3 /config/metafusion.py >> /config/cron.log 2>&1" >> "$CRON_FILE"
  done
else
  echo "$SCHEDULE python3 /config/metafusion.py >> /config/cron.log 2>&1" >> "$CRON_FILE"
fi

chmod 0644 "$CRON_FILE"
crontab "$CRON_FILE"

cron -f