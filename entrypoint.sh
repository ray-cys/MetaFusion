#!/bin/bash
set -e

# Read schedule from env or config (default to daily at 3am)
SCHEDULE="${SCHEDULE:-$(python3 -c 'import sys; from helper.config import load_config_file; print(load_config_file()["settings"].get("schedule", "0 3 * * *"))')}"

echo "$SCHEDULE python3 /config/metafusion.py >> /config/cron.log 2>&1" > /etc/cron.d/metafusion-cron
chmod 0644 /etc/cron.d/metafusion-cron
crontab /etc/cron.d/metafusion-cron

# Start cron in foreground
cron -f