#!/usr/bin/env bash
# Copy this project (code, company lists, poller database) to the server, then run setup there.
#   bash deploy/push_to_server.sh hung@1.2.3.4
# Stop the laptop pollers first so the database isn't mid-write:
#   systemctl --user stop 'internship-watch@*'
set -euo pipefail
target="${1:?usage: deploy/push_to_server.sh user@server-ip}"
cd "$(dirname "$0")/.."
rsync -az --info=progress2 \
    --exclude '.venv/' --exclude '__pycache__/' --exclude '*.pyc' --exclude '.git/' \
    --exclude 'cv/' --exclude 'applications/' --exclude 'Hung Nguyen Resume*' --exclude 'profile.json' \
    --exclude 'web/' --exclude 'engineering-ats/*.out' --exclude 'engineering-ats/*.log' \
    --exclude 'engineering-ats/*.tmp' \
    ./ "$target:ats-scrapers/"
ssh -t "$target" 'bash ~/ats-scrapers/deploy/setup_server.sh'
