#!/usr/bin/env bash
# Copy this project (code, company lists, poller database, .env) to the server and set it up.
#   bash deploy/push_to_server.sh 1.2.3.4
# Stop the laptop pollers first so the database isn't mid-write:
#   systemctl --user stop 'internship-watch@*'
set -euo pipefail
host="${1:?usage: deploy/push_to_server.sh server-ip}"
cd "$(dirname "$0")/.."
ssh "root@$host" 'bash -s' < deploy/setup_root.sh
# Consistent copy of the live database (WAL folded in), not the raw files.
.venv/bin/python -c "import sqlite3; s = sqlite3.connect('engineering-ats/.internships.sqlite'); \\
d = sqlite3.connect('engineering-ats/.internships.push.sqlite'); s.backup(d); d.close()"
rsync -az --info=progress2 --chmod=Fgo-rwx \
    --exclude '.venv/' --exclude '__pycache__/' --exclude '*.pyc' --exclude '.git/' \
    --exclude 'cv/' --exclude 'applications/' --exclude 'Hung Nguyen Resume*' --exclude '*.pdf' \
    --exclude 'profile.json' --exclude 'innova_tickets_26/' --exclude 'research_notes/' \
    --exclude 'web/' --exclude 'engineering-ats/*.out' --exclude 'engineering-ats/*.log' \
    --exclude 'engineering-ats/*.tmp' --exclude 'engineering-ats/.internships.sqlite*' \
    --exclude '.env.feed.local' \
    ./ "ats@$host:ats-scrapers/"
rsync -az engineering-ats/.internships.push.sqlite "ats@$host:ats-scrapers/engineering-ats/.internships.sqlite"
rm -f engineering-ats/.internships.push.sqlite
ssh "ats@$host" "MAX_CHILDREN_OVERRIDE=${MAX_CHILDREN_OVERRIDE:-} bash ~/ats-scrapers/deploy/setup_server.sh"
# Chromium libraries need the venv, which only exists after setup_server.sh: run the root part again.
ssh "root@$host" 'bash -s' < deploy/setup_root.sh
