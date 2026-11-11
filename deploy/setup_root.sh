#!/usr/bin/env bash
# Root part of the server setup (safe to re-run): system packages, the `ats` user that runs the
# pollers (no sudo), lingering so its services run with nobody logged in, Chromium's libraries.
# Leaves the firewall and the server's other apps alone.
set -euo pipefail
USER_NAME=ats
apt-get update -qq
apt-get install -y -qq curl rsync sqlite3 >/dev/null
id "$USER_NAME" >/dev/null 2>&1 || useradd -m -s /bin/bash "$USER_NAME"
install -d -m 700 -o "$USER_NAME" -g "$USER_NAME" "/home/$USER_NAME/.ssh"
install -m 600 -o "$USER_NAME" -g "$USER_NAME" /root/.ssh/authorized_keys "/home/$USER_NAME/.ssh/authorized_keys"
loginctl enable-linger "$USER_NAME"
# Chromium system libraries (cloakbrowser / Playwright download the browser itself).
if [ -x "/home/$USER_NAME/ats-scrapers/.venv/bin/python" ]; then
    "/home/$USER_NAME/ats-scrapers/.venv/bin/python" -m playwright install-deps chromium >/dev/null
fi
echo "root setup done"
