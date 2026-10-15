#!/usr/bin/env bash
# One-time (and safe to re-run) setup on a fresh Ubuntu/Debian server, run as a normal sudo user.
# Installs Python deps, the poller + feed services, and opens the feed port.
set -euo pipefail
cd ~/ats-scrapers

sudo apt-get update -qq
sudo apt-get install -y -qq curl rsync ufw
command -v uv >/dev/null || curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
uv sync --python 3.14
# System libraries Chromium needs (cloakbrowser downloads its own browser binary on first use).
sudo .venv/bin/python -m playwright install-deps chromium

if [ ! -f .env.feed ]; then
    echo "FEED_TOKEN=$(openssl rand -hex 24)" > .env.feed
    chmod 600 .env.feed
fi

mkdir -p ~/.config/systemd/user
cp -r deploy/systemd/* ~/.config/systemd/user/
sudo loginctl enable-linger "$USER"   # keep services running with nobody logged in
systemctl --user daemon-reload
systemctl --user enable --now internship-feed \
    internship-watch@big_ats internship-watch@bigtech internship-watch@canada internship-watch@small_ats

sudo ufw allow OpenSSH >/dev/null
sudo ufw allow 8787/tcp >/dev/null
sudo ufw --force enable >/dev/null

ip=$(curl -s https://api.ipify.org || hostname -I | awk '{print $1}')
echo
echo "Done. Services:"
systemctl --user --no-pager list-units 'internship-*' | sed -n '2,7p'
echo
echo "Set these in Vercel (Project > Settings > Environment Variables):"
echo "  FEED_URL=http://$ip:8787/feed.json"
echo "  $(cat .env.feed)"
