#!/usr/bin/env bash
# User part of the server setup, run as `ats` (safe to re-run): Python env, poller services sized
# to the box, then start them. Run deploy/setup_root.sh as root first.
set -euo pipefail
cd ~/ats-scrapers
command -v uv >/dev/null || curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
uv sync --python 3.14 -q

[ -f .env.feed ] || { echo "FEED_TOKEN=$(openssl rand -hex 24)" > .env.feed; }
chmod 600 .env .env.feed 2>/dev/null || true

# Budget: about 70% of the RAM that's free right now, and cores minus 2 (other apps keep the rest).
free_mb=$(awk '/MemAvailable/ {print int($2/1024)}' /proc/meminfo)
cores=$(nproc)
mem_max=$(( free_mb * 70 / 100 ))
mem_high=$(( mem_max * 85 / 100 ))
cpu=$(( (cores > 3 ? cores - 2 : 1) * 100 ))
# ~200 MB per pass process, shared by 6 watchers (fast + big_ats + bigtech + canada + small_ats + outreach).
children=$(( mem_max / 200 / 6 )); [ "$children" -lt 2 ] && children=2
# Override on boxes with more headroom than the 200 MB estimate allows.
children=${MAX_CHILDREN_OVERRIDE:-$children}
printf 'MAX_CHILDREN=%s\n' "$children" > .env.server

mkdir -p ~/.config/systemd/user
cp -r deploy/systemd/* ~/.config/systemd/user/
sed -i "s/^MemoryHigh=.*/MemoryHigh=${mem_high}M/; s/^MemoryMax=.*/MemoryMax=${mem_max}M/; s/^CPUQuota=.*/CPUQuota=${cpu}%/" \
    ~/.config/systemd/user/internship.slice
export XDG_RUNTIME_DIR="/run/user/$(id -u)"
systemctl --user daemon-reload
systemctl --user enable --now internship-feed internship-health.timer internship-discover-sweep \
    internship-watch@fast internship-watch@big_ats internship-watch@bigtech internship-watch@canada \
    internship-watch@small_ats internship-watch@outreach
# `enable --now` does not restart already-running units; restart so changed
# SOURCES / intervals / limits take effect on an existing deployment.
systemctl --user restart internship-watch@fast internship-watch@big_ats internship-watch@bigtech \
    internship-watch@canada internship-watch@small_ats internship-watch@outreach

echo
echo "Budget: ${mem_max} MB RAM, ${cpu}% CPU for all pollers; MAX_CHILDREN=${children} per watcher"
systemctl --user --no-pager list-units 'internship-*'
ip=$(curl -s https://api.ipify.org || hostname -I | awk '{print $1}')
echo
echo "Feed for the web page: FEED_URL=http://$ip:8787/feed.json  ($(cut -d= -f1 .env.feed) in .env.feed)"
