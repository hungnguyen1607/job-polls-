#!/usr/bin/env bash
# End-to-end local demo: seeded SQLite -> publisher -> Redis Stream -> consumer -> API.
set -euo pipefail

cd "$(dirname "$0")/.."
export PATH="${HOME}/.dotnet:${PATH}"

tmp="${TMPDIR:-/tmp}/jobpolls-demo"
rm -rf "$tmp"
mkdir -p "$tmp"

make -C native >/dev/null
dotnet build JobPolls.slnx -c Release -v q >/dev/null

python3 - "$tmp/demo.sqlite" <<'PY'
import sqlite3, sys
db = sqlite3.connect(sys.argv[1])
db.execute("""CREATE TABLE postings (
 key TEXT PRIMARY KEY, source TEXT, company_key TEXT, company TEXT, title TEXT,
 location TEXT, posted_at TEXT, posted_text TEXT, url TEXT,
 first_seen TEXT, last_seen TEXT, open INTEGER, origin TEXT, baseline INTEGER)""")
rows = [
 ("ats|1","greenhouse","acme","Acme","Software Engineer Intern","Seattle, WA","2026-11-09","2 days ago","https://example.com/1","2026-11-09T10:00:00","2026-11-09T10:00:00",1,"greenhouse",0),
 ("ats|2","greenhouse","acme","Acme","software engineer intern!","Seattle, WA","2026-11-09","2 days ago","https://example.com/2","2026-11-09T10:05:00","2026-11-09T10:05:00",1,"greenhouse",0),
 ("ats|3","lever","north","North Labs","Backend Developer Co-op","Toronto, ON","2026-11-08","3 days ago","https://example.com/3","2026-11-09T10:10:00","2026-11-09T10:10:00",1,"lever",0),
 ("ats|4","ashby","south","South Co","Data Analyst Intern","Austin, TX","2026-11-07","4 days ago","https://example.com/4","2026-11-09T10:15:00","2026-11-09T10:15:00",1,"ashby",0),
 ("ats|5","workday","east","East Group","Mechanical Engineer Intern","Montréal, QC","2026-11-06","5 days ago","https://example.com/5","2026-11-09T10:20:00","2026-11-09T10:20:00",1,"workday",0),
 ("ats|6","greenhouse","west","West Inc","ML Engineer Intern","Vancouver, BC","2026-11-05","6 days ago","https://example.com/6","2026-11-09T10:25:00","2026-11-09T10:25:00",1,"greenhouse",0),
]
db.executemany("INSERT INTO postings VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
db.commit()
PY

stream="jobs:demo-$(date +%s)"
pub="dotnet src/JobPolls.Publisher/bin/Release/net10.0/JobPolls.Publisher.dll"
con="dotnet src/JobPolls.Consumer/bin/Release/net10.0/JobPolls.Consumer.dll"

echo "== publish =="
"$pub" --db "$tmp/demo.sqlite" --stream "$stream" --state "$tmp/pub.json" --once --backfill

echo "== consume =="
"$con" --stream "$stream" --group "demo" --store "$tmp/consumer.db" --count 10 --idle-ms 2000 --name c1

echo "== api: http://localhost:5000 =="
JOBPOLLS_STORE="$tmp/consumer.db" JOBPOLLS_STREAM="$stream" ASPNETCORE_URLS="http://localhost:5000" \
  exec dotnet src/JobPolls.Api/bin/Release/net10.0/JobPolls.Api.dll
