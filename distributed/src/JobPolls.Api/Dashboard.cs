namespace JobPolls.Api;

public static class Dashboard
{
    public const string Html = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Job Polls — live</title>
<style>
  :root { color-scheme: light dark; }
  body { font: 14px/1.45 system-ui, sans-serif; margin: 0; padding: 24px; max-width: 980px; margin-inline: auto; }
  h1 { font-size: 20px; margin: 0 0 4px; }
  #live { font-size: 12px; color: #16a34a; margin-left: 8px; }
  #live.off { color: #dc2626; }
  .bar { display: flex; gap: 8px; margin: 16px 0; }
  input, select { padding: 8px 10px; font: inherit; border: 1px solid #9995; border-radius: 8px; }
  input { flex: 1; }
  #stats { font-size: 12px; opacity: .75; margin-bottom: 12px; }
  ul { list-style: none; padding: 0; margin: 0; }
  li { padding: 10px 0; border-bottom: 1px solid #9993; }
  a { color: inherit; }
  .co { opacity: .7; }
  .loc { opacity: .55; font-size: 12px; }
  .new { display: inline-block; width: 7px; height: 7px; border-radius: 50%; background: #16a34a; margin-right: 6px; vertical-align: middle; }
  .tag { font-size: 11px; text-transform: uppercase; letter-spacing: .04em; opacity: .6; margin-left: 6px; }
</style>
</head>
<body>
<h1>Job Polls <span id="live" class="off">● connecting</span></h1>
<div id="stats">loading stats…</div>
<div class="bar">
  <input id="q" type="search" placeholder="Search company, title, or city" autocomplete="off">
  <select id="region">
    <option value="">All</option>
    <option value="us">US</option>
    <option value="canada">Canada</option>
  </select>
</div>
<ul id="list"></ul>
<script>
const $ = (s) => document.querySelector(s);
const list = $('#list');
let rows = [];

function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

function render() {
  list.innerHTML = rows.map((r) => `
    <li>
      <div>${r.isNew ? '<span class="new"></span>' : ''}<a href="${esc(r.url)}" target="_blank" rel="noopener">${esc(r.title)}</a> <span class="co">· ${esc(r.company)}</span> <span class="tag">${esc(r.region)}</span></div>
      <div class="loc">${esc(r.location)}${r.postedAt ? ' · ' + esc(r.postedAt) : ''}</div>
    </li>`).join('');
}

async function load() {
  const q = encodeURIComponent($('#q').value.trim());
  const region = $('#region').value;
  const res = await fetch(`/api/jobs?q=${q}&region=${region}&limit=200`);
  rows = await res.json();
  render();
}

async function stats() {
  try {
    const s = await fetch('/api/stats').then((r) => r.json());
    $('#stats').textContent =
      `${s.total} stored · ${s.open} open · ${s.canada} CA · ${s.us} US · stream ${s.streamLength} · consumed ${s.consumerEvents} · dup ${s.duplicates}`;
  } catch {}
}

$('#q').addEventListener('input', load);
$('#region').addEventListener('change', load);

const es = new EventSource('/api/events');
es.onopen = () => { $('#live').textContent = '● live'; $('#live').classList.remove('off'); };
es.onerror = () => { $('#live').textContent = '● disconnected'; $('#live').classList.add('off'); };
es.onmessage = (e) => {
  let evt;
  try { evt = JSON.parse(e.data); } catch { return; }
  if (evt.EventType !== 'new') return;
  const region = /canada|ontario|quebec|québec|british columbia|, on|, qc|, bc|toronto|ottawa|vancouver|montréal|montreal/i
    .test(evt.Location || '') ? 'canada' : 'us';
  rows.unshift({
    url: evt.Url, title: evt.Title, company: evt.Company, location: evt.Location,
    postedAt: evt.PostedAt, region, isNew: true
  });
  if (rows.length > 300) rows.pop();
  render();
};

setInterval(stats, 5000);
load();
stats();
</script>
</body>
</html>
""";
}
