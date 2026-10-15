// Proxies the poller server's feed so its address and token stay out of the browser.
// Vercel env: FEED_URL (e.g. http://1.2.3.4:8787/feed.json), FEED_TOKEN.
export default async function handler(req, res) {
  try {
    const upstream = await fetch(process.env.FEED_URL, {
      headers: { Authorization: `Bearer ${process.env.FEED_TOKEN}` },
      signal: AbortSignal.timeout(10000),
    });
    if (!upstream.ok) {
      res.status(502).json({ error: `poller server answered ${upstream.status}` });
      return;
    }
    res.setHeader("Cache-Control", "s-maxage=30, stale-while-revalidate=300");
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.status(200).send(await upstream.text());
  } catch (err) {
    res.status(502).json({ error: `poller server unreachable: ${err.message}` });
  }
}
