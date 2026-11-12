using JobPolls.Contracts;
using Microsoft.Data.Sqlite;

namespace JobPolls.Consumer;

/// <summary>Consumer-owned store. SQLite by default; swap for Postgres in a later step.</summary>
public sealed class Store
{
    private readonly string _connectionString;

    public Store(string path)
    {
        var full = Path.GetFullPath(path);
        var dir = Path.GetDirectoryName(full);
        if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);
        _connectionString = $"Data Source={full};Cache=Shared";
    }

    public void Init()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS postings (
                key TEXT PRIMARY KEY, source TEXT, company TEXT, title TEXT, location TEXT,
                posted_at TEXT, url TEXT, first_seen TEXT, last_seen TEXT, open INTEGER,
                region TEXT, simhash INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_postings_region ON postings(region);
            CREATE INDEX IF NOT EXISTS idx_postings_first_seen ON postings(first_seen);
            """;
        cmd.ExecuteNonQuery();
    }

    public HashSet<string> LoadKeys()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT key FROM postings";
        var keys = new HashSet<string>(StringComparer.Ordinal);
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) keys.Add(reader.GetString(0));
        return keys;
    }

    public HashSet<ulong> LoadSimHashes()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT simhash FROM postings WHERE simhash IS NOT NULL";
        var hashes = new HashSet<ulong>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) hashes.Add((ulong)reader.GetInt64(0));
        return hashes;
    }

    public void Upsert(StoredPosting p)
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO postings
                (key, source, company, title, location, posted_at, url, first_seen, last_seen, open, region, simhash)
            VALUES
                ($key, $source, $company, $title, $location, $posted_at, $url, $first_seen, $last_seen, $open, $region, $simhash)
            ON CONFLICT(key) DO UPDATE SET
                source=excluded.source, company=excluded.company, title=excluded.title,
                location=excluded.location, posted_at=COALESCE(excluded.posted_at, postings.posted_at),
                url=excluded.url, last_seen=excluded.last_seen, open=excluded.open,
                region=excluded.region, simhash=excluded.simhash
            """;
        cmd.Parameters.AddWithValue("$key", p.Key);
        cmd.Parameters.AddWithValue("$source", p.Source);
        cmd.Parameters.AddWithValue("$company", p.Company);
        cmd.Parameters.AddWithValue("$title", p.Title);
        cmd.Parameters.AddWithValue("$location", p.Location);
        cmd.Parameters.AddWithValue("$posted_at", (object?)p.PostedAt ?? DBNull.Value);
        cmd.Parameters.AddWithValue("$url", p.Url);
        cmd.Parameters.AddWithValue("$first_seen", p.FirstSeen);
        cmd.Parameters.AddWithValue("$last_seen", p.LastSeen);
        cmd.Parameters.AddWithValue("$open", p.Open ? 1 : 0);
        cmd.Parameters.AddWithValue("$region", p.Region);
        cmd.Parameters.AddWithValue("$simhash", unchecked((long)p.SimHash));
        cmd.ExecuteNonQuery();
    }

    public (long total, long open, long canada, long us) Stats()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*),
                   COALESCE(SUM(open), 0),
                   COALESCE(SUM(CASE WHEN region='canada' THEN 1 ELSE 0 END), 0),
                   COALESCE(SUM(CASE WHEN region='us' THEN 1 ELSE 0 END), 0)
            FROM postings
            """;
        using var reader = cmd.ExecuteReader();
        reader.Read();
        return (reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3));
    }

    private SqliteConnection Open()
    {
        var conn = new SqliteConnection(_connectionString);
        conn.Open();
        using var busy = conn.CreateCommand();
        busy.CommandText = "PRAGMA busy_timeout=5000";
        busy.ExecuteNonQuery();
        return conn;
    }
}
