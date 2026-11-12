using JobPolls.Contracts;
using Microsoft.Data.Sqlite;

namespace JobPolls.Api;

public sealed class StoreReader
{
    private readonly string _path;
    private readonly string _connectionString;

    public StoreReader(string path)
    {
        _path = Path.GetFullPath(path);
        _connectionString = $"Data Source={_path};Mode=ReadOnly;Cache=Private";
    }

    public bool Exists => File.Exists(_path);

    public IReadOnlyList<StoredPosting> Query(string? region, string? query, int limit)
    {
        if (!Exists) return [];

        using var conn = new SqliteConnection(_connectionString);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT key, source, company, title, location, posted_at, url,
                   first_seen, last_seen, open, region, simhash
            FROM postings
            WHERE ($region IS NULL OR region = $region)
              AND ($q IS NULL OR lower(company) LIKE $like OR lower(title) LIKE $like OR lower(location) LIKE $like)
            ORDER BY first_seen DESC
            LIMIT $limit
            """;
        cmd.Parameters.AddWithValue("$region", (object?)region ?? DBNull.Value);
        cmd.Parameters.AddWithValue("$q", (object?)query ?? DBNull.Value);
        cmd.Parameters.AddWithValue("$like", query is null ? DBNull.Value : $"%{query.ToLowerInvariant()}%");
        cmd.Parameters.AddWithValue("$limit", Math.Clamp(limit, 1, 1000));

        var rows = new List<StoredPosting>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            rows.Add(new StoredPosting(
                Key: reader.GetString(0),
                Source: reader.IsDBNull(1) ? "" : reader.GetString(1),
                Company: reader.IsDBNull(2) ? "" : reader.GetString(2),
                Title: reader.IsDBNull(3) ? "" : reader.GetString(3),
                Location: reader.IsDBNull(4) ? "" : reader.GetString(4),
                PostedAt: reader.IsDBNull(5) ? null : reader.GetString(5),
                Url: reader.IsDBNull(6) ? "" : reader.GetString(6),
                FirstSeen: reader.IsDBNull(7) ? "" : reader.GetString(7),
                LastSeen: reader.IsDBNull(8) ? "" : reader.GetString(8),
                Open: !reader.IsDBNull(9) && reader.GetInt64(9) != 0,
                Region: reader.IsDBNull(10) ? "" : reader.GetString(10),
                SimHash: reader.IsDBNull(11) ? 0 : unchecked((ulong)reader.GetInt64(11))));
        }

        return rows;
    }

    public (long total, long open, long canada, long us) Stats()
    {
        if (!Exists) return (0, 0, 0, 0);

        using var conn = new SqliteConnection(_connectionString);
        conn.Open();
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
}
