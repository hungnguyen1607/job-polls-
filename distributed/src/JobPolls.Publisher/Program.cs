using System.Globalization;
using System.Text.Json;
using JobPolls.Contracts;
using Microsoft.Data.Sqlite;
using StackExchange.Redis;

var options = Options.Parse(args);
if (options.Help)
{
    Options.PrintUsage();
    return 0;
}

using var mux = await ConnectionMultiplexer.ConnectAsync(options.Redis);
var redis = mux.GetDatabase();

var cursor = options.Backfill ? 0 : LoadCursor(options.StateFile) ?? MaxRowId(options.Database);
Console.WriteLine($"publisher: db={options.Database} redis={options.Redis} stream={options.Stream} cursor={cursor} backfill={options.Backfill}");

var published = 0L;
while (true)
{
    var batch = ReadBatch(options.Database, cursor, options.Batch);
    if (batch.Count == 0)
    {
        if (options.Once) break;
        await Task.Delay(TimeSpan.FromSeconds(options.Interval));
        continue;
    }

    foreach (var row in batch)
    {
        var evt = row.ToEvent();
        var fields = new NameValueEntry[]
        {
            new("key", evt.Key),
            new("event", evt.EventType),
            new("payload", JsonSerializer.Serialize(evt)),
        };
        await redis.StreamAddAsync(options.Stream, fields);
        cursor = row.RowId;
        published++;
    }

    SaveCursor(options.StateFile, cursor);
    Console.WriteLine($"published {batch.Count} ({published} total), cursor={cursor}");

    if (options.Once && batch.Count < options.Batch) break;
}

Console.WriteLine($"publisher done: {published} events");
return 0;

static long MaxRowId(string dbPath)
{
    using var conn = new SqliteConnection($"Data Source={dbPath};Mode=ReadOnly;Cache=Private");
    conn.Open();
    using var cmd = conn.CreateCommand();
    cmd.CommandText = "SELECT COALESCE(MAX(rowid), 0) FROM postings";
    return Convert.ToInt64(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);
}

static List<Row> ReadBatch(string dbPath, long cursor, int limit)
{
    using var conn = new SqliteConnection($"Data Source={dbPath};Mode=ReadOnly;Cache=Private");
    conn.Open();
    using var cmd = conn.CreateCommand();
    cmd.CommandText = """
        SELECT rowid, key, source, company_key, company, title, location,
               posted_at, posted_text, url, first_seen, last_seen, open, origin, baseline
        FROM postings
        WHERE rowid > $cursor
        ORDER BY rowid
        LIMIT $limit
        """;
    cmd.Parameters.AddWithValue("$cursor", cursor);
    cmd.Parameters.AddWithValue("$limit", limit);

    var rows = new List<Row>();
    using var reader = cmd.ExecuteReader();
    while (reader.Read())
    {
        rows.Add(new Row(
            RowId: reader.GetInt64(0),
            Key: reader.GetString(1),
            Source: reader.IsDBNull(2) ? "" : reader.GetString(2),
            CompanyKey: reader.IsDBNull(3) ? "" : reader.GetString(3),
            Company: reader.IsDBNull(4) ? "" : reader.GetString(4),
            Title: reader.IsDBNull(5) ? "" : reader.GetString(5),
            Location: reader.IsDBNull(6) ? "" : reader.GetString(6),
            PostedAt: reader.IsDBNull(7) ? null : reader.GetString(7),
            PostedText: reader.IsDBNull(8) ? null : reader.GetString(8),
            Url: reader.IsDBNull(9) ? "" : reader.GetString(9),
            FirstSeen: reader.IsDBNull(10) ? "" : reader.GetString(10),
            LastSeen: reader.IsDBNull(11) ? "" : reader.GetString(11),
            Open: !reader.IsDBNull(12) && reader.GetInt64(12) != 0,
            Origin: reader.IsDBNull(13) ? "" : reader.GetString(13),
            Baseline: !reader.IsDBNull(14) && reader.GetInt64(14) != 0));
    }

    return rows;
}

static long? LoadCursor(string path)
{
    if (!File.Exists(path)) return null;
    try
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        return doc.RootElement.GetProperty("rowid").GetInt64();
    }
    catch (Exception)
    {
        return null;
    }
}

static void SaveCursor(string path, long rowid)
{
    var dir = Path.GetDirectoryName(Path.GetFullPath(path));
    if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);
    File.WriteAllText(path, JsonSerializer.Serialize(new { rowid }));
}

internal sealed record Row(
    long RowId, string Key, string Source, string CompanyKey, string Company,
    string Title, string Location, string? PostedAt, string? PostedText, string Url,
    string FirstSeen, string LastSeen, bool Open, string Origin, bool Baseline)
{
    public PostingEvent ToEvent() => new(
        Key, Source, CompanyKey, Company, Title, Location, PostedAt, PostedText, Url,
        FirstSeen, LastSeen, Open, Origin, Baseline, PostingEvent.New);
}

internal sealed record Options
{
    public string Database { get; init; } =
        Environment.GetEnvironmentVariable("JOBPOLLS_DB")
        ?? "/home/hung/ats-scrapers/engineering-ats/.internships.sqlite";

    public string Redis { get; init; } =
        Environment.GetEnvironmentVariable("JOBPOLLS_REDIS") ?? "localhost:6379";

    public string Stream { get; init; } = "jobs:events";
    public string StateFile { get; init; } = ".publisher-state.json";
    public int Batch { get; init; } = 500;
    public int Interval { get; init; } = 2;
    public bool Once { get; init; }
    public bool Backfill { get; init; }
    public bool Help { get; init; }

    public static Options Parse(string[] args)
    {
        var o = new Options();
        for (var i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--db": o = o with { Database = args[++i] }; break;
                case "--redis": o = o with { Redis = args[++i] }; break;
                case "--stream": o = o with { Stream = args[++i] }; break;
                case "--state": o = o with { StateFile = args[++i] }; break;
                case "--batch": o = o with { Batch = int.Parse(args[++i], CultureInfo.InvariantCulture) }; break;
                case "--interval": o = o with { Interval = int.Parse(args[++i], CultureInfo.InvariantCulture) }; break;
                case "--once": o = o with { Once = true }; break;
                case "--backfill": o = o with { Backfill = true }; break;
                case "-h" or "--help": o = o with { Help = true }; break;
                default: throw new ArgumentException($"unknown option: {args[i]}");
            }
        }

        return o;
    }

    public static void PrintUsage()
    {
        Console.WriteLine("""
            JobPolls.Publisher — tail the poller SQLite store into a Redis Stream.

              --db PATH         postings SQLite (default $JOBPOLLS_DB)
              --redis HOST:PORT (default localhost:6379)
              --stream NAME     (default jobs:events)
              --state PATH      rowid cursor file (default .publisher-state.json)
              --batch N         rows per batch (default 500)
              --interval SEC    idle poll interval (default 2)
              --backfill        start from rowid 0 (default: skip history)
              --once            drain once, then exit
            """);
    }
}
