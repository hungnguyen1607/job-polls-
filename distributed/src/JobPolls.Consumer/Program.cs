using System.Globalization;
using System.Text.Json;
using JobPolls.Contracts;
using JobPolls.Consumer;
using JobPolls.Native;
using StackExchange.Redis;

var options = Options.Parse(args);
if (options.Help) { Options.PrintUsage(); return 0; }

using var mux = await ConnectionMultiplexer.ConnectAsync(options.Redis);
var redis = mux.GetDatabase();

var store = new Store(options.Store);
store.Init();

var seenKeys = store.LoadKeys();
var seenHashes = store.LoadSimHashes();
var statsKey = "jobs:stats";

try
{
    await redis.StreamCreateConsumerGroupAsync(options.Stream, options.Group, "0", createStream: true);
}
catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP", StringComparison.Ordinal))
{
    // group exists
}

Console.WriteLine(
    $"consumer[{options.Name}]: stream={options.Stream} group={options.Group} store={options.Store} " +
    $"existing={seenKeys.Count} threshold={options.Threshold} native={NativeDedup.Version()}");

long processed = 0, duplicates = 0, errors = 0;
var idle = 0;

while (processed < options.Count)
{
    var entries = await redis.StreamReadGroupAsync(
        options.Stream, options.Group, options.Name, ">", count: options.Batch);

    if (entries.Length == 0)
    {
        idle += 250;
        if (options.Once || idle >= options.IdleMs) break;
        await Task.Delay(250);
        continue;
    }

    idle = 0;

    var events = new List<(StreamEntry Entry, PostingEvent Event)>();
    foreach (var entry in entries)
    {
        try
        {
            var payload = Field(entry, "payload");
            var evt = JsonSerializer.Deserialize<PostingEvent>(payload);
            if (evt is not null) events.Add((entry, evt));
            else errors++;
        }
        catch (Exception ex)
        {
            errors++;
            Console.Error.WriteLine($"bad event {entry.Id}: {ex.Message}");
            await redis.StreamAcknowledgeAsync(options.Stream, options.Group, entry.Id);
        }
    }

    if (events.Count == 0) continue;

    // Native batch pass: normalize + SimHash + intra-batch near-dup (C++ arena, no per-item alloc).
    var titles = events.Select(e => e.Event.Title).ToArray();
    var (intraDup, hashes) = NativeDedup.DedupBatch(titles, options.Threshold);
    NativeDedup.ArenaReset();

    for (var i = 0; i < events.Count; i++)
    {
        var (entry, evt) = events[i];

        if (intraDup[i] || seenHashes.Contains(hashes[i]) || !seenKeys.Add(evt.Key))
        {
            duplicates++;
            await redis.StreamAcknowledgeAsync(options.Stream, options.Group, entry.Id);
            continue;
        }

        try
        {
            var stored = new StoredPosting(
                evt.Key, evt.Source, evt.Company, evt.Title, evt.Location,
                evt.PostedAt, evt.Url, evt.FirstSeen, evt.LastSeen, evt.Open,
                RegionDetector.Classify(evt.Location), hashes[i]);

            store.Upsert(stored);
            seenHashes.Add(hashes[i]);
            await redis.StreamAcknowledgeAsync(options.Stream, options.Group, entry.Id);
            processed++;
        }
        catch (Exception ex)
        {
            errors++;
            Console.Error.WriteLine($"store failed {evt.Key}: {ex.Message}");
        }
    }

    await redis.HashSetAsync(statsKey, "events", processed);
    await redis.HashSetAsync(statsKey, "duplicates", duplicates);
    await redis.HashSetAsync(statsKey, "errors", errors);
    await redis.HashSetAsync(statsKey, "lastEventMs", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

    if (events.Count > 0)
        Console.WriteLine($"[{options.Name}] +{events.Count - intraDup.Count(x => x)} stored, {intraDup.Count(x => x)} intra-batch dup, total={processed}");
}

Console.WriteLine(
    $"consumer[{options.Name}] done: stored={processed} duplicates={duplicates} errors={errors} " +
    $"arena={NativeDedup.ArenaBytes}B");
return 0;

static string Field(StreamEntry entry, string name)
{
    foreach (var value in entry.Values)
    {
        if (value.Name.ToString() == name) return value.Value.ToString();
    }

    return "";
}

internal sealed record Options
{
    public string Redis { get; init; } = Environment.GetEnvironmentVariable("JOBPOLLS_REDIS") ?? "localhost:6379";
    public string Stream { get; init; } = "jobs:events";
    public string Group { get; init; } = "jobs:consumers";
    public string Name { get; init; } = $"{Environment.MachineName}-{Environment.ProcessId}";
    public string Store { get; init; } = Environment.GetEnvironmentVariable("JOBPOLLS_STORE") ?? "jobpolls.consumer.db";
    public int Batch { get; init; } = 64;
    public uint Threshold { get; init; } = 3;
    public long Count { get; init; } = long.MaxValue;
    public int IdleMs { get; init; } = 30_000;
    public bool Once { get; init; }
    public bool Help { get; init; }

    public static Options Parse(string[] args)
    {
        var o = new Options();
        for (var i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--redis": o = o with { Redis = args[++i] }; break;
                case "--stream": o = o with { Stream = args[++i] }; break;
                case "--group": o = o with { Group = args[++i] }; break;
                case "--name": o = o with { Name = args[++i] }; break;
                case "--store": o = o with { Store = args[++i] }; break;
                case "--batch": o = o with { Batch = int.Parse(args[++i], CultureInfo.InvariantCulture) }; break;
                case "--threshold": o = o with { Threshold = uint.Parse(args[++i], CultureInfo.InvariantCulture) }; break;
                case "--count": o = o with { Count = long.Parse(args[++i], CultureInfo.InvariantCulture) }; break;
                case "--idle-ms": o = o with { IdleMs = int.Parse(args[++i], CultureInfo.InvariantCulture) }; break;
                case "--once": o = o with { Once = true }; break;
                case "-h" or "--help": o = o with { Help = true }; break;
                default: throw new ArgumentException($"unknown option: {args[i]}");
            }
        }

        return o;
    }

    public static void PrintUsage()
    {
        Console.WriteLine("""
            JobPolls.Consumer — consume Redis Stream events, dedup with the native core, store rows.

              --redis HOST:PORT   (default localhost:6379)
              --stream NAME       (default jobs:events)
              --group NAME        (default jobs:consumers)
              --name ID           consumer name
              --store PATH        SQLite store (default jobpolls.consumer.db)
              --batch N           events per read (default 64)
              --threshold N       SimHash Hamming threshold (default 3)
              --count N           stop after N stored events
              --idle-ms MS        exit after this much idle (default 30000)
              --once              drain once, then exit
            """);
    }
}
