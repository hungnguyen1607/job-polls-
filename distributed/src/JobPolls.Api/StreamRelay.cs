using System.Text.Json;
using StackExchange.Redis;

namespace JobPolls.Api;

/// <summary>
/// Reads the Redis Stream as its own consumer group and pushes events to SSE
/// clients. Starting at "$" means the API only relays new events; history is
/// served from the store via /api/jobs.
/// </summary>
public sealed class StreamRelay(
    Broadcaster broadcaster,
    IConfiguration configuration,
    ILogger<StreamRelay> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var redis = configuration["JOBPOLLS_REDIS"] ?? "localhost:6379";
        var stream = configuration["JOBPOLLS_STREAM"] ?? "jobs:events";
        var group = configuration["JOBPOLLS_API_GROUP"] ?? "jobs:api";
        var name = $"{Environment.MachineName}-{Environment.ProcessId}";

        ConnectionMultiplexer? mux = null;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                mux ??= await ConnectionMultiplexer.ConnectAsync(redis);
                var db = mux.GetDatabase();

                try
                {
                    await db.StreamCreateConsumerGroupAsync(stream, group, "$", createStream: true);
                    logger.LogInformation("relay: created group {Group} on {Stream} at $", group, stream);
                }
                catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP", StringComparison.Ordinal))
                {
                    // already created
                }

                logger.LogInformation("relay: consuming {Stream} group {Group}", stream, group);

                while (!stoppingToken.IsCancellationRequested)
                {
                    var entries = await db.StreamReadGroupAsync(stream, group, name, ">", count: 64);
                    if (entries.Length == 0)
                    {
                        await Task.Delay(250, stoppingToken);
                        continue;
                    }

                    foreach (var entry in entries)
                    {
                        var payload = Field(entry, "payload");
                        if (!string.IsNullOrEmpty(payload))
                        {
                            var compact = Compact(payload);
                            broadcaster.Publish(compact);
                        }

                        await db.StreamAcknowledgeAsync(stream, group, entry.Id);
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "relay: redis error, retrying");
                mux?.Dispose();
                mux = null;
                await Task.Delay(HardDelay, stoppingToken);
            }
        }

        mux?.Dispose();
    }

    private static readonly TimeSpan HardDelay = TimeSpan.FromSeconds(2);

    private static string Field(StreamEntry entry, string name)
    {
        foreach (var value in entry.Values)
            if (value.Name.ToString() == name) return value.Value.ToString();
        return "";
    }

    private static string Compact(string payload)
    {
        try
        {
            using var doc = JsonDocument.Parse(payload);
            return JsonSerializer.Serialize(doc.RootElement);
        }
        catch (JsonException)
        {
            return payload;
        }
    }
}
