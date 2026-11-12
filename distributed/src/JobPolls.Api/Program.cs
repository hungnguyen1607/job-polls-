using System.Text;
using System.Threading.Channels;
using JobPolls.Api;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

var storePath = Environment.GetEnvironmentVariable("JOBPOLLS_STORE") ?? "jobpolls.consumer.db";
var redisEndpoint = Environment.GetEnvironmentVariable("JOBPOLLS_REDIS") ?? "localhost:6379";
var streamName = Environment.GetEnvironmentVariable("JOBPOLLS_STREAM") ?? "jobs:events";

builder.Services.AddSingleton(new StoreReader(storePath));
builder.Services.AddSingleton<Broadcaster>();
builder.Services.AddHostedService<StreamRelay>();

var app = builder.Build();

app.MapGet("/", () => Results.Content(Dashboard.Html, "text/html"));
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.MapGet("/api/jobs", (StoreReader store, string? region, string? q, int? limit) =>
    Results.Ok(store.Query(
        string.IsNullOrWhiteSpace(region) ? null : region,
        string.IsNullOrWhiteSpace(q) ? null : q,
        limit ?? 200)));

app.MapGet("/api/stats", async (StoreReader store) =>
{
    var (total, open, canada, us) = store.Stats();

    long streamLength = 0, consumerEvents = 0, duplicates = 0, errors = 0;
    try
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        using var mux = await ConnectionMultiplexer.ConnectAsync(redisEndpoint);
        var db = mux.GetDatabase();
        streamLength = await db.StreamLengthAsync(streamName);

        foreach (var entry in await db.HashGetAllAsync("jobs:stats"))
        {
            var value = long.TryParse(entry.Value.ToString(), out var parsed) ? parsed : 0;
            switch (entry.Name.ToString())
            {
                case "events": consumerEvents = value; break;
                case "duplicates": duplicates = value; break;
                case "errors": errors = value; break;
            }
        }
    }
    catch (Exception)
    {
        // Redis down: still serve store stats.
    }

    return Results.Ok(new
    {
        Total = total,
        Open = open,
        Canada = canada,
        Us = us,
        StreamLength = streamLength,
        ConsumerEvents = consumerEvents,
        Duplicates = duplicates,
        Errors = errors,
    });
});

app.MapGet("/api/events", async (HttpContext context, Broadcaster broadcaster) =>
{
    context.Response.Headers.ContentType = "text/event-stream";
    context.Response.Headers.CacheControl = "no-cache";
    context.Response.Headers.Connection = "keep-alive";

    var (id, reader) = broadcaster.Subscribe();
    try
    {
        await WriteAsync(context, ": connected\n\n");

        while (!context.RequestAborted.IsCancellationRequested)
        {
            using var wait = CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted);
            wait.CancelAfter(TimeSpan.FromSeconds(15));

            try
            {
                if (!await reader.WaitToReadAsync(wait.Token)) break;

                while (reader.TryRead(out var message))
                {
                    await WriteAsync(context, $"data: {message}\n\n");
                }
            }
            catch (OperationCanceledException) when (!context.RequestAborted.IsCancellationRequested)
            {
                await WriteAsync(context, ": ping\n\n");
            }
        }
    }
    catch (OperationCanceledException)
    {
        // client went away
    }
    finally
    {
        broadcaster.Unsubscribe(id);
    }
});

app.Run();

static async Task WriteAsync(HttpContext context, string text)
{
    await context.Response.WriteAsync(text, Encoding.UTF8, context.RequestAborted);
    await context.Response.Body.FlushAsync(context.RequestAborted);
}
