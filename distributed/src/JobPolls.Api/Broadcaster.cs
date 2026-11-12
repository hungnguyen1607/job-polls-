using System.Collections.Concurrent;
using System.Threading.Channels;

namespace JobPolls.Api;

/// <summary>Fan-out to connected SSE clients (bounded per-subscriber, drop-oldest).</summary>
public sealed class Broadcaster
{
    private readonly ConcurrentDictionary<Guid, Channel<string>> _subs = new();

    public (Guid Id, ChannelReader<string> Reader) Subscribe()
    {
        var channel = Channel.CreateBounded<string>(new BoundedChannelOptions(1024)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
        });
        var id = Guid.NewGuid();
        _subs[id] = channel;
        return (id, channel.Reader);
    }

    public void Unsubscribe(Guid id)
    {
        if (_subs.TryRemove(id, out var channel)) channel.Writer.TryComplete();
    }

    public void Publish(string json)
    {
        foreach (var channel in _subs.Values) channel.Writer.TryWrite(json);
    }
}
