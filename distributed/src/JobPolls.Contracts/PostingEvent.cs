namespace JobPolls.Contracts;

/// <summary>Immutable event published to the Redis Stream by the publisher.</summary>
public sealed record PostingEvent(
    string Key,
    string Source,
    string CompanyKey,
    string Company,
    string Title,
    string Location,
    string? PostedAt,
    string? PostedText,
    string Url,
    string FirstSeen,
    string LastSeen,
    bool Open,
    string Origin,
    bool Baseline,
    string EventType)
{
    public const string New = "new";
    public const string Updated = "updated";
    public const string Closed = "closed";
}

/// <summary>Row stored by the consumer and served by the API.</summary>
public sealed record StoredPosting(
    string Key,
    string Source,
    string Company,
    string Title,
    string Location,
    string? PostedAt,
    string Url,
    string FirstSeen,
    string LastSeen,
    bool Open,
    string Region,
    ulong SimHash);

public static class RegionDetector
{
    private static readonly string[] CanadaTokens =
    [
        "canada", "ontario", "quebec", "québec", "british columbia", "alberta",
        "manitoba", "saskatchewan", "nova scotia", "new brunswick", "toronto",
        "ottawa", "vancouver", "montreal", "montréal", "waterloo", "kitchener",
        "calgary", "edmonton", "mississauga", "halifax", "winnipeg", "burnaby",
        ", on", ", qc", ", bc", ", ab", ", mb", ", sk", ", ns", ", nb",
    ];

    public static string Classify(string? location)
    {
        var value = (location ?? "").ToLowerInvariant();
        foreach (var token in CanadaTokens)
        {
            if (value.Contains(token, StringComparison.Ordinal))
                return "canada";
        }

        return "us";
    }
}
