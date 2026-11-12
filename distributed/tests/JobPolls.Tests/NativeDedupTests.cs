using JobPolls.Contracts;
using JobPolls.Native;
using Xunit;

namespace JobPolls.Tests;

public class NativeDedupTests
{
    [Fact]
    public void ReportsVersion()
    {
        Assert.Contains("jobpolls-native", NativeDedup.Version());
    }

    [Fact]
    public void NormalizesCasePunctuationAndWhitespace()
    {
        Assert.Equal("software developer intern", NativeDedup.Normalize("  Software   Developer, INTERN!  "));
    }

    [Fact]
    public void SimHashIsStableAcrossNormalization()
    {
        var a = NativeDedup.SimHash("Software Engineer Intern");
        var b = NativeDedup.SimHash("software   engineer intern!");
        Assert.Equal(a, b);
        Assert.Equal(0u, NativeDedup.Hamming(a, b));
    }

    [Fact]
    public void DifferentRolesDiffer()
    {
        var a = NativeDedup.SimHash("Software Engineer Intern");
        var b = NativeDedup.SimHash("Mechanical Design Engineer");
        Assert.True(NativeDedup.Hamming(a, b) > 3);
    }

    [Fact]
    public void DedupBatchFlagsNearDuplicates()
    {
        var (duplicates, hashes) = NativeDedup.DedupBatch(
        [
            "Software Engineer Intern",
            "software engineer intern",
            "Backend Developer Co-op",
        ], threshold: 3);

        Assert.False(duplicates[0]);
        Assert.True(duplicates[1]);
        Assert.False(duplicates[2]);
        Assert.NotEqual(0UL, hashes[0]);
    }

    [Fact]
    public void EmptyBatchIsSafe()
    {
        var (duplicates, hashes) = NativeDedup.DedupBatch([]);
        Assert.Empty(duplicates);
        Assert.Empty(hashes);
    }

    [Theory]
    [InlineData("Toronto, ON", "canada")]
    [InlineData("Montréal, QC", "canada")]
    [InlineData("Seattle, WA", "us")]
    [InlineData("New York, NY", "us")]
    public void DetectsRegion(string location, string expected)
    {
        Assert.Equal(expected, RegionDetector.Classify(location));
    }
}
