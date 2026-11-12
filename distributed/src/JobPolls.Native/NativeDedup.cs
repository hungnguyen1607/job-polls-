using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace JobPolls.Native;

/// <summary>
/// Managed wrapper over the C ABI in <c>native/include/jobpolls.h</c>.
/// Buffer ownership: the caller allocates inputs + outputs; the native side
/// never returns heap pointers across the boundary.
/// </summary>
public static unsafe partial class NativeDedup
{
    private const string Library = "jobpolls_native";

    static NativeDedup()
    {
        NativeLibrary.SetDllImportResolver(typeof(NativeDedup).Assembly, Resolve);
    }

    [LibraryImport(Library)]
    private static partial nint jp_version();

    [LibraryImport(Library)]
    private static partial nuint jp_normalize(byte* input, byte* output, nuint outputCapacity);

    [LibraryImport(Library)]
    private static partial ulong jp_simhash64(byte* text);

    [LibraryImport(Library)]
    private static partial uint jp_hamming64(ulong a, ulong b);

    [LibraryImport(Library)]
    private static partial int jp_dedup_batch(
        byte** texts, nuint count, uint threshold, byte* duplicates, ulong* hashes);

    [LibraryImport(Library)]
    private static partial ulong jp_arena_bytes();

    [LibraryImport(Library)]
    private static partial void jp_arena_reset();

    public static string Version()
    {
        var ptr = jp_version();
        return ptr == 0 ? "unknown" : Marshal.PtrToStringUTF8(ptr) ?? "unknown";
    }

    public static string Normalize(string input)
    {
        var source = Marshal.StringToCoTaskMemUTF8(input ?? "");
        var output = (byte*)NativeMemory.Alloc(4096);
        try
        {
            var length = jp_normalize((byte*)source, output, 4096);
            if (length == nuint.MaxValue) length = 4095;
            return Marshal.PtrToStringUTF8((nint)output, (int)length) ?? "";
        }
        finally
        {
            Marshal.FreeCoTaskMem(source);
            NativeMemory.Free(output);
        }
    }

    public static ulong SimHash(string text)
    {
        var ptr = Marshal.StringToCoTaskMemUTF8(text ?? "");
        try
        {
            return jp_simhash64((byte*)ptr);
        }
        finally
        {
            Marshal.FreeCoTaskMem(ptr);
        }
    }

    public static uint Hamming(ulong a, ulong b) => jp_hamming64(a, b);

    /// <summary>
    /// Near-duplicate detection within a batch. <c>duplicates[i]</c> is true when
    /// item i is within <paramref name="threshold"/> Hamming bits of an earlier item.
    /// </summary>
    public static (bool[] Duplicates, ulong[] Hashes) DedupBatch(
        IReadOnlyList<string> texts, uint threshold = 3)
    {
        var n = texts.Count;
        if (n == 0) return (Array.Empty<bool>(), Array.Empty<ulong>());

        var handles = new nint[n];
        var pointers = (byte**)NativeMemory.Alloc((nuint)(n * sizeof(nint)));
        var duplicates = (byte*)NativeMemory.Alloc((nuint)n);
        var hashes = (ulong*)NativeMemory.Alloc((nuint)(n * sizeof(ulong)));

        try
        {
            for (var i = 0; i < n; i++)
            {
                handles[i] = Marshal.StringToCoTaskMemUTF8(texts[i] ?? "");
                pointers[i] = (byte*)handles[i];
            }

            var rc = jp_dedup_batch(pointers, (nuint)n, threshold, duplicates, hashes);
            if (rc != 0) throw new InvalidOperationException("jp_dedup_batch failed");

            var dup = new bool[n];
            var hash = new ulong[n];
            for (var i = 0; i < n; i++)
            {
                dup[i] = duplicates[i] != 0;
                hash[i] = hashes[i];
            }

            return (dup, hash);
        }
        finally
        {
            for (var i = 0; i < n; i++)
            {
                if (handles[i] != 0) Marshal.FreeCoTaskMem(handles[i]);
            }

            NativeMemory.Free(pointers);
            NativeMemory.Free(duplicates);
            NativeMemory.Free(hashes);
        }
    }

    public static ulong ArenaBytes => jp_arena_bytes();

    public static void ArenaReset() => jp_arena_reset();

    private static nint Resolve(string name, Assembly assembly, DllImportSearchPath? path)
    {
        if (name != Library) return 0;

        foreach (var candidate in Candidates())
        {
            if (File.Exists(candidate) && NativeLibrary.TryLoad(candidate, out var handle))
                return handle;
        }

        return NativeLibrary.TryLoad("libjobpolls_native.so", out var fallback) ? fallback : 0;
    }

    private static IEnumerable<string> Candidates()
    {
        var env = Environment.GetEnvironmentVariable("JOBPOLLS_NATIVE");
        if (!string.IsNullOrEmpty(env)) yield return env;

        var baseDir = AppContext.BaseDirectory;
        yield return Path.Combine(baseDir, "libjobpolls_native.so");
        yield return Path.Combine(baseDir, "native", "build", "libjobpolls_native.so");

        var dir = new DirectoryInfo(baseDir);
        for (var i = 0; i < 8 && dir is not null; i++, dir = dir.Parent)
        {
            yield return Path.Combine(dir.FullName, "native", "build", "libjobpolls_native.so");
        }
    }
}
