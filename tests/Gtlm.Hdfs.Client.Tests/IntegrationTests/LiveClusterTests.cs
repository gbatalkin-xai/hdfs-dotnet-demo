namespace Gtlm.Hdfs.Client.Tests.IntegrationTests;

using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Gtlm.Hdfs.Client.BlockReading;
using Gtlm.Hdfs.Client.Configuration;
using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Net;

/// <summary>
/// Integration tests against a real Hadoop cluster (Docker Compose).
/// Skipped unless HDFS_TEST_ENABLED=true environment variable is set.
///
/// Setup:
///   cd tests/Gtlm.Hdfs.Client.Tests/IntegrationTests
///   ./setup-test-data.sh
///   export HDFS_TEST_ENABLED=true
///   dotnet test --filter "Category=Integration"
/// </summary>
[Trait("Category", "Integration")]
public class LiveClusterTests : IAsyncLifetime
{
    private const string NameNodeWebHdfs = "http://localhost:9870";
    private const string DataNodeIp = "127.0.0.1";
    private const int DataNodePort = 9866;

    private static readonly string TestDataDir =
        Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "IntegrationTests");

    private readonly HdfsClientOptions _options = new()
    {
        ConnectTimeout = TimeSpan.FromSeconds(10),
        ReadTimeout = TimeSpan.FromSeconds(30),
    };

    private PeerCache _peerCache = null!;

    private static void EnsureEnabled() => IntegrationGuard.RequireCluster();

    public Task InitializeAsync()
    {
        _peerCache = new PeerCache(4, TimeSpan.FromMinutes(5));
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _peerCache.DisposeAsync();
    }

    [Fact]
    public async Task ReadSmallFile_MatchesUploadedContent()
    {
        EnsureEnabled();
        var expectedPath = Path.Combine(TestDataDir, "testdata-small.dat");
        IntegrationGuard.RequireFile(expectedPath);
        var expected = await File.ReadAllBytesAsync(expectedPath);

        var blocks = await GetBlockLocationsAsync("/test/small.dat", 0, expected.Length);
        Assert.NotEmpty(blocks);

        using var ms = new MemoryStream();
        foreach (var block in blocks)
        {
            long offsetInBlock = 0;
            long length = block.Block.NumBytes;

            var peer = await Peer.ConnectAsync(block.Locations[0], _options);
            await using var reader = await RemoteBlockReader.CreateAsync(
                "/test/small.dat", block.Block, block.Token,
                offsetInBlock, length, true, "integration-test",
                peer, _peerCache, _options);

            await reader.CopyToAsync(ms);
        }

        Assert.Equal(expected, ms.ToArray());
    }

    [Fact]
    public async Task ReadExactChunkFile_MatchesContent()
    {
        EnsureEnabled();
        var expectedPath = Path.Combine(TestDataDir, "testdata-exact.dat");
        IntegrationGuard.RequireFile(expectedPath);
        var expected = await File.ReadAllBytesAsync(expectedPath);

        var blocks = await GetBlockLocationsAsync("/test/exact.dat", 0, expected.Length);
        Assert.NotEmpty(blocks);

        var block = blocks[0];
        var peer = await Peer.ConnectAsync(block.Locations[0], _options);
        await using var reader = await RemoteBlockReader.CreateAsync(
            "/test/exact.dat", block.Block, block.Token,
            0, block.Block.NumBytes, true, "integration-test",
            peer, _peerCache, _options);

        using var ms = new MemoryStream();
        await reader.CopyToAsync(ms);

        Assert.Equal(expected, ms.ToArray());
    }

    [Fact]
    public async Task ReadMediumFile_AllBlocks_MatchesContent()
    {
        EnsureEnabled();
        var expectedPath = Path.Combine(TestDataDir, "testdata-medium.dat");
        IntegrationGuard.RequireFile(expectedPath);
        var expected = await File.ReadAllBytesAsync(expectedPath);

        var blocks = await GetBlockLocationsAsync("/test/medium.dat", 0, expected.Length);
        Assert.NotEmpty(blocks);

        using var ms = new MemoryStream();
        foreach (var block in blocks)
        {
            var peer = _peerCache.TryGet(block.Locations[0])
                ?? await Peer.ConnectAsync(block.Locations[0], _options);

            await using var reader = await RemoteBlockReader.CreateAsync(
                "/test/medium.dat", block.Block, block.Token,
                0, block.Block.NumBytes, true, "integration-test",
                peer, _peerCache, _options);

            await reader.CopyToAsync(ms);
        }

        Assert.Equal(expected, ms.ToArray());
    }

    [Fact]
    public async Task ReadWithOffset_NonAligned_ReadsCorrectSubset()
    {
        EnsureEnabled();
        var expectedPath = Path.Combine(TestDataDir, "testdata-small.dat");
        IntegrationGuard.RequireFile(expectedPath);
        var fullData = await File.ReadAllBytesAsync(expectedPath);
        if (fullData.Length < 512) throw new SkipTestException("Test file too small");

        // Read 256 bytes starting at offset 100 (non-chunk-aligned)
        long offset = 100;
        long length = 256;
        var expected = fullData[(int)offset..(int)(offset + length)];

        var blocks = await GetBlockLocationsAsync("/test/small.dat", offset, length);
        var block = blocks[0];

        var peer = await Peer.ConnectAsync(block.Locations[0], _options);
        await using var reader = await RemoteBlockReader.CreateAsync(
            "/test/small.dat", block.Block, block.Token,
            offset, length, true, "integration-test",
            peer, _peerCache, _options);

        using var ms = new MemoryStream();
        await reader.CopyToAsync(ms);

        Assert.Equal(expected, ms.ToArray());
    }

    [Fact]
    public async Task PeerCache_ReusesConnection()
    {
        EnsureEnabled();
        var blocks = await GetBlockLocationsAsync("/test/small.dat", 0, 1024);
        if (blocks.Count == 0) throw new SkipTestException("No blocks found");

        var block = blocks[0];
        var dn = block.Locations[0];

        // First read
        var peer1 = await Peer.ConnectAsync(dn, _options);
        await using (var reader = await RemoteBlockReader.CreateAsync(
            "/test/small.dat", block.Block, block.Token,
            0, block.Block.NumBytes, true, "test",
            peer1, _peerCache, _options))
        {
            using var ms = new MemoryStream();
            await reader.CopyToAsync(ms);
        }
        // reader.DisposeAsync returned peer1 to cache

        // Second read should reuse the cached peer
        var cachedPeer = _peerCache.TryGet(dn);
        Assert.NotNull(cachedPeer);
    }

    // --- WebHDFS helper to get block locations (Phase 1 workaround) ---

    private static async Task<IReadOnlyList<LocatedBlock>> GetBlockLocationsAsync(
        string path, long offset, long length)
    {
        using var http = new HttpClient();
        var url = $"{NameNodeWebHdfs}/webhdfs/v1{path}?op=GET_BLOCK_LOCATIONS&offset={offset}&length={length}";
        var response = await http.GetFromJsonAsync<WebHdfsBlockLocationsResponse>(url,
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

        if (response?.LocatedBlocks?.Block == null)
            return [];

        return response.LocatedBlocks.Block.Select(b => new LocatedBlock
        {
            Block = new ExtendedBlock(
                b.Block.PoolId,
                b.Block.BlockId,
                b.Block.GenerationStamp,
                b.Block.NumBytes),
            Offset = b.StartOffset,
            Token = BlockToken.Empty, // Non-secure cluster
            Locations = b.Locations?.Select(loc => new DatanodeInfo
            {
                IpAddress = DataNodeIp, // Use mapped port from docker
                HostName = loc.HostName,
                DatanodeUuid = loc.DatanodeUuid,
                XferPort = DataNodePort, // Use mapped port
                InfoPort = (int)loc.InfoPort,
                IpcPort = (int)loc.IpcPort,
            }).ToList() ?? [],
        }).ToList();
    }

    // --- WebHDFS JSON models ---

    private sealed class WebHdfsBlockLocationsResponse
    {
        public WebHdfsLocatedBlocks? LocatedBlocks { get; set; }
    }

    private sealed class WebHdfsLocatedBlocks
    {
        [JsonPropertyName("locatedBlocks")]
        public List<WebHdfsLocatedBlock>? Block { get; set; }
    }

    private sealed class WebHdfsLocatedBlock
    {
        public long StartOffset { get; set; }
        public WebHdfsBlock Block { get; set; } = null!;
        public List<WebHdfsDataNode>? Locations { get; set; }
    }

    private sealed class WebHdfsBlock
    {
        [JsonPropertyName("blockPoolId")]
        public string PoolId { get; set; } = "";
        public long BlockId { get; set; }
        [JsonPropertyName("generationStamp")]
        public long GenerationStamp { get; set; }
        public long NumBytes { get; set; }
    }

    private sealed class WebHdfsDataNode
    {
        public string HostName { get; set; } = "";
        public string DatanodeUuid { get; set; } = "";
        public long InfoPort { get; set; }
        public long IpcPort { get; set; }
    }
}

/// <summary>
/// Guards integration tests: throws to skip when the HDFS cluster is not available.
/// </summary>
internal static class IntegrationGuard
{
    public static void RequireCluster()
    {
        if (Environment.GetEnvironmentVariable("HDFS_TEST_ENABLED") != "true")
            throw new SkipTestException("HDFS cluster not available (set HDFS_TEST_ENABLED=true)");
    }

    public static void RequireFile(string path)
    {
        if (!File.Exists(path))
            throw new SkipTestException($"Test data file not found: {path}");
    }
}

/// <summary>
/// Thrown to dynamically skip a test. xUnit treats this as a skip, not a failure.
/// </summary>
internal sealed class SkipTestException : Exception
{
    public SkipTestException(string message) : base(message) { }
}
