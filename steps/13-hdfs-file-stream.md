# Step 13: HdfsFileStream

**Phase:** 2 (Full Client)
**Prerequisites:** Step 09 (RemoteBlockReader), Step 12 (NameNode RPC)
**Produces:** High-level `Stream` that reads HDFS files transparently

---

## Objective

Implement `HdfsFileStream` -- a `Stream` that takes an HDFS file path, resolves it to
block locations via the NameNode, and chains `RemoteBlockReader` instances to provide
seamless sequential reading. Consumers can use it like any .NET `Stream` without
knowledge of HDFS block structure.

---

## Tasks

### 13.1 `HdfsFileStream` Class

**File:** `src/Gtlm.Hdfs.Client/BlockReading/HdfsFileStream.cs`

```csharp
namespace Gtlm.Hdfs.Client.BlockReading;

public sealed class HdfsFileStream : Stream, IAsyncDisposable
{
    private readonly string _path;
    private readonly IReadOnlyList<LocatedBlock> _blocks;
    private readonly BlockReaderFactory _readerFactory;
    private readonly long _fileLength;

    private int _currentBlockIndex;
    private RemoteBlockReader? _currentReader;
    private long _position;
    private bool _disposed;

    private HdfsFileStream(
        string path,
        IReadOnlyList<LocatedBlock> blocks,
        long fileLength,
        BlockReaderFactory readerFactory);

    /// <summary>
    /// Open an HDFS file for reading.
    /// Resolves block locations from the NameNode.
    /// </summary>
    public static async Task<HdfsFileStream> OpenAsync(
        string path,
        NameNodeRpcClient nameNode,
        BlockReaderFactory readerFactory,
        CancellationToken ct = default);

    public override async ValueTask<int> ReadAsync(
        Memory<byte> buffer, CancellationToken ct = default);

    // Seeking (limited: forward-only within current or subsequent blocks)
    public override long Seek(long offset, SeekOrigin origin);
}
```

### 13.2 `OpenAsync` Factory

```csharp
public static async Task<HdfsFileStream> OpenAsync(
    string path,
    NameNodeRpcClient nameNode,
    BlockReaderFactory readerFactory,
    CancellationToken ct = default)
{
    // 1. Get file info to determine file length
    var fileStatus = await nameNode.GetFileInfoAsync(path, ct)
        ?? throw new FileNotFoundException($"HDFS file not found: {path}");

    if (fileStatus.IsDirectory)
        throw new IOException($"Cannot read directory as file: {path}");

    // 2. Get all block locations
    var blocks = await nameNode.GetBlockLocationsAsync(path, 0, fileStatus.Length, ct);

    if (blocks.Count == 0 && fileStatus.Length > 0)
        throw new IOException($"No block locations returned for non-empty file: {path}");

    return new HdfsFileStream(path, blocks, fileStatus.Length, readerFactory);
}
```

### 13.3 `ReadAsync` -- Block Chaining

```csharp
public override async ValueTask<int> ReadAsync(
    Memory<byte> buffer, CancellationToken ct = default)
{
    if (_disposed || _position >= _fileLength)
        return 0;

    // Open a reader for the current block if needed
    if (_currentReader is null || _currentReader.IsComplete)
    {
        await AdvanceToNextBlockAsync(ct);
        if (_currentReader is null)
            return 0; // Past last block
    }

    int bytesRead = await _currentReader.ReadAsync(buffer, ct);

    if (bytesRead == 0 && !_currentReader.IsComplete)
    {
        // Current reader returned 0 but isn't complete -- try next block
        await AdvanceToNextBlockAsync(ct);
        if (_currentReader is null)
            return 0;
        bytesRead = await _currentReader.ReadAsync(buffer, ct);
    }

    _position += bytesRead;
    return bytesRead;
}

private async ValueTask AdvanceToNextBlockAsync(CancellationToken ct)
{
    // Dispose current reader (returns peer to cache)
    if (_currentReader is not null)
    {
        await _currentReader.DisposeAsync();
        _currentReader = null;
        _currentBlockIndex++;
    }

    if (_currentBlockIndex >= _blocks.Count)
    {
        _currentReader = null;
        return;
    }

    var block = _blocks[_currentBlockIndex];

    // Calculate offset within this block
    long offsetInBlock = _position - block.Offset;
    long remainingInBlock = block.Block.NumBytes - offsetInBlock;

    // Choose the best DataNode (first in list = nearest by network distance)
    var dataNode = block.Locations[0];

    _currentReader = await _readerFactory.CreateRemoteReaderAsync(
        _path, block, dataNode, offsetInBlock, remainingInBlock, ct);
}
```

### 13.4 `HdfsClient` -- Top-Level API

**File:** `src/Gtlm.Hdfs.Client/HdfsClient.cs`

```csharp
namespace Gtlm.Hdfs.Client;

/// <summary>
/// High-level HDFS client. Opens files for reading.
/// </summary>
public sealed class HdfsClient : IAsyncDisposable
{
    private readonly NameNodeRpcClient _nameNode;
    private readonly BlockReaderFactory _readerFactory;
    private readonly PeerCache _peerCache;

    public HdfsClient(HdfsClientOptions options, ILoggerFactory? loggerFactory = null);

    /// <summary>Connect to the NameNode.</summary>
    public async Task ConnectAsync(CancellationToken ct = default);

    /// <summary>Open a file for reading. Returns a Stream.</summary>
    public async Task<Stream> OpenReadAsync(string path, CancellationToken ct = default);

    /// <summary>Get file metadata.</summary>
    public async Task<HdfsFileStatus?> GetFileInfoAsync(
        string path, CancellationToken ct = default);

    /// <summary>List directory contents.</summary>
    public async Task<IReadOnlyList<HdfsFileStatus>> ListDirectoryAsync(
        string path, CancellationToken ct = default);

    public async ValueTask DisposeAsync();
}
```

Usage example:

```csharp
await using var client = new HdfsClient(new HdfsClientOptions
{
    NameNodeHost = "namenode.cluster.local",
    NameNodePort = 8020,
});
await client.ConnectAsync();

await using var stream = await client.OpenReadAsync("/data/input.parquet");
await using var fileStream = File.Create("/local/output.parquet");
await stream.CopyToAsync(fileStream);
```

### 13.5 Configuration Extension

**File:** `src/Gtlm.Hdfs.Client/Configuration/HdfsClientOptions.cs` (update)

Add NameNode connection settings:

```csharp
public string NameNodeHost { get; set; } = "localhost";
public int NameNodePort { get; set; } = 8020;
```

---

## Acceptance Criteria

- [ ] `HdfsFileStream.OpenAsync` resolves a file path to block locations
- [ ] `ReadAsync` seamlessly chains across multiple blocks
- [ ] Position tracking is accurate across block boundaries
- [ ] Block reader disposal returns peers to cache between blocks
- [ ] End-of-file returns 0 bytes
- [ ] `HdfsClient` provides a clean top-level API
- [ ] `CopyToAsync` works correctly for large files
- [ ] `FileNotFoundException` thrown for nonexistent paths
- [ ] Integration test: read a multi-block file end-to-end via `HdfsClient`
