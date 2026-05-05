# Step 09: RemoteBlockReader

**Phase:** 1 (MVP)
**Prerequisites:** Steps 02-08 (all prior components)
**Produces:** `BlockReading/RemoteBlockReader.cs` -- the core `Stream` implementation

---

## Objective

Implement the `RemoteBlockReader` class that ties together all prior components into a
`Stream`-compatible block reader. This is the primary public API of the library for
Phase 1. Consumers receive a `Stream` they can read from -- under the hood it manages
the DataNode connection, packet streaming, checksum verification, and connection cleanup.

This is the direct .NET equivalent of Java's `BlockReaderRemote`.

---

## Tasks

### 9.1 `IBlockReader` Interface

**File:** `src/Gtlm.Hdfs.Client/BlockReading/IBlockReader.cs`

```csharp
namespace Gtlm.Hdfs.Client.BlockReading;

/// <summary>
/// Interface for HDFS block readers.
/// Allows different implementations (remote, local short-circuit, etc.).
/// </summary>
public interface IBlockReader : IAsyncDisposable
{
    /// <summary>Read block data into the buffer.</summary>
    ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default);

    /// <summary>Total bytes available to read from this block.</summary>
    long Length { get; }

    /// <summary>Number of bytes read so far.</summary>
    long Position { get; }

    /// <summary>True if all requested bytes have been read.</summary>
    bool IsComplete { get; }
}
```

### 9.2 `RemoteBlockReader` Class -- Full Implementation

**File:** `src/Gtlm.Hdfs.Client/BlockReading/RemoteBlockReader.cs`

```csharp
namespace Gtlm.Hdfs.Client.BlockReading;

using System.Buffers;
using Gtlm.Hdfs.Client.Checksum;
using Gtlm.Hdfs.Client.Configuration;
using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Net;
using Gtlm.Hdfs.Client.Protocol;
using Gtlm.Hdfs.Client.Proto;
using Microsoft.Extensions.Logging;

public sealed class RemoteBlockReader : Stream, IBlockReader
{
    // Immutable state (set in constructor)
    private readonly Peer _peer;
    private readonly DataChecksum _checksum;
    private readonly bool _verifyChecksum;
    private readonly long _startOffset;
    private readonly long _bytesToRead;
    private readonly string _filename;
    private readonly long _blockId;
    private readonly PeerCache? _peerCache;
    private readonly PacketReader _packetReader;
    private readonly ILogger? _logger;

    // Mutable read state
    private long _bytesRead;
    private byte[]? _currentPacketData;     // Rented from ArrayPool
    private int _packetDataOffset;
    private int _packetDataRemaining;
    private bool _lastPacketReceived;
    private bool _disposed;

    // Alignment: first packet may start before startOffset
    private int _skipBytesInFirstPacket;

    // ... constructor and factory below
}
```

### 9.3 Factory Method: `CreateAsync`

This is the .NET equivalent of `BlockReaderRemote.newBlockReader()`.

```csharp
/// <summary>
/// Create a new RemoteBlockReader and initiate the OP_READ_BLOCK handshake.
/// On success, the reader is ready to stream data via ReadAsync.
/// </summary>
public static async Task<RemoteBlockReader> CreateAsync(
    string file,
    ExtendedBlock block,
    BlockToken token,
    long startOffset,
    long length,
    bool verifyChecksum,
    string clientName,
    Peer peer,
    PeerCache? peerCache,
    HdfsClientOptions options,
    ILogger? logger = null,
    CancellationToken ct = default)
{
    // 1. Send OP_READ_BLOCK request
    await DataTransferSender.SendReadBlockAsync(
        peer, block, token, clientName, startOffset, length,
        sendChecksums: verifyChecksum, ct);

    // 2. Receive and parse response
    var response = await DataTransferReceiver.ReceiveBlockOpResponseAsync(peer, ct);

    // 3. Validate status
    var checksumInfo = DataTransferReceiver.ValidateAndExtractChecksumInfo(
        response, peer.DataNode, block, file);

    // 4. Validate chunk alignment
    DataTransferReceiver.ValidateChunkOffset(
        checksumInfo.ChunkOffset, startOffset, checksumInfo.BytesPerChecksum, file);

    // 5. Create checksum verifier
    var checksum = DataChecksum.Create(checksumInfo.ChecksumType, checksumInfo.BytesPerChecksum);

    // 6. Calculate bytes to skip in first packet (due to chunk alignment)
    int skipBytes = (int)(startOffset - checksumInfo.ChunkOffset);

    // 7. Total bytes that will arrive over the wire
    // (includes the alignment prefix we need to skip)
    long bytesNeededToFinish = length + skipBytes;

    logger?.LogDebug(
        "Created RemoteBlockReader for {File} block {BlockId} at offset {Offset}, " +
        "length {Length}, checksum {Type}/{BPC}, skip {Skip}",
        file, block.BlockId, startOffset, length,
        checksumInfo.ChecksumType, checksumInfo.BytesPerChecksum, skipBytes);

    return new RemoteBlockReader(
        peer, checksum, verifyChecksum, startOffset, length, file,
        block.BlockId, peerCache, skipBytes, logger);
}
```

### 9.4 Constructor

```csharp
private RemoteBlockReader(
    Peer peer,
    DataChecksum checksum,
    bool verifyChecksum,
    long startOffset,
    long bytesToRead,
    string filename,
    long blockId,
    PeerCache? peerCache,
    int skipBytesInFirstPacket,
    ILogger? logger)
{
    _peer = peer;
    _checksum = checksum;
    _verifyChecksum = verifyChecksum;
    _startOffset = startOffset;
    _bytesToRead = bytesToRead;
    _filename = filename;
    _blockId = blockId;
    _peerCache = peerCache;
    _skipBytesInFirstPacket = skipBytesInFirstPacket;
    _logger = logger;

    _packetReader = new PacketReader(peer.Input, checksum);
}
```

### 9.5 `ReadAsync` Implementation

```csharp
public override async ValueTask<int> ReadAsync(
    Memory<byte> buffer, CancellationToken ct = default)
{
    ObjectDisposedException.ThrowIf(_disposed, this);

    if (_bytesRead >= _bytesToRead)
        return 0;

    // If current packet exhausted, read next packet
    if (_packetDataRemaining == 0)
    {
        if (_lastPacketReceived)
            return 0;

        await ReadNextPacketInternalAsync(ct);
    }

    // Copy from current packet to caller's buffer
    int toCopy = Math.Min(buffer.Length, _packetDataRemaining);
    toCopy = (int)Math.Min(toCopy, _bytesToRead - _bytesRead);

    if (toCopy == 0)
        return 0;

    new ReadOnlySpan<byte>(_currentPacketData, _packetDataOffset, toCopy)
        .CopyTo(buffer.Span);

    _packetDataOffset += toCopy;
    _packetDataRemaining -= toCopy;
    _bytesRead += toCopy;

    return toCopy;
}
```

### 9.6 `ReadNextPacketInternalAsync`

```csharp
private async ValueTask ReadNextPacketInternalAsync(CancellationToken ct)
{
    // Return previous packet buffer to pool
    ReturnCurrentPacketBuffer();

    PacketData packet;
    do
    {
        packet = await _packetReader.ReadNextPacketAsync(ct);
    } while (packet.Header.Seqno == -1); // Skip heartbeat packets

    _lastPacketReceived = packet.IsLastPacket;

    if (packet.DataLength == 0)
    {
        // Empty last packet -- no data to process
        _packetDataRemaining = 0;
        return;
    }

    // Verify checksums before exposing data to caller
    if (_verifyChecksum && _checksum.ChecksumSize > 0)
    {
        _checksum.VerifyChunks(
            packet.Data.Span,
            packet.Checksums.Span,
            packet.OffsetInBlock);
    }

    // Handle first-packet alignment skip
    int dataOffset = 0;
    int dataLength = packet.DataLength;

    if (_skipBytesInFirstPacket > 0)
    {
        dataOffset = _skipBytesInFirstPacket;
        dataLength -= _skipBytesInFirstPacket;
        _skipBytesInFirstPacket = 0;
    }

    // Rent a buffer and copy the usable data
    _currentPacketData = ArrayPool<byte>.Shared.Rent(dataLength);
    packet.Data.Span.Slice(dataOffset, dataLength).CopyTo(_currentPacketData);
    _packetDataOffset = 0;
    _packetDataRemaining = dataLength;
}

private void ReturnCurrentPacketBuffer()
{
    if (_currentPacketData is not null)
    {
        ArrayPool<byte>.Shared.Return(_currentPacketData);
        _currentPacketData = null;
    }
}
```

### 9.7 `DisposeAsync`

```csharp
public override async ValueTask DisposeAsync()
{
    if (_disposed) return;
    _disposed = true;

    ReturnCurrentPacketBuffer();

    try
    {
        // Send final status to DataNode
        var status = _bytesRead >= _bytesToRead || _lastPacketReceived
            ? Status.ChecksumOk
            : Status.Error;

        await DataTransferSender.SendClientReadStatusAsync(_peer, status);
    }
    catch (Exception ex)
    {
        _logger?.LogWarning(ex, "Failed to send final read status to DataNode {DN}", _peer.DataNode);
    }

    // Return peer to cache if read was clean, otherwise dispose
    bool cleanClose = _bytesRead >= _bytesToRead || _lastPacketReceived;
    if (cleanClose && _peerCache is not null && !_peer.IsClosed)
    {
        _peerCache.Return(_peer);
    }
    else
    {
        await _peer.DisposeAsync();
    }
}

protected override void Dispose(bool disposing)
{
    if (disposing && !_disposed)
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
    base.Dispose(disposing);
}
```

### 9.8 Stream Boilerplate

```csharp
// --- Stream property overrides ---
public override bool CanRead => true;
public override bool CanSeek => false;
public override bool CanWrite => false;
public override long Length => _bytesToRead;
public override long Position
{
    get => _bytesRead;
    set => throw new NotSupportedException("RemoteBlockReader does not support seeking.");
}
public bool IsComplete => _bytesRead >= _bytesToRead;

// --- Unsupported operations ---
public override void Flush() { }
public override int Read(byte[] buffer, int offset, int count) =>
    ReadAsync(buffer.AsMemory(offset, count)).AsTask().GetAwaiter().GetResult();
public override long Seek(long offset, SeekOrigin origin) =>
    throw new NotSupportedException();
public override void SetLength(long value) =>
    throw new NotSupportedException();
public override void Write(byte[] buffer, int offset, int count) =>
    throw new NotSupportedException();
```

### 9.9 `BlockReaderFactory` -- Convenience Factory

**File:** `src/Gtlm.Hdfs.Client/BlockReading/BlockReaderFactory.cs`

```csharp
namespace Gtlm.Hdfs.Client.BlockReading;

using Gtlm.Hdfs.Client.Configuration;
using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Net;
using Microsoft.Extensions.Logging;

/// <summary>
/// Creates block readers. Manages the Peer lifecycle (connect or reuse from cache).
/// </summary>
public sealed class BlockReaderFactory
{
    private readonly HdfsClientOptions _options;
    private readonly PeerCache _peerCache;
    private readonly ILoggerFactory? _loggerFactory;

    public BlockReaderFactory(
        HdfsClientOptions options,
        PeerCache peerCache,
        ILoggerFactory? loggerFactory = null)
    {
        _options = options;
        _peerCache = peerCache;
        _loggerFactory = loggerFactory;
    }

    /// <summary>
    /// Create a block reader for the given block on the given DataNode.
    /// Reuses a cached peer if available, otherwise opens a new connection.
    /// </summary>
    public async Task<RemoteBlockReader> CreateRemoteReaderAsync(
        string file,
        LocatedBlock locatedBlock,
        DatanodeInfo dataNode,
        long offsetInBlock,
        long length,
        CancellationToken ct = default)
    {
        var peer = _peerCache.TryGet(dataNode)
            ?? await Peer.ConnectAsync(dataNode, _options, ct);

        try
        {
            return await RemoteBlockReader.CreateAsync(
                file: file,
                block: locatedBlock.Block,
                token: locatedBlock.Token,
                startOffset: offsetInBlock,
                length: length,
                verifyChecksum: _options.VerifyChecksum,
                clientName: _options.ClientName,
                peer: peer,
                peerCache: _peerCache,
                options: _options,
                logger: _loggerFactory?.CreateLogger<RemoteBlockReader>(),
                ct: ct);
        }
        catch
        {
            // If handshake fails, dispose the peer (don't return to cache)
            await peer.DisposeAsync();
            throw;
        }
    }
}
```

---

## Behavioral Parity with Java `BlockReaderRemote`

| Java Behavior | .NET Implementation |
|---------------|---------------------|
| `newBlockReader()` static factory | `CreateAsync()` static async factory |
| `firstChunkOffset` skip | `_skipBytesInFirstPacket` in first `ReadNextPacketInternalAsync` |
| `PacketReceiver.receiveNextPacket()` | `PacketReader.ReadNextPacketAsync()` |
| Checksum verify per packet | `_checksum.VerifyChunks()` in `ReadNextPacketInternalAsync` |
| `ClientReadStatusProto(CHECKSUM_OK)` on close | `DisposeAsync()` sends status |
| Return peer to `PeerCache` on clean close | `DisposeAsync()` checks `cleanClose` |
| `IOException` on corrupt data | `ChecksumException` (subclass of `IOException`) |
| `read(ByteBuffer buf)` | `ReadAsync(Memory<byte> buffer)` |

---

## Acceptance Criteria

- [ ] `CreateAsync` performs the full handshake: version + opcode + proto request,
      receives response, validates status and checksum info
- [ ] `ReadAsync` returns correct data matching the block content
- [ ] Checksum verification detects corrupted data
- [ ] First-packet alignment skip works correctly when `startOffset` is not chunk-aligned
- [ ] Heartbeat packets (seqno=-1) are silently skipped
- [ ] `DisposeAsync` sends `CHECKSUM_OK` status on clean completion
- [ ] Peer returned to cache on clean close, disposed on error
- [ ] `ArrayPool` buffers are returned (no leaks)
- [ ] `ReadAsync` returns 0 after all bytes read
- [ ] `Position` tracks bytes read accurately
- [ ] Works through standard `Stream` API (`stream.CopyToAsync`, `StreamReader`, etc.)
