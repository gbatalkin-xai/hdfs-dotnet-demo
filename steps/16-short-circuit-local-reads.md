# Step 16: Short-Circuit Local Reads

**Phase:** 2 (Full Client)
**Prerequisites:** Step 09 (RemoteBlockReader), Step 12 (NameNode RPC)
**Produces:** `BlockReading/LocalBlockReader.cs` -- direct file reads for co-located clients

---

## Objective

When the client runs on the same machine as a DataNode, bypass the TCP connection and
read block data directly from the local filesystem. This eliminates network overhead
and is significantly faster for co-located workloads (e.g., MapReduce tasks, Spark
executors on HDFS nodes).

---

## Tasks

### 16.1 Short-Circuit Protocol Overview

HDFS short-circuit reads use a Unix domain socket to request file descriptors from the
DataNode:

1. Client connects to the DataNode's domain socket
   (`dfs.domain.socket.path`, e.g., `/var/run/hdfs-sockets/dn`)
2. Client sends `OpRequestShortCircuitAccessProto` with the block ID
3. DataNode validates the token and sends back the file descriptors for the block
   data file and checksum meta file via `SCM_RIGHTS` (Unix FD passing)
4. Client reads the block data and checksums directly from the local files

### 16.2 `LocalBlockReader` Class

**File:** `src/Gtlm.Hdfs.Client/BlockReading/LocalBlockReader.cs`

```csharp
namespace Gtlm.Hdfs.Client.BlockReading;

/// <summary>
/// Reads HDFS block data from the local filesystem using short-circuit access.
/// Requires the client to be co-located with the DataNode.
/// </summary>
public sealed class LocalBlockReader : Stream, IBlockReader
{
    private readonly FileStream _dataFile;
    private readonly FileStream _metaFile;
    private readonly DataChecksum _checksum;
    private readonly bool _verifyChecksum;
    private readonly long _startOffset;
    private readonly long _bytesToRead;
    private long _bytesRead;

    public static async Task<LocalBlockReader?> TryCreateAsync(
        ExtendedBlock block,
        DatanodeInfo localDataNode,
        long startOffset,
        long length,
        bool verifyChecksum,
        HdfsClientOptions options,
        CancellationToken ct = default);

    public override async ValueTask<int> ReadAsync(
        Memory<byte> buffer, CancellationToken ct = default);
}
```

### 16.3 `BlockReaderFactory` Integration

Update `BlockReaderFactory` to try short-circuit first:

```csharp
public async Task<IBlockReader> CreateReaderAsync(...)
{
    // Try short-circuit if DataNode is local
    if (IsLocalDataNode(dataNode))
    {
        var localReader = await LocalBlockReader.TryCreateAsync(
            block, dataNode, offset, length, verifyChecksum, _options, ct);
        if (localReader is not null)
            return localReader;
    }

    // Fall back to remote reader
    return await CreateRemoteReaderAsync(...);
}
```

### 16.4 Shared Memory Slot Protocol

For newer Hadoop versions, short-circuit reads use shared memory segments instead of
FD passing. This allows the DataNode to track which blocks are being read and manage
cache accordingly.

```csharp
/// <summary>
/// Request a shared memory slot from the DataNode.
/// Used for zero-copy reads and cache management.
/// </summary>
private async Task<ShortCircuitShmSlot> RequestShmSlotAsync(...)
{
    // Send OpRequestShortCircuitAccessProto
    // Receive ShortCircuitShmResponseProto with slot info
    // Map shared memory segment
}
```

### 16.5 Platform Considerations

- **Unix domain sockets:** Supported on Linux and macOS in .NET via
  `Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified)`
- **FD passing (SCM_RIGHTS):** Not directly supported in managed .NET. Options:
  - P/Invoke to `sendmsg`/`recvmsg` with `cmsg` for SCM_RIGHTS
  - Use a native interop library
  - Alternative: DataNode domain socket protocol without FD passing (newer Hadoop
    versions support this via shared memory)
- **Windows:** Short-circuit reads are not supported (no Unix domain sockets for
  FD passing). Fall back to remote reads.

---

## Acceptance Criteria

- [ ] Local reads work when client and DataNode are on the same machine
- [ ] Checksum verification works with locally-read data
- [ ] Falls back to remote reads when short-circuit is not available
- [ ] Domain socket connection handles permission errors gracefully
- [ ] Performance improvement measurable vs remote reads on same host
