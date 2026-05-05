# HDFS BlockReaderRemote for .NET 10 -- Implementation Plan

## 1. Overview

This document proposes a native .NET 10 implementation of HDFS block-level reading,
modeled on Apache Hadoop's `BlockReaderRemote` (Java). The goal is high-throughput,
low-allocation sequential block reads directly from DataNodes using the HDFS Data
Transfer Protocol -- bypassing the HTTP overhead of WebHDFS.

**Reference implementation:**
[`BlockReaderRemote.java`](https://github.com/apache/hadoop/blob/5e1537769a9db2dc59e77e3980db978171df0588/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/client/impl/BlockReaderRemote.java#L89)

**Why native, not WebHDFS?** WebHDFS adds HTTP framing, JSON parsing, and an extra
NameNode hop for every data read. For bulk data pipelines (multi-GB reads), the native
binary protocol eliminates that overhead and enables zero-copy buffer management.

---

## 2. Scope

### In Scope (Phase 1)
- `OP_READ_BLOCK` protocol: connect to DataNode, send read request, stream packets
- Packet parsing: length-prefixed binary frames with protobuf headers
- Checksum verification: CRC32 and CRC32C per-chunk validation
- `Stream`-based API (`RemoteBlockReader : Stream`) for consumer compatibility
- Connection pooling / peer caching for DataNode socket reuse
- Block token support (tokens obtained from NameNode via external means)
- Configuration model mirroring relevant `dfs.client.*` settings

### In Scope (Phase 2)
- NameNode RPC client (`ClientProtocol`) for `getBlockLocations`, `getFileInfo`
- High-level `HdfsFileStream` that resolves paths to blocks and chains readers
- SASL/Kerberos authentication via `Kerberos.NET`
- Data transfer encryption (`dfs.encrypt.data.transfer`)
- Replica failover (try next DataNode on read error)
- Short-circuit local reads (when client is co-located with DataNode)

### Out of Scope
- Write path (`OP_WRITE_BLOCK`, pipeline writes)
- Erasure coding block reconstruction
- HDFS federation / ViewFS
- HA NameNode failover (can be layered later)

---

## 3. Architecture

```
┌──────────────────────────────────────────────────┐
│                  Consumer Code                    │
│          (reads via Stream / IAsyncDisposable)     │
├──────────────────────────────────────────────────┤
│              HdfsFileStream (Phase 2)             │
│   Resolves path → LocatedBlock[], chains readers  │
├──────────────────────────────────────────────────┤
│              RemoteBlockReader : Stream            │
│   Packet loop, checksum verify, buffer management │
├──────────────────────────────────────────────────┤
│          DataTransferProtocol (Sender/Receiver)   │
│   Protobuf serialization, op-code framing         │
├──────────────────────────────────────────────────┤
│                Peer (TCP connection)               │
│   Socket + PipeReader/PipeWriter + peer cache      │
├──────────────────────────────────────────────────┤
│          SaslDataTransferHandler (Phase 2)         │
│   GSSAPI/DIGEST-MD5 negotiation, stream wrapping  │
└──────────────────────────────────────────────────┘
```

---

## 4. Project Structure

```
dotnet/
└── src/
    └── Gtlm.Hdfs.Client/
        ├── Gtlm.Hdfs.Client.csproj          # net10.0, Google.Protobuf, Kerberos.NET
        ├── Proto/                           # Generated C# from Hadoop .proto files
        │   ├── datatransfer.proto           # OpReadBlockProto, PacketHeaderProto, etc.
        │   ├── hdfs.proto                   # ExtendedBlockProto, ChecksumTypeProto, etc.
        │   └── Security.proto              # TokenProto
        ├── Protocol/
        │   ├── DataTransferSender.cs        # Sends OP_READ_BLOCK requests
        │   ├── DataTransferReceiver.cs      # Parses BlockOpResponseProto
        │   ├── PacketReader.cs              # Reads length-prefixed packet frames
        │   └── OpCode.cs                    # DataNode operation codes (81 = READ_BLOCK)
        ├── Checksum/
        │   ├── DataChecksum.cs              # Factory: CRC32, CRC32C, NULL
        │   ├── Crc32Algorithm.cs            # Software CRC32 (IEEE)
        │   └── Crc32CAlgorithm.cs           # CRC32C (Castagnoli), use intrinsics
        ├── Net/
        │   ├── Peer.cs                      # Socket wrapper with PipeReader/PipeWriter
        │   ├── PeerCache.cs                 # Connection pool keyed by DatanodeID
        │   └── SaslDataTransferHandler.cs   # Phase 2: SASL negotiation
        ├── BlockReading/
        │   ├── RemoteBlockReader.cs         # Core class -- Stream impl
        │   ├── IBlockReader.cs              # Interface for reader abstraction
        │   └── BlockReaderFactory.cs        # Factory choosing reader strategy
        ├── Configuration/
        │   └── HdfsClientOptions.cs         # Typed config (buffer sizes, timeouts, etc.)
        └── Models/
            ├── ExtendedBlock.cs             # Block identity (poolId, blockId, genStamp)
            ├── DatanodeInfo.cs              # DataNode address + metadata
            └── BlockToken.cs                # Block access token wrapper
└── tests/
    └── Gtlm.Hdfs.Client.Tests/
        ├── PacketReaderTests.cs             # Unit tests with captured wire data
        ├── ChecksumTests.cs                 # CRC32/CRC32C verification
        ├── RemoteBlockReaderTests.cs        # Mock-socket integration tests
        └── IntegrationTests/
            └── LiveClusterTests.cs          # Against real Hadoop cluster
```

---

## 5. Wire Protocol Detail

### 5.1 OP_READ_BLOCK Flow

```
Client                                          DataNode
  │                                                │
  │──── TCP connect (port 9866) ──────────────────►│
  │                                                │
  │──── [2 bytes: version=28] ───────────────────►│
  │──── [1 byte: OP_READ_BLOCK=81] ──────────────►│
  │──── [varint-prefixed OpReadBlockProto] ───────►│
  │                                                │
  │◄─── [varint-prefixed BlockOpResponseProto] ────│
  │     (status, ReadOpChecksumInfoProto)           │
  │                                                │
  │◄─── [Packet 0: 4B len + 2B hdrLen + proto     │
  │      + checksums + data] ──────────────────────│
  │◄─── [Packet 1 ...] ───────────────────────────│
  │◄─── [Packet N: lastPacketInBlock=true,         │
  │      dataLen=0] ──────────────────────────────│
  │                                                │
  │──── [ClientReadStatusProto: CHECKSUM_OK] ─────►│
  │                                                │
  │──── close / return to peer cache ──────────────│
```

### 5.2 Packet Binary Layout (Big-Endian)

```
┌─────────────┬─────────────┬──────────────────────┬────────────┬──────────┐
│ packetLen   │ headerLen   │ PacketHeaderProto     │ Checksums  │ Data     │
│ (4 bytes)   │ (2 bytes)   │ (headerLen bytes)     │ (N bytes)  │ (M bytes)│
│ int32 BE    │ int16 BE    │ protobuf              │            │          │
└─────────────┴─────────────┴──────────────────────┴────────────┴──────────┘
```

- **packetLen**: Total bytes following this field (headerLen + proto + checksums + data + 4)
- **PacketHeaderProto** fields (all fixed-width for predictable sizing):
  - `sfixed64 offsetInBlock` -- byte offset within the HDFS block
  - `sfixed64 seqno` -- sequence number
  - `bool lastPacketInBlock` -- true for final packet
  - `sfixed32 dataLen` -- length of data payload only
  - `bool syncBlock` -- hsync/hflush indicator
- **Checksums**: `ceil(dataLen / bytesPerChecksum) * checksumSize` bytes
  - Default: 512 bytes per chunk, 4 bytes per CRC = one CRC32 per 512B of data
- **Data**: exactly `dataLen` bytes of block content

### 5.3 Key Protobuf Messages

```protobuf
// Request
message OpReadBlockProto {
  required ClientOperationHeaderProto header = 1;  // block + token + clientName
  required uint64 offset = 2;
  required uint64 len = 3;
  optional bool sendChecksums = 4 [default = true];
  optional CachingStrategyProto cachingStrategy = 5;
}

// Response
message BlockOpResponseProto {
  required Status status = 1;
  optional ReadOpChecksumInfoProto readOpChecksumInfo = 4;
}

message ReadOpChecksumInfoProto {
  required ChecksumProto checksum = 1;    // type + bytesPerChecksum
  required uint64 chunkOffset = 2;        // alignment offset for first chunk
}

// Per-packet header
message PacketHeaderProto {
  required sfixed64 offsetInBlock = 1;
  required sfixed64 seqno = 2;
  required bool lastPacketInBlock = 3;
  required sfixed32 dataLen = 4;
  optional bool syncBlock = 5 [default = false];
}

// Client → DataNode final ack
message ClientReadStatusProto {
  required Status status = 1;  // CHECKSUM_OK on success
}
```

---

## 6. Core Components -- Design

### 6.1 `Peer` (TCP Connection Wrapper)

Wraps a `Socket` with `System.IO.Pipelines` for zero-copy reads.

```csharp
public sealed class Peer : IAsyncDisposable
{
    public PipeReader Input { get; }
    public PipeWriter Output { get; }
    public DatanodeInfo DataNode { get; }
    public bool IsLocal { get; }

    public static async Task<Peer> ConnectAsync(
        DatanodeInfo datanode, HdfsClientOptions options, CancellationToken ct);

    // Flush output, read exact N bytes, etc.
    public async ValueTask FlushAsync(CancellationToken ct);
    public async ValueTask<ReadOnlySequence<byte>> ReadExactlyAsync(int count, CancellationToken ct);
}
```

**Implementation notes:**
- Use `Socket.ConnectAsync` with configurable connect timeout
- Create `Pipe` from `NetworkStream` using `StreamPipeReaderOptions` with `MemoryPool<byte>.Shared`
- Buffer size from `HdfsClientOptions.RemoteBufferSize` (default 512KB, matching Hadoop's `dfs.client.block.reader.remote.buffer.size`)

### 6.2 `PeerCache` (Connection Pool)

```csharp
public sealed class PeerCache : IDisposable
{
    public PeerCache(int maxPerDataNode, TimeSpan idleTimeout);

    public Peer? TryGet(DatanodeInfo datanode);
    public void Return(Peer peer);
    // Evicts idle connections on a timer
}
```

Keyed by `DatanodeInfo.DatanodeUuid`. Returns idle peers for reuse, avoiding TCP
handshake overhead on sequential reads from the same DataNode. Matches Java's `PeerCache`.

### 6.3 `DataTransferSender` (Request Serialization)

```csharp
public static class DataTransferSender
{
    /// Writes the OP_READ_BLOCK request onto the peer's output pipe.
    public static async ValueTask SendReadBlockAsync(
        PipeWriter output,
        ExtendedBlock block,
        BlockToken token,
        string clientName,
        long offset,
        long length,
        bool sendChecksums,
        CancellationToken ct);
}
```

Wire format:
1. Write `DATA_TRANSFER_VERSION` (2 bytes, value 28)
2. Write `OP_READ_BLOCK` (1 byte, value 81)
3. Serialize `OpReadBlockProto` with varint length prefix

### 6.4 `PacketReader` (Packet Frame Parser)

The performance-critical component. Reads the binary packet framing from `PipeReader`.

```csharp
public ref struct PacketReader
{
    public static bool TryReadPacket(
        ref ReadOnlySequence<byte> buffer,
        out PacketHeaderProto header,
        out ReadOnlySequence<byte> checksums,
        out ReadOnlySequence<byte> data);
}
```

**Parsing strategy using `System.IO.Pipelines`:**
1. Peek at first 6 bytes: `packetLen` (4B BE int) + `headerLen` (2B BE short)
2. If buffer has fewer than `6 + headerLen + checksumLen + dataLen` bytes, return false (need more data)
3. Parse `PacketHeaderProto` from next `headerLen` bytes
4. Compute `checksumLen = ceil(header.DataLen / bytesPerChecksum) * checksumSize`
5. Slice checksums and data from remaining buffer
6. Advance the `PipeReader`

This avoids copying -- checksums and data are `ReadOnlySequence<byte>` slices of the
pipe's pooled memory.

### 6.5 `DataChecksum` (Checksum Verification)

```csharp
public abstract class DataChecksum
{
    public int BytesPerChecksum { get; }
    public int ChecksumSize { get; }

    public static DataChecksum Create(ChecksumTypeProto type, int bytesPerChecksum);

    /// Verify checksums for a data buffer. Throws on mismatch.
    public void VerifyChunks(ReadOnlySpan<byte> data, ReadOnlySpan<byte> checksums);
}
```

- **CRC32C**: Use `System.Runtime.Intrinsics.X86.Sse42.Crc32` (hardware-accelerated on x64)
  and `System.Runtime.Intrinsics.Arm.Crc32` (on ARM64). Fall back to software table.
- **CRC32 (IEEE)**: Software lookup table, or use `System.IO.Hashing.Crc32` (.NET 7+).
- Chunk-by-chunk verification: iterate `bytesPerChecksum`-sized slices of data,
  compare computed CRC against the corresponding 4 bytes in the checksum buffer.

### 6.6 `RemoteBlockReader` (Core Class)

```csharp
public sealed class RemoteBlockReader : Stream, IBlockReader, IAsyncDisposable
{
    // --- State ---
    private readonly Peer _peer;
    private readonly DataChecksum _checksum;
    private readonly bool _verifyChecksum;
    private readonly long _startOffset;
    private readonly long _bytesToRead;
    private long _bytesRead;
    private readonly PeerCache? _peerCache;

    // Current packet buffer
    private byte[] _packetBuf;     // rented from ArrayPool
    private int _packetDataOffset;
    private int _packetDataRemaining;
    private bool _lastPacket;

    // --- Factory (matches Java's newBlockReader) ---
    public static async Task<RemoteBlockReader> CreateAsync(
        string file,
        ExtendedBlock block,
        BlockToken token,
        long startOffset,
        long length,
        bool verifyChecksum,
        string clientName,
        Peer peer,
        DatanodeInfo datanode,
        PeerCache? peerCache,
        HdfsClientOptions options,
        CancellationToken ct = default)
    {
        // 1. Send OP_READ_BLOCK via DataTransferSender
        // 2. Read BlockOpResponseProto (varint-prefixed)
        // 3. Validate status (SUCCESS)
        // 4. Extract ReadOpChecksumInfoProto → DataChecksum + firstChunkOffset
        // 5. Validate firstChunkOffset alignment
        // 6. Construct and return RemoteBlockReader
    }

    // --- Stream overrides ---
    public override async ValueTask<int> ReadAsync(
        Memory<byte> buffer, CancellationToken ct = default)
    {
        if (_bytesRead >= _bytesToRead) return 0;

        // If current packet exhausted, read next packet
        if (_packetDataRemaining == 0)
        {
            if (_lastPacket) return 0;
            await ReadNextPacketAsync(ct);
        }

        // Copy from packet buffer to caller's buffer
        int toCopy = Math.Min(buffer.Length, _packetDataRemaining);
        toCopy = (int)Math.Min(toCopy, _bytesToRead - _bytesRead);

        _packetBuf.AsMemory(_packetDataOffset, toCopy).CopyTo(buffer);
        _packetDataOffset += toCopy;
        _packetDataRemaining -= toCopy;
        _bytesRead += toCopy;

        return toCopy;
    }

    private async ValueTask ReadNextPacketAsync(CancellationToken ct)
    {
        // 1. Read 6 bytes: packetLen (4B) + headerLen (2B)
        // 2. Read remaining packet bytes
        // 3. Deserialize PacketHeaderProto
        // 4. Slice checksum and data regions
        // 5. If _verifyChecksum: _checksum.VerifyChunks(data, checksums)
        // 6. Copy data to _packetBuf (or use directly if contiguous)
        // 7. Update _packetDataOffset, _packetDataRemaining, _lastPacket
    }

    public override async ValueTask DisposeAsync()
    {
        // 1. Send ClientReadStatusProto (CHECKSUM_OK if successful)
        // 2. Return _packetBuf to ArrayPool
        // 3. Return peer to PeerCache (if eligible) or dispose
    }

    // Stream boilerplate
    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => _bytesToRead;
    public override long Position { get => _bytesRead; set => throw new NotSupportedException(); }
}
```

**Key behavioral parity with Java `BlockReaderRemote`:**
- `firstChunkOffset` handling: the first packet may contain extra bytes before `startOffset`
  (due to checksum chunk alignment). Skip `startOffset - firstChunkOffset` bytes from the
  first packet's data before returning to caller.
- Sends `ClientReadStatusProto` with `CHECKSUM_OK` on successful close.
- Returns peer to `PeerCache` on clean close (no errors, block fully read or cancelled
  cleanly). Disposes peer on error.

### 6.7 `HdfsClientOptions` (Configuration)

```csharp
public sealed class HdfsClientOptions
{
    public int RemoteBufferSize { get; set; } = 512 * 1024;         // dfs.client.block.reader.remote.buffer.size
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(60);
    public TimeSpan ReadTimeout { get; set; } = TimeSpan.FromSeconds(60);
    public int PeerCacheMaxPerDataNode { get; set; } = 64;
    public TimeSpan PeerCacheIdleTimeout { get; set; } = TimeSpan.FromSeconds(300);
    public bool VerifyChecksum { get; set; } = true;
    public string ClientName { get; set; } = $"DotNetHdfsClient_{Environment.MachineName}";

    // Phase 2
    public string? KerberosPrincipal { get; set; }
    public string? KeytabPath { get; set; }
    public bool EncryptDataTransfer { get; set; } = false;
}
```

---

## 7. .NET 10 Features Leveraged

| Feature | Usage |
|---------|-------|
| `System.IO.Pipelines` (`PipeReader`/`PipeWriter`) | Zero-copy packet parsing from DataNode socket. Avoids intermediate byte[] allocations for the hot read path. |
| `Memory<byte>` / `Span<byte>` / `ReadOnlySequence<byte>` | Checksum verification and data slicing operate on spans without copying. |
| `ArrayPool<byte>.Shared` | Rent/return packet data buffers to avoid GC pressure on large reads. |
| `System.Runtime.Intrinsics` (SSE4.2 / ARM CRC32) | Hardware-accelerated CRC32C checksum computation. |
| `System.IO.Hashing.Crc32` | Software CRC32 (IEEE) for non-CRC32C clusters. |
| `Google.Protobuf` (NuGet) | Generated C# classes from Hadoop's `.proto` files for all protocol messages. |
| `ValueTask<T>` | Async hot path avoids `Task` allocation when data is already buffered. |
| `IAsyncDisposable` / `await using` | Clean resource cleanup (socket, peer cache return, status ack). |
| `CancellationToken` throughout | Cooperative cancellation for all I/O operations. |

---

## 8. Protobuf Code Generation

Copy the following `.proto` files from the [Apache Hadoop repository](https://github.com/apache/hadoop/tree/trunk/hadoop-hdfs-project/hadoop-hdfs-client/src/main/proto):

1. `datatransfer.proto` -- `OpReadBlockProto`, `PacketHeaderProto`, `BlockOpResponseProto`, `ClientReadStatusProto`, `Status` enum
2. `hdfs.proto` -- `ExtendedBlockProto`, `ChecksumTypeProto`, `DatanodeIDProto`, `DatanodeInfoProto`
3. `Security.proto` (from `hadoop-common`) -- `TokenProto`

Generate C# using `Grpc.Tools`:

```xml
<!-- In Gtlm.Hdfs.Client.csproj -->
<ItemGroup>
  <PackageReference Include="Google.Protobuf" Version="3.*" />
  <PackageReference Include="Grpc.Tools" Version="2.*" PrivateAssets="All" />
</ItemGroup>

<ItemGroup>
  <Protobuf Include="Proto/*.proto" GrpcServices="None" />
</ItemGroup>
```

Adjust `option csharp_namespace = "Gtlm.Hdfs.Client.Proto";` in each `.proto` file
and resolve cross-file imports via `ProtoRoot`.

---

## 9. Error Handling and Resilience

| Scenario | Behavior |
|----------|----------|
| `BlockOpResponseProto.Status != SUCCESS` | Throw `HdfsProtocolException` with status code and DataNode identity. |
| Checksum mismatch | Throw `ChecksumException`. Caller (Phase 2 `HdfsFileStream`) retries with next replica. |
| Socket timeout / disconnect | Throw `IOException`. Peer is NOT returned to cache. |
| `firstChunkOffset` out of range | Throw `IOException` (same validation as Java). |
| Block token expired | Throw `AccessTokenException`. Caller must re-fetch token from NameNode. |

---

## 10. Testing Strategy

### Unit Tests
- **`PacketReaderTests`**: Construct byte arrays matching the packet binary format,
  verify correct parsing of header, checksum slice, data slice. Test edge cases:
  empty last packet, maximum-size packets, multi-segment `ReadOnlySequence`.
- **`ChecksumTests`**: Known CRC32/CRC32C vectors. Verify `VerifyChunks` passes on
  correct data and throws on corruption.
- **`DataTransferSenderTests`**: Capture serialized bytes, verify against expected
  wire format (version + opcode + varint-prefixed proto).

### Integration Tests (Mock)
- **`RemoteBlockReaderTests`**: Use `System.IO.Pipelines.Pipe` as a mock socket.
  Pre-fill with a captured or constructed response (BlockOpResponseProto + packet stream).
  Verify `ReadAsync` returns correct data.

### Integration Tests (Live Cluster)
- **`LiveClusterTests`**: Requires a Hadoop cluster (Docker Compose with
  `apache/hadoop:3.3` image). Write a file via `hdfs dfs -put`, then read blocks
  using `RemoteBlockReader` and compare content byte-for-byte.
- Target Hadoop 3.3.x and 3.4.x.

---

## 11. Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `Google.Protobuf` | 3.x | Runtime protobuf serialization |
| `Grpc.Tools` | 2.x | Build-time proto → C# codegen |
| `System.IO.Hashing` | 9.x+ | CRC32 (IEEE) implementation |
| `System.IO.Pipelines` | (in-box) | High-performance I/O |
| `Kerberos.NET` | 5.x | Phase 2: Kerberos auth / SASL GSSAPI |
| `Microsoft.Extensions.Options` | (in-box) | Typed configuration binding |
| `Microsoft.Extensions.Logging.Abstractions` | (in-box) | Structured logging |

---

## 12. Implementation Phases

### Phase 1: Core Block Reader (MVP)

1. **Proto setup**: Copy `.proto` files, configure `Grpc.Tools` codegen, verify generated classes compile.
2. **Peer + PeerCache**: Socket connection with `PipeReader`/`PipeWriter`. Basic connection pool.
3. **DataTransferSender**: Serialize `OP_READ_BLOCK` request.
4. **DataTransferReceiver**: Parse `BlockOpResponseProto`, extract checksum info.
5. **PacketReader**: Binary packet frame parser.
6. **DataChecksum**: CRC32C (intrinsics) + CRC32 (software). Chunk verification.
7. **RemoteBlockReader**: Full `Stream` implementation. Factory method, read loop, dispose with status ack.
8. **Unit + mock integration tests**.
9. **Live cluster integration test** against Docker Hadoop.

### Phase 2: Full Client

1. **NameNode RPC**: Implement Hadoop IPC protocol for `ClientNamenodeProtocol`
   (`getBlockLocations`, `getFileInfo`, `getListing`). This is a separate protobuf-over-TCP
   RPC layer with its own framing.
2. **HdfsFileStream**: High-level `Stream` that resolves a path to `LocatedBlock[]`,
   creates `RemoteBlockReader` per block, chains them seamlessly.
3. **SASL negotiation**: `SaslDataTransferHandler` using `Kerberos.NET` for GSSAPI.
   Wraps the `Peer` stream in an encrypted/integrity-checked layer.
4. **Replica failover**: On checksum error or timeout, try the next DataNode in
   the `LocatedBlock.Locations` list.
5. **Short-circuit reads**: When client runs on the same host as DataNode, read
   block files directly from the local filesystem (via shared memory slot protocol).

### Phase 3: Hardening

1. **HA NameNode**: Support multiple NameNode addresses with automatic failover.
2. **Metrics**: Expose read throughput, checksum failures, connection pool stats
   via `System.Diagnostics.Metrics`.
3. **Configuration from XML**: Parse `hdfs-site.xml` / `core-site.xml` for
   drop-in compatibility with existing Hadoop configurations.
4. **Benchmarks**: BenchmarkDotNet suite comparing throughput against WebHDFS
   and the Java client (via JNI baseline).

---

## 13. Risks and Mitigations

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Protocol version mismatch between Hadoop releases | Medium | Pin to DATA_TRANSFER_VERSION=28 (Hadoop 2.6+). Test against 3.3 and 3.4. |
| SASL/Kerberos complexity (Phase 2) | High | Defer to Phase 2. Phase 1 targets clusters with `dfs.data.transfer.protection=none`. Use `Kerberos.NET` which handles cross-platform GSS tokens. |
| CRC32C performance on non-x64 platforms | Low | ARM64 also has CRC32 intrinsics. Software fallback exists. |
| Incomplete protobuf compatibility (proto2 required fields) | Low | `Google.Protobuf` handles proto2 syntax. Test with real cluster responses. |
| DataNode protocol edge cases (heartbeat packets, OOB restarts) | Medium | Study `PacketReceiver.java` and `BlockSender.java` for all edge cases. Log and handle `seqno=-1` heartbeats. |

---

## 14. Reference Materials

- **Java source**: [`BlockReaderRemote.java`](https://github.com/apache/hadoop/blob/trunk/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/client/impl/BlockReaderRemote.java), [`PacketReceiver.java`](https://github.com/apache/hadoop/blob/trunk/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/datatransfer/PacketReceiver.java), [`Sender.java`](https://github.com/apache/hadoop/blob/trunk/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/datatransfer/Sender.java)
- **Proto definitions**: [`datatransfer.proto`](https://github.com/apache/hadoop/blob/trunk/hadoop-hdfs-project/hadoop-hdfs-client/src/main/proto/datatransfer.proto), [`hdfs.proto`](https://github.com/apache/hadoop/blob/trunk/hadoop-hdfs-project/hadoop-hdfs-client/src/main/proto/hdfs.proto)
- **Go reference client**: [`colinmarc/hdfs`](https://github.com/colinmarc/hdfs) -- clean Go implementation of the same protocol, excellent for cross-referencing packet parsing
- **C++ reference**: [`libhdfs3`](https://github.com/erikmuttersbach/libhdfs3) -- native C++ HDFS client
- **.NET Pipelines guide**: [System.IO.Pipelines: High-performance I/O in .NET](https://devblogs.microsoft.com/dotnet/system-io-pipelines-high-performance-io-in-net/)
- **Kerberos.NET**: [`dotnet/Kerberos.NET`](https://github.com/dotnet/Kerberos.NET)
