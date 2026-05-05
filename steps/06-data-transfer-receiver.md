# Step 06: DataTransferReceiver

**Phase:** 1 (MVP)
**Prerequisites:** Step 01 (proto), Step 05 (sender -- to test round-trip)
**Produces:** `Protocol/DataTransferReceiver.cs` -- parses `BlockOpResponseProto`

---

## Objective

Implement the response parser for the initial `OP_READ_BLOCK` handshake response. After
the client sends the read request (Step 05), the DataNode responds with a
`BlockOpResponseProto` containing the status and checksum metadata. This step parses
that response and validates it.

This mirrors the response-handling section of Java's `BlockReaderRemote.newBlockReader()`.

---

## Tasks

### 6.1 `DataTransferReceiver` Class

**File:** `src/Gtlm.Hdfs.Client/Protocol/DataTransferReceiver.cs`

```csharp
namespace Gtlm.Hdfs.Client.Protocol;

using Google.Protobuf;
using Gtlm.Hdfs.Client.Net;
using Gtlm.Hdfs.Client.Proto;

/// <summary>
/// Parses data transfer protocol responses from an HDFS DataNode.
/// </summary>
internal static class DataTransferReceiver
{
    /// <summary>
    /// Read and parse the BlockOpResponseProto from the DataNode.
    /// This is the first message received after sending OP_READ_BLOCK.
    ///
    /// Wire format: varint-length-prefixed protobuf message.
    /// </summary>
    public static async Task<BlockOpResponseProto> ReceiveBlockOpResponseAsync(
        Peer peer,
        CancellationToken ct = default)
    {
        var stream = peer.GetInputStream();

        // ParseDelimitedFrom reads a varint length prefix, then the message bytes
        return BlockOpResponseProto.Parser.ParseDelimitedFrom(stream);
    }
}
```

### 6.2 `ChecksumInfo` -- Parsed Result Type

**File:** `src/Gtlm.Hdfs.Client/Protocol/ChecksumInfo.cs`

```csharp
namespace Gtlm.Hdfs.Client.Protocol;

using Gtlm.Hdfs.Client.Proto;

/// <summary>
/// Parsed checksum metadata from the DataNode's OP_READ_BLOCK response.
/// </summary>
internal readonly record struct ChecksumInfo(
    ChecksumTypeProto ChecksumType,
    int BytesPerChecksum,
    long ChunkOffset);
```

### 6.3 Response Validation

**File:** `src/Gtlm.Hdfs.Client/Protocol/DataTransferReceiver.cs` (continued)

```csharp
/// <summary>
/// Validate the DataNode's response and extract checksum info.
/// Throws on non-SUCCESS status.
/// </summary>
public static ChecksumInfo ValidateAndExtractChecksumInfo(
    BlockOpResponseProto response,
    DatanodeInfo dataNode,
    ExtendedBlock block,
    string file)
{
    if (response.Status != Status.Success)
    {
        throw new HdfsProtocolException(
            $"DataNode {dataNode} failed OP_READ_BLOCK for block {block.BlockId} " +
            $"of file {file}: status={response.Status}" +
            (string.IsNullOrEmpty(response.Message) ? "" : $", message={response.Message}"));
    }

    var checksumInfo = response.ReadOpChecksumInfo
        ?? throw new HdfsProtocolException(
            $"DataNode {dataNode} returned SUCCESS but no ReadOpChecksumInfo " +
            $"for block {block.BlockId}");

    var checksum = checksumInfo.Checksum
        ?? throw new HdfsProtocolException(
            $"DataNode {dataNode} returned empty ChecksumProto " +
            $"for block {block.BlockId}");

    return new ChecksumInfo(
        ChecksumType: checksum.Type,
        BytesPerChecksum: (int)checksum.BytesPerChecksum,
        ChunkOffset: (long)checksumInfo.ChunkOffset);
}
```

### 6.4 `firstChunkOffset` Validation

```csharp
/// <summary>
/// Validate the firstChunkOffset from the DataNode response.
/// Must satisfy: 0 <= firstChunkOffset <= startOffset
///               AND firstChunkOffset > startOffset - bytesPerChecksum
///
/// This ensures the first chunk is aligned and overlaps with the requested offset.
/// </summary>
public static void ValidateChunkOffset(
    long firstChunkOffset,
    long startOffset,
    int bytesPerChecksum,
    string file)
{
    if (firstChunkOffset < 0 ||
        firstChunkOffset > startOffset ||
        firstChunkOffset <= (startOffset - bytesPerChecksum))
    {
        throw new IOException(
            $"BlockReader: error in first chunk offset ({firstChunkOffset}), " +
            $"startOffset is {startOffset} for file {file}");
    }
}
```

This is the exact same validation as in the Java `BlockReaderRemote.newBlockReader()`.

### 6.5 Exception Types

**File:** `src/Gtlm.Hdfs.Client/Protocol/HdfsProtocolException.cs`

```csharp
namespace Gtlm.Hdfs.Client.Protocol;

/// <summary>
/// Thrown when the HDFS data transfer protocol returns an error or unexpected state.
/// </summary>
public class HdfsProtocolException : IOException
{
    public HdfsProtocolException(string message) : base(message) { }
    public HdfsProtocolException(string message, Exception inner) : base(message, inner) { }
}
```

**File:** `src/Gtlm.Hdfs.Client/Protocol/AccessTokenException.cs`

```csharp
namespace Gtlm.Hdfs.Client.Protocol;

/// <summary>
/// Thrown when a block access token is invalid or expired.
/// Callers should re-fetch block locations (and tokens) from the NameNode.
/// </summary>
public class AccessTokenException : HdfsProtocolException
{
    public AccessTokenException(string message) : base(message) { }
}
```

**File:** `src/Gtlm.Hdfs.Client/Protocol/ChecksumException.cs`

```csharp
namespace Gtlm.Hdfs.Client.Protocol;

/// <summary>
/// Thrown when a data checksum does not match.
/// Callers should retry with a different replica.
/// </summary>
public class ChecksumException : HdfsProtocolException
{
    public long Offset { get; }

    public ChecksumException(string message, long offset) : base(message)
    {
        Offset = offset;
    }
}
```

### 6.6 Enhanced Status Check with Specific Exceptions

Update `ValidateAndExtractChecksumInfo` to throw typed exceptions:

```csharp
if (response.Status == Status.ErrorAccessToken)
{
    throw new AccessTokenException(
        $"Block token rejected by DataNode {dataNode} for block {block.BlockId}: " +
        (response.Message ?? "no details"));
}
```

---

## Integration with `RemoteBlockReader.CreateAsync` (Step 09)

The complete handshake flow will look like:

```csharp
// 1. Send request (Step 05)
await DataTransferSender.SendReadBlockAsync(peer, block, token, ...);

// 2. Receive response (this step)
var response = await DataTransferReceiver.ReceiveBlockOpResponseAsync(peer, ct);

// 3. Validate and extract checksum info (this step)
var checksumInfo = DataTransferReceiver.ValidateAndExtractChecksumInfo(
    response, peer.DataNode, block, file);

// 4. Validate chunk offset (this step)
DataTransferReceiver.ValidateChunkOffset(
    checksumInfo.ChunkOffset, startOffset, checksumInfo.BytesPerChecksum, file);

// 5. Create checksum verifier (Step 07)
var checksum = DataChecksum.Create(checksumInfo.ChecksumType, checksumInfo.BytesPerChecksum);
```

---

## Acceptance Criteria

- [ ] `ReceiveBlockOpResponseAsync` correctly parses a varint-prefixed `BlockOpResponseProto`
- [ ] SUCCESS status returns valid `ChecksumInfo` with type, bytesPerChecksum, chunkOffset
- [ ] Non-SUCCESS status throws `HdfsProtocolException` with status and message
- [ ] `ERROR_ACCESS_TOKEN` throws `AccessTokenException`
- [ ] `ValidateChunkOffset` passes for valid offsets and throws for out-of-range offsets
- [ ] Unit test: construct a `BlockOpResponseProto`, serialize it, parse it back
