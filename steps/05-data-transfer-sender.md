# Step 05: DataTransferSender

**Phase:** 1 (MVP)
**Prerequisites:** Step 01 (proto), Step 02 (models), Step 03 (Peer)
**Produces:** `Protocol/DataTransferSender.cs` -- sends `OP_READ_BLOCK` requests

---

## Objective

Implement the client-side request serialization for the HDFS Data Transfer Protocol.
This sends the handshake (version + opcode) and the `OpReadBlockProto` message to the
DataNode, initiating a block read.

This mirrors Java's `org.apache.hadoop.hdfs.protocol.datatransfer.Sender.readBlock()`.

---

## Tasks

### 5.1 `OpCode` Constants

**File:** `src/Gtlm.Hdfs.Client/Protocol/OpCode.cs`

```csharp
namespace Gtlm.Hdfs.Client.Protocol;

/// <summary>
/// HDFS DataNode operation codes.
/// Sent as a single byte after the version handshake.
/// </summary>
internal static class OpCode
{
    public const byte WriteBlock = 80;
    public const byte ReadBlock = 81;
    public const byte ReadMetadata = 82;
    public const byte ReplaceBlock = 83;
    public const byte CopyBlock = 84;
    public const byte BlockChecksum = 85;
    public const byte TransferBlock = 86;
    public const byte RequestShortCircuitFds = 87;
    public const byte ReleaseShortCircuitFds = 88;
    public const byte RequestShortCircuitShm = 89;
    public const byte BlockGroupChecksum = 90;
    public const byte Customoper = 127;
}
```

### 5.2 `DataTransferConstants`

**File:** `src/Gtlm.Hdfs.Client/Protocol/DataTransferConstants.cs`

```csharp
namespace Gtlm.Hdfs.Client.Protocol;

internal static class DataTransferConstants
{
    /// <summary>
    /// Data transfer protocol version. Value 28 is used by Hadoop 2.6+ through 3.x.
    /// </summary>
    public const short DataTransferVersion = 28;

    /// <summary>
    /// Size of the packet length prefix (4 bytes) + header length prefix (2 bytes).
    /// </summary>
    public const int PacketLengthsSize = 6;
}
```

### 5.3 `DataTransferSender` Class

**File:** `src/Gtlm.Hdfs.Client/Protocol/DataTransferSender.cs`

```csharp
namespace Gtlm.Hdfs.Client.Protocol;

using Google.Protobuf;
using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Net;
using Gtlm.Hdfs.Client.Proto;

/// <summary>
/// Sends data transfer protocol requests to an HDFS DataNode.
/// </summary>
internal static class DataTransferSender
{
    /// <summary>
    /// Send an OP_READ_BLOCK request to the DataNode.
    ///
    /// Wire format:
    ///   [2 bytes BE: DATA_TRANSFER_VERSION (28)]
    ///   [1 byte: OP_READ_BLOCK (81)]
    ///   [varint: proto message length]
    ///   [N bytes: serialized OpReadBlockProto]
    /// </summary>
    public static async ValueTask SendReadBlockAsync(
        Peer peer,
        ExtendedBlock block,
        BlockToken token,
        string clientName,
        long offset,
        long length,
        bool sendChecksums,
        CancellationToken ct = default)
    {
        var stream = peer.GetOutputStream();

        // 1. Write version (2 bytes, big-endian)
        var versionBytes = new byte[2];
        BinaryPrimitives.WriteInt16BigEndian(versionBytes, DataTransferConstants.DataTransferVersion);
        await stream.WriteAsync(versionBytes, ct);

        // 2. Write opcode (1 byte)
        stream.WriteByte(OpCode.ReadBlock);

        // 3. Build the protobuf message
        var proto = new OpReadBlockProto
        {
            Header = new ClientOperationHeaderProto
            {
                BaseHeader = new BaseHeaderProto
                {
                    Block = block.ToProto(),
                    Token = token.ToProto(),
                },
                ClientName = clientName,
            },
            Offset = (ulong)offset,
            Len = (ulong)length,
            SendChecksums = sendChecksums,
        };

        // 4. Write varint-length-prefixed protobuf message
        //    (DataNode expects varint length prefix, not fixed-size)
        proto.WriteDelimitedTo(stream);

        // 5. Flush to ensure request is sent immediately
        await stream.FlushAsync(ct);
    }

    /// <summary>
    /// Send the client's final read status to the DataNode.
    /// Called after all packets have been read (or on error).
    /// </summary>
    public static async ValueTask SendClientReadStatusAsync(
        Peer peer,
        Status status,
        CancellationToken ct = default)
    {
        var stream = peer.GetOutputStream();

        var proto = new ClientReadStatusProto
        {
            Status = status,
        };

        proto.WriteDelimitedTo(stream);
        await stream.FlushAsync(ct);
    }
}
```

### 5.4 BigEndian Helper (if not using `BinaryPrimitives`)

The `System.Buffers.Binary.BinaryPrimitives` class (in-box) provides
`WriteInt16BigEndian`, `WriteInt32BigEndian`, `ReadInt32BigEndian`, etc. Ensure the
`using System.Buffers.Binary;` is included.

### 5.5 Varint-Prefixed Protobuf Writing

`Google.Protobuf` provides `message.WriteDelimitedTo(stream)` which writes a varint
length prefix followed by the serialized message. This matches the Hadoop Java pattern
of `PBHelperClient.vintPrefixed()`.

If `WriteDelimitedTo` is not available in the version of `Google.Protobuf` being used,
implement manually:

```csharp
private static void WriteDelimited(IMessage message, Stream stream)
{
    var size = message.CalculateSize();
    var codedOutput = new CodedOutputStream(stream, leaveOpen: true);
    codedOutput.WriteLength(size);
    message.WriteTo(codedOutput);
    codedOutput.Flush();
}
```

---

## Wire Format Verification

The complete byte sequence sent to the DataNode for `OP_READ_BLOCK`:

```
Offset  Bytes    Description
------  -----    -----------
0       00 1C    Version 28 (big-endian int16)
2       51       OP_READ_BLOCK (81 decimal)
3       XX       Varint: length of OpReadBlockProto
4..N    ...      Serialized OpReadBlockProto
```

The DataNode reads this, validates the block token, locates the block on disk,
and begins streaming packets in response.

---

## Acceptance Criteria

- [ ] `SendReadBlockAsync` writes the correct 3-byte header (version + opcode)
- [ ] Protobuf message is varint-length-prefixed
- [ ] `ExtendedBlock`, `BlockToken`, `clientName`, `offset`, `length` are correctly
      encoded in the `OpReadBlockProto`
- [ ] `SendClientReadStatusAsync` sends a valid `ClientReadStatusProto`
- [ ] Bytes are flushed to the socket immediately after writing
- [ ] Unit test: capture bytes written to a `MemoryStream`, verify against expected
      wire format
