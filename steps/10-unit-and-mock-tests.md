# Step 10: Unit and Mock Integration Tests

**Phase:** 1 (MVP)
**Prerequisites:** Steps 01-09 (all Phase 1 components)
**Produces:** Comprehensive test suite validating correctness without a live cluster

---

## Objective

Build a test suite covering every component from Steps 02-09. Tests use synthetic
binary data, in-memory `Pipe` instances, and mocked sockets to verify protocol
correctness without requiring a Hadoop cluster.

---

## Tasks

### 10.1 Test Infrastructure -- Packet Builder

**File:** `tests/Gtlm.Hdfs.Client.Tests/Helpers/PacketBuilder.cs`

A helper that constructs valid binary packets matching the HDFS wire format.

```csharp
namespace Gtlm.Hdfs.Client.Tests.Helpers;

using System.Buffers.Binary;
using Google.Protobuf;
using Gtlm.Hdfs.Client.Proto;

/// <summary>
/// Builds synthetic HDFS data packets for testing.
/// </summary>
internal static class PacketBuilder
{
    /// <summary>
    /// Build a complete binary packet with header, checksums, and data.
    /// Returns the raw bytes as they would appear on the wire.
    /// </summary>
    public static byte[] BuildPacket(
        long offsetInBlock,
        long seqno,
        bool lastPacketInBlock,
        byte[] data,
        int bytesPerChecksum,
        Func<byte[], uint> checksumFunc)
    {
        // Build PacketHeaderProto
        var header = new PacketHeaderProto
        {
            OffsetInBlock = offsetInBlock,
            Seqno = seqno,
            LastPacketInBlock = lastPacketInBlock,
            DataLen = data.Length,
        };
        byte[] headerBytes = header.ToByteArray();

        // Build checksums
        int numChunks = (data.Length + bytesPerChecksum - 1) / bytesPerChecksum;
        byte[] checksums = new byte[numChunks * 4];
        for (int i = 0; i < numChunks; i++)
        {
            int start = i * bytesPerChecksum;
            int end = Math.Min(start + bytesPerChecksum, data.Length);
            byte[] chunk = data[start..end];
            uint crc = checksumFunc(chunk);
            BinaryPrimitives.WriteUInt32BigEndian(checksums.AsSpan(i * 4), crc);
        }

        // Assemble packet
        int packetLen = 2 + headerBytes.Length + checksums.Length + data.Length + 4;
        byte[] packet = new byte[4 + packetLen];

        int pos = 0;
        // packetLen (4 bytes BE)
        BinaryPrimitives.WriteInt32BigEndian(packet.AsSpan(pos), packetLen);
        pos += 4;
        // headerLen (2 bytes BE)
        BinaryPrimitives.WriteInt16BigEndian(packet.AsSpan(pos), (short)headerBytes.Length);
        pos += 2;
        // header proto
        headerBytes.CopyTo(packet.AsSpan(pos));
        pos += headerBytes.Length;
        // checksums
        checksums.CopyTo(packet.AsSpan(pos));
        pos += checksums.Length;
        // data
        data.CopyTo(packet.AsSpan(pos));

        return packet;
    }

    /// <summary>
    /// Build an empty last packet (signals end of block).
    /// </summary>
    public static byte[] BuildLastPacket(long offsetInBlock, long seqno)
    {
        return BuildPacket(offsetInBlock, seqno, lastPacketInBlock: true,
            data: [], bytesPerChecksum: 512, _ => 0);
    }
}
```

### 10.2 Test Infrastructure -- Response Builder

**File:** `tests/Gtlm.Hdfs.Client.Tests/Helpers/ResponseBuilder.cs`

Builds serialized `BlockOpResponseProto` messages.

```csharp
internal static class ResponseBuilder
{
    public static byte[] BuildSuccessResponse(
        ChecksumTypeProto checksumType = ChecksumTypeProto.ChecksumCrc32C,
        uint bytesPerChecksum = 512,
        ulong chunkOffset = 0)
    {
        var response = new BlockOpResponseProto
        {
            Status = Status.Success,
            ReadOpChecksumInfo = new ReadOpChecksumInfoProto
            {
                Checksum = new ChecksumProto
                {
                    Type = checksumType,
                    BytesPerChecksum = bytesPerChecksum,
                },
                ChunkOffset = chunkOffset,
            },
        };

        using var ms = new MemoryStream();
        response.WriteDelimitedTo(ms);
        return ms.ToArray();
    }

    public static byte[] BuildErrorResponse(Status status, string message = "")
    {
        var response = new BlockOpResponseProto
        {
            Status = status,
            Message = message,
        };

        using var ms = new MemoryStream();
        response.WriteDelimitedTo(ms);
        return ms.ToArray();
    }
}
```

### 10.3 Checksum Tests

**File:** `tests/Gtlm.Hdfs.Client.Tests/ChecksumTests.cs`

```csharp
public class ChecksumTests
{
    [Fact] CRC32_KnownVector()        // "123456789" → 0xCBF43926
    [Fact] CRC32C_KnownVector()       // "123456789" → 0xE3069283
    [Fact] VerifyChunks_Valid_NoThrow()
    [Fact] VerifyChunks_Corrupt_ThrowsChecksumException()
    [Fact] VerifyChunks_PartialLastChunk()
    [Fact] GetChecksumBytesForDataLength_Calculations()
    [Fact] NullChecksum_NeverThrows()
}
```

### 10.4 PacketReader Tests

**File:** `tests/Gtlm.Hdfs.Client.Tests/PacketReaderTests.cs`

```csharp
public class PacketReaderTests
{
    [Fact] ReadNextPacket_SinglePacket_ParsesCorrectly()
    [Fact] ReadNextPacket_MultiplePackets_Sequential()
    [Fact] ReadNextPacket_EmptyLastPacket()
    [Fact] ReadNextPacket_LargePacket_64KB()
    [Fact] ReadNextPacket_HeartbeatSeqno()
    [Fact] ReadNextPacket_ConnectionClosed_ThrowsEndOfStream()
    [Fact] ReadNextPacket_PartialWrite_WaitsForMoreData()
}
```

Each test creates a `System.IO.Pipelines.Pipe`, writes synthetic packet bytes to
the `PipeWriter`, then reads via `PacketReader` from the `PipeReader`.

### 10.5 DataTransferSender Tests

**File:** `tests/Gtlm.Hdfs.Client.Tests/DataTransferSenderTests.cs`

```csharp
public class DataTransferSenderTests
{
    [Fact] SendReadBlock_WritesCorrectVersion()     // 0x001C
    [Fact] SendReadBlock_WritesCorrectOpCode()      // 0x51
    [Fact] SendReadBlock_ProtobufIsVarintPrefixed()
    [Fact] SendReadBlock_FieldsMatchInput()         // Deserialize and compare
    [Fact] SendClientReadStatus_SerializesCorrectly()
}
```

Uses a `MemoryStream`-backed mock `Peer` to capture written bytes.

### 10.6 DataTransferReceiver Tests

**File:** `tests/Gtlm.Hdfs.Client.Tests/DataTransferReceiverTests.cs`

```csharp
public class DataTransferReceiverTests
{
    [Fact] ReceiveResponse_Success_ReturnsChecksumInfo()
    [Fact] ReceiveResponse_Error_ThrowsHdfsProtocolException()
    [Fact] ReceiveResponse_AccessTokenError_ThrowsAccessTokenException()
    [Fact] ValidateChunkOffset_Valid_NoThrow()
    [Fact] ValidateChunkOffset_Negative_Throws()
    [Fact] ValidateChunkOffset_TooFarBack_Throws()
    [Fact] ValidateChunkOffset_BeyondStartOffset_Throws()
}
```

### 10.7 PeerCache Tests

**File:** `tests/Gtlm.Hdfs.Client.Tests/PeerCacheTests.cs`

```csharp
public class PeerCacheTests
{
    [Fact] Return_ThenGet_ReturnsSamePeer()
    [Fact] TryGet_Empty_ReturnsNull()
    [Fact] TryGet_ExpiredPeer_ReturnsNull_DisposedPeer()
    [Fact] Return_ExceedsMax_DisposesExcess()
    [Fact] Dispose_DisposesAllCachedPeers()
    [Fact] ConcurrentAccess_ThreadSafe()
}
```

### 10.8 RemoteBlockReader Integration Test (Mock Socket)

**File:** `tests/Gtlm.Hdfs.Client.Tests/RemoteBlockReaderTests.cs`

The most important test. Creates a full mock DataNode conversation using `Pipe`:

```csharp
public class RemoteBlockReaderTests
{
    [Fact]
    public async Task ReadBlock_SimpleSequentialRead()
    {
        // Arrange: build expected data (e.g. 2048 bytes of random data)
        // Build response: SUCCESS + checksum info
        // Build packets: 2 packets of 1024 bytes each + empty last packet
        // Wire it all together in a Pipe

        // Act: CreateAsync + ReadAsync in a loop (or CopyToAsync)

        // Assert: output matches expected data byte-for-byte
    }

    [Fact] ReadBlock_NonAlignedOffset_SkipsCorrectly()
    [Fact] ReadBlock_ChecksumMismatch_ThrowsChecksumException()
    [Fact] ReadBlock_PartialRead_SmallBuffer()
    [Fact] ReadBlock_Dispose_SendsChecksumOk()
    [Fact] ReadBlock_Dispose_ReturnsPeerToCache()
    [Fact] ReadBlock_ErrorStatus_ThrowsHdfsProtocolException()
    [Fact] ReadBlock_HeartbeatPackets_Skipped()
}
```

### 10.9 Model Tests

**File:** `tests/Gtlm.Hdfs.Client.Tests/ModelTests.cs`

```csharp
public class ModelTests
{
    [Fact] ExtendedBlock_ToProto_RoundTrip()
    [Fact] BlockToken_ToProto_RoundTrip()
    [Fact] BlockToken_Empty_IsValid()
    [Fact] DatanodeInfo_Equality_ByUuid()
    [Fact] DatanodeInfo_Equality_DifferentUuid_NotEqual()
}
```

---

## Running Tests

```bash
cd dotnet/
dotnet test Gtlm.Hdfs.sln --verbosity normal
```

All tests must pass. No tests should require network access or a running Hadoop cluster.

---

## Acceptance Criteria

- [ ] All unit tests pass
- [ ] Checksum test vectors match published CRC32/CRC32C values
- [ ] PacketReader correctly handles edge cases (empty packets, partial reads, heartbeats)
- [ ] RemoteBlockReader mock integration test reads correct data end-to-end
- [ ] No flaky tests (deterministic, no timing dependencies)
- [ ] Test coverage for all exception paths (protocol errors, checksum failures, EOF)
