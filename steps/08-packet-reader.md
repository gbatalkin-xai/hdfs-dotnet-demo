# Step 08: PacketReader

**Phase:** 1 (MVP)
**Prerequisites:** Step 01 (proto), Step 03 (Peer), Step 07 (DataChecksum)
**Produces:** `Protocol/PacketReader.cs` -- binary packet frame parser

---

## Objective

Implement the packet parser for the HDFS data transfer protocol streaming phase. After
the handshake, the DataNode streams block data as a sequence of length-prefixed binary
packets. Each packet contains a protobuf header, checksum bytes, and data bytes.

This is the **hottest path** in the entire library. Every byte of block data flows
through this parser. Design for zero-copy and minimal allocation.

---

## Tasks

### 8.1 Packet Structure Recap

```
┌─────────────┬─────────────┬──────────────────────┬────────────┬──────────┐
│ packetLen   │ headerLen   │ PacketHeaderProto     │ Checksums  │ Data     │
│ 4 bytes BE  │ 2 bytes BE  │ (headerLen bytes)     │ (C bytes)  │ (D bytes)│
└─────────────┴─────────────┴──────────────────────┴────────────┴──────────┘

Where:
  - packetLen includes everything after its own 4 bytes
  - C = ceil(D / bytesPerChecksum) * checksumSize
  - D = PacketHeaderProto.dataLen
  - Total packet size = 4 + packetLen
```

### 8.2 `PacketData` Result Type

**File:** `src/Gtlm.Hdfs.Client/Protocol/PacketData.cs`

```csharp
namespace Gtlm.Hdfs.Client.Protocol;

using Gtlm.Hdfs.Client.Proto;

/// <summary>
/// Parsed packet from the DataNode's packet stream.
/// Holds references to the checksum and data regions.
/// </summary>
internal readonly struct PacketData
{
    /// <summary>Parsed protobuf header.</summary>
    public PacketHeaderProto Header { get; init; }

    /// <summary>Checksum bytes (one CRC per chunk of data).</summary>
    public ReadOnlyMemory<byte> Checksums { get; init; }

    /// <summary>Block data payload.</summary>
    public ReadOnlyMemory<byte> Data { get; init; }

    /// <summary>True if this is the final packet in the block.</summary>
    public bool IsLastPacket => Header.LastPacketInBlock;

    /// <summary>Byte offset of this packet's data within the block.</summary>
    public long OffsetInBlock => Header.OffsetInBlock;

    /// <summary>Length of the data payload.</summary>
    public int DataLength => Header.DataLen;
}
```

### 8.3 `PacketReader` Class

**File:** `src/Gtlm.Hdfs.Client/Protocol/PacketReader.cs`

```csharp
namespace Gtlm.Hdfs.Client.Protocol;

using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using Google.Protobuf;
using Gtlm.Hdfs.Client.Checksum;
using Gtlm.Hdfs.Client.Proto;

/// <summary>
/// Reads and parses HDFS data transfer packets from a PipeReader.
/// Handles the binary framing, protobuf header deserialization, and
/// checksum/data region slicing.
/// </summary>
internal sealed class PacketReader
{
    private readonly PipeReader _reader;
    private readonly DataChecksum _checksum;

    public PacketReader(PipeReader reader, DataChecksum checksum)
    {
        _reader = reader;
        _checksum = checksum;
    }

    /// <summary>
    /// Read the next packet from the stream.
    /// Returns the parsed packet with header, checksum, and data regions.
    ///
    /// The data is copied into a rented buffer (owned by the caller).
    /// The caller must return the buffer to ArrayPool when done.
    /// </summary>
    public async ValueTask<PacketData> ReadNextPacketAsync(CancellationToken ct = default)
    {
        // Phase 1: Read the packet length prefix (4 bytes)
        int packetLen = await ReadPacketLengthAsync(ct);

        // Phase 2: Read headerLen (2 bytes) + rest of packet
        int remaining = packetLen;
        var (headerLen, header) = await ReadPacketHeaderAsync(remaining, ct);
        remaining -= (2 + headerLen);

        // Phase 3: Compute checksum and data region sizes
        int dataLen = header.DataLen;
        int checksumLen = _checksum.GetChecksumBytesForDataLength(dataLen);

        // Phase 4: Read checksums + data
        var (checksums, data) = await ReadChecksumAndDataAsync(checksumLen, dataLen, ct);

        return new PacketData
        {
            Header = header,
            Checksums = checksums,
            Data = data,
        };
    }

    private async ValueTask<int> ReadPacketLengthAsync(CancellationToken ct)
    {
        while (true)
        {
            var result = await _reader.ReadAsync(ct);
            var buffer = result.Buffer;

            if (buffer.Length >= 4)
            {
                int packetLen = ReadInt32BigEndian(buffer);
                _reader.AdvanceTo(buffer.GetPosition(4));
                return packetLen;
            }

            if (result.IsCompleted)
                throw new EndOfStreamException("Connection closed while reading packet length.");

            _reader.AdvanceTo(buffer.Start, buffer.End);
        }
    }

    private async ValueTask<(int headerLen, PacketHeaderProto header)>
        ReadPacketHeaderAsync(int remaining, CancellationToken ct)
    {
        while (true)
        {
            var result = await _reader.ReadAsync(ct);
            var buffer = result.Buffer;

            if (buffer.Length >= 2)
            {
                int headerLen = ReadInt16BigEndian(buffer);

                // Need headerLen more bytes for the proto
                if (buffer.Length >= 2 + headerLen)
                {
                    var headerBytes = buffer.Slice(2, headerLen);
                    var header = PacketHeaderProto.Parser.ParseFrom(headerBytes.ToArray());
                    _reader.AdvanceTo(buffer.GetPosition(2 + headerLen));
                    return (headerLen, header);
                }
            }

            if (result.IsCompleted)
                throw new EndOfStreamException("Connection closed while reading packet header.");

            _reader.AdvanceTo(buffer.Start, buffer.End);
        }
    }

    private async ValueTask<(ReadOnlyMemory<byte> checksums, ReadOnlyMemory<byte> data)>
        ReadChecksumAndDataAsync(int checksumLen, int dataLen, CancellationToken ct)
    {
        int totalNeeded = checksumLen + dataLen;

        if (totalNeeded == 0)
            return (ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty);

        while (true)
        {
            var result = await _reader.ReadAsync(ct);
            var buffer = result.Buffer;

            if (buffer.Length >= totalNeeded)
            {
                // Copy checksums and data into owned buffers
                // (PipeReader memory is only valid until next AdvanceTo)
                var checksums = buffer.Slice(0, checksumLen).ToArray();
                var data = buffer.Slice(checksumLen, dataLen).ToArray();

                _reader.AdvanceTo(buffer.GetPosition(totalNeeded));
                return (checksums, data);
            }

            if (result.IsCompleted)
                throw new EndOfStreamException(
                    $"Connection closed while reading packet payload. " +
                    $"Expected {totalNeeded} bytes, got {buffer.Length}.");

            _reader.AdvanceTo(buffer.Start, buffer.End);
        }
    }

    // --- Big-endian read helpers ---

    private static int ReadInt32BigEndian(ReadOnlySequence<byte> buffer)
    {
        Span<byte> span = stackalloc byte[4];
        buffer.Slice(0, 4).CopyTo(span);
        return BinaryPrimitives.ReadInt32BigEndian(span);
    }

    private static short ReadInt16BigEndian(ReadOnlySequence<byte> buffer)
    {
        Span<byte> span = stackalloc byte[2];
        buffer.Slice(0, 2).CopyTo(span);
        return BinaryPrimitives.ReadInt16BigEndian(span);
    }
}
```

### 8.4 Performance Optimization Notes

**Current approach:** `ToArray()` copies checksum and data bytes out of the `PipeReader`
buffer. This is correct and simple but allocates.

**Future optimization (post-MVP):**
- Use `ArrayPool<byte>.Shared.Rent()` for the data buffer and return it after the
  consumer has processed the data. This requires careful lifetime management.
- For very large packets (64KB+), consider processing the data directly from the
  `ReadOnlySequence<byte>` without copying, by holding the `PipeReader` unconsumed
  until the consumer is done. This requires changes to the `RemoteBlockReader` to
  call `AdvanceTo` after copying to the caller's buffer.

**`PacketHeaderProto` parsing:** The protobuf uses fixed-width fields (`sfixed64`,
`sfixed32`, `bool`) so parsing is fast -- no varint decoding for the primary fields.
Still, `ParseFrom(byte[])` allocates. A custom binary parser for the 5 fixed fields
could avoid this allocation entirely in the future.

### 8.5 Handling Special Packets

- **Last packet:** `header.LastPacketInBlock == true` and `header.DataLen == 0`.
  No checksums or data follow. The `PacketData.Data` will be empty.
- **Heartbeat packets:** `seqno == -1`. These should be silently skipped in the read
  loop. The `RemoteBlockReader` should call `ReadNextPacketAsync` again.

```csharp
// In RemoteBlockReader.ReadNextPacketAsync wrapper:
PacketData packet;
do
{
    packet = await _packetReader.ReadNextPacketAsync(ct);
} while (packet.Header.Seqno == -1); // Skip heartbeats
```

---

## Wire Format Example

A packet with 1024 bytes of data, CRC32C checksums, 512 bytes per checksum:

```
Offset  Size   Value        Description
------  ----   -----        -----------
0       4      0x00000422   packetLen = 1058 (2 + ~25 + 8 + 1024)
4       2      0x0019       headerLen = 25 (PacketHeaderProto size)
6       25     [protobuf]   PacketHeaderProto: offset=0, seqno=0, last=false, dataLen=1024
31      8      [CRC x 2]    Two 4-byte CRC32C values (one per 512-byte chunk)
39      1024   [block data] Actual block content
```

---

## Acceptance Criteria

- [ ] Correctly parses packets with known binary data
- [ ] Handles multi-segment `ReadOnlySequence<byte>` (when PipeReader spans multiple
      memory segments)
- [ ] Empty last packet (dataLen=0, lastPacketInBlock=true) parsed correctly
- [ ] Heartbeat packets (seqno=-1) are identifiable
- [ ] `EndOfStreamException` thrown on premature connection close
- [ ] Big-endian int32/int16 read correctly on little-endian platforms
- [ ] Unit test: construct a synthetic packet byte array, feed through a `Pipe`,
      verify `PacketData` fields match expected values
