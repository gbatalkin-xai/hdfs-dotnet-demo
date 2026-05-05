namespace Gtlm.Hdfs.Client.Protocol;

using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using Google.Protobuf;
using Gtlm.Hdfs.Client.Checksum;
using Gtlm.Hdfs.Client.Proto;

/// <summary>
/// Reads and parses HDFS data transfer packets from a PipeReader.
///
/// Packet binary layout (big-endian):
///   [4 bytes: packetLen]
///   [2 bytes: headerLen]
///   [headerLen bytes: PacketHeaderProto]
///   [C bytes: checksums]
///   [D bytes: data]
///
/// Where C = ceil(D / bytesPerChecksum) * checksumSize
///       D = PacketHeaderProto.dataLen
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
    /// The returned PacketData owns its byte arrays (copied from pipe buffers).
    /// </summary>
    public async ValueTask<PacketData> ReadNextPacketAsync(CancellationToken ct = default)
    {
        // Phase 1: Read packetLen (4 bytes BE)
        int packetLen = await ReadInt32BigEndianAsync(ct);

        // Phase 2: Read headerLen (2 bytes BE)
        short headerLen = await ReadInt16BigEndianAsync(ct);

        // Phase 3: Read PacketHeaderProto
        var headerBytes = await ReadBytesAsync(headerLen, ct);
        var header = PacketHeaderProto.Parser.ParseFrom(headerBytes);

        int dataLen = header.DataLen;
        int checksumLen = _checksum.GetChecksumBytesForDataLength(dataLen);

        // Phase 4: Read remaining bytes (checksums + data)
        // packetLen includes: 2 (headerLen field) + headerLen + checksums + data + 4 (sizeof packetLen itself? depends on impl)
        // In HDFS, after reading packetLen and headerLen+header, the rest is checksums+data
        int payloadLen = checksumLen + dataLen;

        byte[] checksums;
        byte[] data;

        if (payloadLen == 0)
        {
            checksums = [];
            data = [];
        }
        else
        {
            var payload = await ReadBytesAsync(payloadLen, ct);
            checksums = payload[..checksumLen];
            data = payload[checksumLen..];
        }

        return new PacketData
        {
            Header = header,
            Checksums = checksums,
            Data = data,
        };
    }

    private async ValueTask<int> ReadInt32BigEndianAsync(CancellationToken ct)
    {
        while (true)
        {
            var result = await _reader.ReadAsync(ct);
            var buffer = result.Buffer;

            if (buffer.Length >= 4)
            {
                Span<byte> span = stackalloc byte[4];
                buffer.Slice(0, 4).CopyTo(span);
                _reader.AdvanceTo(buffer.GetPosition(4));
                return BinaryPrimitives.ReadInt32BigEndian(span);
            }

            if (result.IsCompleted)
                throw new EndOfStreamException(
                    $"Connection closed while reading 4-byte int. Got {buffer.Length} bytes.");

            _reader.AdvanceTo(buffer.Start, buffer.End);
        }
    }

    private async ValueTask<short> ReadInt16BigEndianAsync(CancellationToken ct)
    {
        while (true)
        {
            var result = await _reader.ReadAsync(ct);
            var buffer = result.Buffer;

            if (buffer.Length >= 2)
            {
                Span<byte> span = stackalloc byte[2];
                buffer.Slice(0, 2).CopyTo(span);
                _reader.AdvanceTo(buffer.GetPosition(2));
                return BinaryPrimitives.ReadInt16BigEndian(span);
            }

            if (result.IsCompleted)
                throw new EndOfStreamException(
                    $"Connection closed while reading 2-byte short. Got {buffer.Length} bytes.");

            _reader.AdvanceTo(buffer.Start, buffer.End);
        }
    }

    private async ValueTask<byte[]> ReadBytesAsync(int count, CancellationToken ct)
    {
        if (count == 0) return [];

        while (true)
        {
            var result = await _reader.ReadAsync(ct);
            var buffer = result.Buffer;

            if (buffer.Length >= count)
            {
                var bytes = new byte[count];
                buffer.Slice(0, count).CopyTo(bytes);
                _reader.AdvanceTo(buffer.GetPosition(count));
                return bytes;
            }

            if (result.IsCompleted)
                throw new EndOfStreamException(
                    $"Connection closed while reading {count} bytes. Got {buffer.Length} bytes.");

            _reader.AdvanceTo(buffer.Start, buffer.End);
        }
    }
}
