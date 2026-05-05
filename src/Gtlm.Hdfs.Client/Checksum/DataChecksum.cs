namespace Gtlm.Hdfs.Client.Checksum;

using System.Buffers.Binary;
using Gtlm.Hdfs.Client.Protocol;
using Gtlm.Hdfs.Client.Proto;

/// <summary>
/// HDFS data checksum calculator and verifier.
/// Each instance is configured for a specific algorithm and chunk size.
/// </summary>
public abstract class DataChecksum
{
    /// <summary>Bytes of data covered by each checksum.</summary>
    public int BytesPerChecksum { get; }

    /// <summary>Size of each checksum value in bytes (4 for CRC32/CRC32C, 0 for NULL).</summary>
    public int ChecksumSize { get; }

    protected DataChecksum(int bytesPerChecksum, int checksumSize)
    {
        BytesPerChecksum = bytesPerChecksum;
        ChecksumSize = checksumSize;
    }

    /// <summary>
    /// Factory: create a DataChecksum from the proto checksum type.
    /// </summary>
    public static DataChecksum Create(ChecksumTypeProto type, int bytesPerChecksum)
    {
        return type switch
        {
            ChecksumTypeProto.ChecksumCrc32 => new Crc32Checksum(bytesPerChecksum),
            ChecksumTypeProto.ChecksumCrc32C => new Crc32CChecksum(bytesPerChecksum),
            ChecksumTypeProto.ChecksumNull => new NullChecksum(bytesPerChecksum),
            _ => throw new ArgumentException($"Unsupported checksum type: {type}"),
        };
    }

    /// <summary>
    /// Compute the checksum of a single chunk.
    /// </summary>
    protected abstract uint ComputeChecksum(ReadOnlySpan<byte> data);

    /// <summary>
    /// Verify checksums for a data buffer.
    /// Data is split into bytesPerChecksum-sized chunks.
    /// Each chunk's computed CRC is compared against the corresponding 4-byte
    /// big-endian value in the checksums buffer.
    /// Throws ChecksumException on mismatch.
    /// </summary>
    public void VerifyChunks(
        ReadOnlySpan<byte> data,
        ReadOnlySpan<byte> checksums,
        long offsetInBlock)
    {
        if (ChecksumSize == 0) return;

        int numChunks = (data.Length + BytesPerChecksum - 1) / BytesPerChecksum;
        int expectedChecksumBytes = numChunks * ChecksumSize;

        if (checksums.Length < expectedChecksumBytes)
        {
            throw new ChecksumException(
                $"Not enough checksum bytes: expected {expectedChecksumBytes}, got {checksums.Length}",
                offsetInBlock);
        }

        for (int i = 0; i < numChunks; i++)
        {
            int dataStart = i * BytesPerChecksum;
            int dataEnd = Math.Min(dataStart + BytesPerChecksum, data.Length);
            var chunk = data[dataStart..dataEnd];

            uint computed = ComputeChecksum(chunk);

            int csOffset = i * ChecksumSize;
            uint expected = BinaryPrimitives.ReadUInt32BigEndian(checksums[csOffset..]);

            if (computed != expected)
            {
                throw new ChecksumException(
                    $"Checksum mismatch at chunk {i} (offset {offsetInBlock + dataStart}): " +
                    $"expected 0x{expected:X8}, computed 0x{computed:X8}",
                    offsetInBlock + dataStart);
            }
        }
    }

    /// <summary>
    /// Calculate how many checksum bytes correspond to the given data length.
    /// </summary>
    public int GetChecksumBytesForDataLength(int dataLength)
    {
        if (dataLength == 0 || ChecksumSize == 0) return 0;
        int numChunks = (dataLength + BytesPerChecksum - 1) / BytesPerChecksum;
        return numChunks * ChecksumSize;
    }
}
