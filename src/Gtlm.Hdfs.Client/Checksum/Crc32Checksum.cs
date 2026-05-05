namespace Gtlm.Hdfs.Client.Checksum;

using System.IO.Hashing;

/// <summary>
/// CRC32 (IEEE 802.3) checksum. Used by older HDFS configurations.
/// Uses System.IO.Hashing.Crc32 which implements IEEE polynomial.
/// </summary>
internal sealed class Crc32Checksum : DataChecksum
{
    public Crc32Checksum(int bytesPerChecksum) : base(bytesPerChecksum, checksumSize: 4) { }

    protected override uint ComputeChecksum(ReadOnlySpan<byte> data)
    {
        return ComputeIeeeCrc32(data);
    }

    internal static uint ComputeIeeeCrc32(ReadOnlySpan<byte> data)
    {
        return Crc32.HashToUInt32(data);
    }
}
