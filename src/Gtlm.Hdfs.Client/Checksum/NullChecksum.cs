namespace Gtlm.Hdfs.Client.Checksum;

/// <summary>
/// No-op checksum. Used when CHECKSUM_NULL is negotiated.
/// </summary>
internal sealed class NullChecksum : DataChecksum
{
    public NullChecksum(int bytesPerChecksum) : base(bytesPerChecksum, checksumSize: 0) { }

    protected override uint ComputeChecksum(ReadOnlySpan<byte> data) => 0;
}
