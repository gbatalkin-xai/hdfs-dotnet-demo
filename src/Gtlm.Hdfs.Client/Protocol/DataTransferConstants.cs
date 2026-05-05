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
