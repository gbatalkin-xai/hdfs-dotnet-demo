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
}
