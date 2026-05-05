namespace Gtlm.Hdfs.Client.Models;

/// <summary>
/// Metadata for an HDFS file or directory.
/// </summary>
public sealed class HdfsFileStatus
{
    public required string Path { get; init; }
    public required long Length { get; init; }
    public required bool IsDirectory { get; init; }
    public required long BlockSize { get; init; }
    public required short Replication { get; init; }
    public required long ModificationTime { get; init; }
    public required long AccessTime { get; init; }
    public string Owner { get; init; } = "";
    public string Group { get; init; } = "";
    public int Permission { get; init; } = 0x1FF; // 0777
}
