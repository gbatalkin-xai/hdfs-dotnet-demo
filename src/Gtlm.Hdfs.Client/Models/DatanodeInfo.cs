namespace Gtlm.Hdfs.Client.Models;

/// <summary>
/// Identity and network address of an HDFS DataNode.
/// </summary>
public sealed class DatanodeInfo : IEquatable<DatanodeInfo>
{
    public required string IpAddress { get; init; }
    public required string HostName { get; init; }
    public required string DatanodeUuid { get; init; }

    /// <summary>Data transfer port (default 9866 in Hadoop 3.x).</summary>
    public required int XferPort { get; init; }

    public int InfoPort { get; init; }
    public int IpcPort { get; init; }

    /// <summary>The endpoint used for data transfer connections.</summary>
    public string XferAddress => $"{IpAddress}:{XferPort}";

    internal static DatanodeInfo FromProto(Proto.DatanodeInfoProto proto) => new()
    {
        IpAddress = proto.Id.IpAddr,
        HostName = proto.Id.HostName,
        DatanodeUuid = proto.Id.DatanodeUuid,
        XferPort = (int)proto.Id.XferPort,
        InfoPort = (int)proto.Id.InfoPort,
        IpcPort = (int)proto.Id.IpcPort,
    };

    internal static DatanodeInfo FromProto(Proto.DatanodeIDProto proto) => new()
    {
        IpAddress = proto.IpAddr,
        HostName = proto.HostName,
        DatanodeUuid = proto.DatanodeUuid,
        XferPort = (int)proto.XferPort,
        InfoPort = (int)proto.InfoPort,
        IpcPort = (int)proto.IpcPort,
    };

    public bool Equals(DatanodeInfo? other) =>
        other is not null && DatanodeUuid == other.DatanodeUuid;

    public override bool Equals(object? obj) => Equals(obj as DatanodeInfo);
    public override int GetHashCode() => DatanodeUuid.GetHashCode();
    public override string ToString() => $"{HostName}({IpAddress}:{XferPort})";
}
