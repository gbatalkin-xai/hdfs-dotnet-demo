namespace Gtlm.Hdfs.Client.Models;

using Google.Protobuf;

/// <summary>
/// An opaque security token granting access to a specific block.
/// Obtained from the NameNode as part of LocatedBlock metadata.
/// </summary>
public sealed class BlockToken
{
    public required byte[] Identifier { get; init; }
    public required byte[] Password { get; init; }
    public required string Kind { get; init; }
    public required string Service { get; init; }

    /// <summary>Empty token for non-secure clusters.</summary>
    public static BlockToken Empty { get; } = new()
    {
        Identifier = [],
        Password = [],
        Kind = "",
        Service = "",
    };

    internal Proto.TokenProto ToProto() => new()
    {
        Identifier = ByteString.CopyFrom(Identifier),
        Password = ByteString.CopyFrom(Password),
        Kind = Kind,
        Service = Service,
    };

    internal static BlockToken FromProto(Proto.TokenProto proto) => new()
    {
        Identifier = proto.Identifier.ToByteArray(),
        Password = proto.Password.ToByteArray(),
        Kind = proto.Kind,
        Service = proto.Service,
    };
}
