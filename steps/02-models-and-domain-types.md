# Step 02: Models and Domain Types

**Phase:** 1 (MVP)
**Prerequisites:** Step 01 (proto setup compiles)
**Produces:** `Models/` directory with domain types used throughout the codebase

---

## Objective

Create clean C# domain types that wrap the generated protobuf types. These provide a
type-safe, idiomatic C# API and decouple the rest of the codebase from raw proto classes.

---

## Tasks

### 2.1 `ExtendedBlock` -- Block Identity

**File:** `src/Gtlm.Hdfs.Client/Models/ExtendedBlock.cs`

```csharp
namespace Gtlm.Hdfs.Client.Models;

/// <summary>
/// Identifies a unique block across the HDFS cluster.
/// Wraps the poolId + blockId + generationStamp triple.
/// </summary>
public readonly record struct ExtendedBlock(
    string PoolId,
    long BlockId,
    long GenerationStamp,
    long NumBytes = 0)
{
    /// <summary>
    /// Convert to the protobuf wire type.
    /// </summary>
    internal Proto.ExtendedBlockProto ToProto() => new()
    {
        PoolId = PoolId,
        BlockId = (ulong)BlockId,
        GenerationStamp = (ulong)GenerationStamp,
        NumBytes = (ulong)NumBytes,
    };

    /// <summary>
    /// Create from a protobuf wire type.
    /// </summary>
    internal static ExtendedBlock FromProto(Proto.ExtendedBlockProto proto) => new(
        PoolId: proto.PoolId,
        BlockId: (long)proto.BlockId,
        GenerationStamp: (long)proto.GenerationStamp,
        NumBytes: (long)proto.NumBytes);
}
```

### 2.2 `DatanodeInfo` -- DataNode Address and Metadata

**File:** `src/Gtlm.Hdfs.Client/Models/DatanodeInfo.cs`

```csharp
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

    /// <summary>
    /// The endpoint used for data transfer connections.
    /// </summary>
    public string XferAddress => $"{IpAddress}:{XferPort}";

    internal static DatanodeInfo FromProto(Proto.DatanodeIDProto proto) => new()
    {
        IpAddress = proto.IpAddr,
        HostName = proto.HostName,
        DatanodeUuid = proto.DatanodeUuid,
        XferPort = (int)proto.XferPort,
        InfoPort = (int)proto.InfoPort,
        IpcPort = (int)proto.IpcPort,
    };

    // Equality by UUID (globally unique per DataNode)
    public bool Equals(DatanodeInfo? other) => other is not null
        && DatanodeUuid == other.DatanodeUuid;

    public override bool Equals(object? obj) => Equals(obj as DatanodeInfo);
    public override int GetHashCode() => DatanodeUuid.GetHashCode();
    public override string ToString() => $"{HostName}({IpAddress}:{XferPort})";
}
```

### 2.3 `BlockToken` -- Block Access Token

**File:** `src/Gtlm.Hdfs.Client/Models/BlockToken.cs`

```csharp
namespace Gtlm.Hdfs.Client.Models;

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
        Identifier = Google.Protobuf.ByteString.CopyFrom(Identifier),
        Password = Google.Protobuf.ByteString.CopyFrom(Password),
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
```

### 2.4 `LocatedBlock` -- Block Location (Phase 2 prep, stub now)

**File:** `src/Gtlm.Hdfs.Client/Models/LocatedBlock.cs`

```csharp
namespace Gtlm.Hdfs.Client.Models;

/// <summary>
/// A block together with its DataNode locations and access token.
/// Returned by the NameNode for getBlockLocations.
/// </summary>
public sealed class LocatedBlock
{
    public required ExtendedBlock Block { get; init; }

    /// <summary>Byte offset of this block within the file.</summary>
    public required long Offset { get; init; }

    /// <summary>DataNode locations sorted by network distance (nearest first).</summary>
    public required IReadOnlyList<DatanodeInfo> Locations { get; init; }

    /// <summary>Security token for accessing this block.</summary>
    public required BlockToken Token { get; init; }

    public bool IsLastBlock { get; init; }
}
```

### 2.5 `HdfsFileStatus` -- File Metadata (Phase 2 prep, stub now)

**File:** `src/Gtlm.Hdfs.Client/Models/HdfsFileStatus.cs`

```csharp
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
```

---

## Design Decisions

- **`record struct` for `ExtendedBlock`:** Immutable value type -- blocks are compared
  by value (poolId + blockId + genStamp) and passed around frequently. Struct avoids
  heap allocation.
- **`class` for `DatanodeInfo`:** Reference type because it's stored in caches and
  collections. Equality by `DatanodeUuid` which is globally unique.
- **`FromProto` / `ToProto` as `internal`:** Proto conversion is an implementation
  detail. Consumers work with the domain types only.
- **Phase 2 types stubbed now:** `LocatedBlock` and `HdfsFileStatus` are defined with
  minimal properties so that Phase 1 code can reference them in signatures. The full
  `FromProto` conversion will be added in Step 12 (NameNode RPC).

---

## Acceptance Criteria

- [ ] All five model types compile
- [ ] `ExtendedBlock.ToProto()` round-trips through `FromProto()` correctly
- [ ] `DatanodeInfo` equality works by UUID
- [ ] `BlockToken.Empty` provides a valid empty token for non-secure clusters
- [ ] No public dependency on `Google.Protobuf` types in the model APIs
