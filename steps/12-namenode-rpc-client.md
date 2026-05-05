# Step 12: NameNode RPC Client

**Phase:** 2 (Full Client)
**Prerequisites:** Steps 01-09 (Phase 1 complete)
**Produces:** NameNode RPC layer for resolving file paths to block locations

---

## Objective

Implement the Hadoop IPC protocol client for communicating with the HDFS NameNode.
This enables the library to resolve file paths to `LocatedBlock` lists (DataNode
addresses, block IDs, tokens, offsets) -- the metadata needed to drive
`RemoteBlockReader`.

The NameNode RPC is a separate protocol from the DataNode data transfer protocol
(Step 05-06). It uses Hadoop's custom IPC framing with protobuf payloads over TCP
port 8020.

---

## Tasks

### 12.1 Additional Proto Files

Download and configure codegen for the NameNode RPC proto files:

```bash
# ClientNamenodeProtocol -- the main NameNode RPC interface
curl -sL https://raw.githubusercontent.com/apache/hadoop/trunk/hadoop-hdfs-project/hadoop-hdfs-client/src/main/proto/ClientNamenodeProtocol.proto \
  -o src/Gtlm.Hdfs.Client/Proto/ClientNamenodeProtocol.proto

# IPC connection context and RPC header
curl -sL https://raw.githubusercontent.com/apache/hadoop/trunk/hadoop-common-project/hadoop-common/src/main/proto/IpcConnectionContext.proto \
  -o src/Gtlm.Hdfs.Client/Proto/IpcConnectionContext.proto

curl -sL https://raw.githubusercontent.com/apache/hadoop/trunk/hadoop-common-project/hadoop-common/src/main/proto/RpcHeader.proto \
  -o src/Gtlm.Hdfs.Client/Proto/RpcHeader.proto

curl -sL https://raw.githubusercontent.com/apache/hadoop/trunk/hadoop-common-project/hadoop-common/src/main/proto/ProtobufRpcEngine.proto \
  -o src/Gtlm.Hdfs.Client/Proto/ProtobufRpcEngine.proto
```

Trim `ClientNamenodeProtocol.proto` to only the methods we need:
- `getBlockLocations`
- `getFileInfo`
- `getListing`
- `getServerDefaults`

### 12.2 Hadoop IPC Wire Format

The NameNode RPC uses a custom framing protocol:

**Connection handshake (once per connection):**
```
[4 bytes: "hrpc" magic]
[1 byte: version (9)]
[1 byte: service class (0)]
[1 byte: auth method (0=SIMPLE, 1=KERBEROS, etc.)]
[4 bytes BE: length of IpcConnectionContextProto]
[N bytes: IpcConnectionContextProto]
```

**Per-RPC call:**
```
[4 bytes BE: total frame length]
[RpcRequestHeaderProto (varint-prefixed)]
[RequestHeaderProto (varint-prefixed)]   -- method name, protocol
[Request body (varint-prefixed)]          -- e.g., GetBlockLocationsRequestProto
```

**Response:**
```
[4 bytes BE: total frame length]
[RpcResponseHeaderProto (varint-prefixed)]
[Response body (varint-prefixed)]         -- e.g., GetBlockLocationsResponseProto
```

### 12.3 `NameNodeRpcClient` Class

**File:** `src/Gtlm.Hdfs.Client/Rpc/NameNodeRpcClient.cs`

```csharp
namespace Gtlm.Hdfs.Client.Rpc;

public sealed class NameNodeRpcClient : IAsyncDisposable
{
    private readonly string _host;
    private readonly int _port;
    private readonly HdfsClientOptions _options;
    private Socket? _socket;
    private NetworkStream? _stream;
    private int _callId;

    public NameNodeRpcClient(string host, int port, HdfsClientOptions options);

    /// <summary>Connect and send the IPC handshake.</summary>
    public async Task ConnectAsync(CancellationToken ct = default);

    /// <summary>
    /// Get block locations for a file.
    /// Returns the list of LocatedBlocks covering [offset, offset+length).
    /// </summary>
    public async Task<IReadOnlyList<LocatedBlock>> GetBlockLocationsAsync(
        string path, long offset, long length, CancellationToken ct = default);

    /// <summary>Get file/directory metadata.</summary>
    public async Task<HdfsFileStatus?> GetFileInfoAsync(
        string path, CancellationToken ct = default);

    /// <summary>List directory contents.</summary>
    public async Task<IReadOnlyList<HdfsFileStatus>> GetListingAsync(
        string path, CancellationToken ct = default);

    /// <summary>Generic RPC call helper.</summary>
    private async Task<TResponse> CallAsync<TRequest, TResponse>(
        string methodName, TRequest request, CancellationToken ct)
        where TRequest : Google.Protobuf.IMessage
        where TResponse : Google.Protobuf.IMessage<TResponse>, new();
}
```

### 12.4 IPC Connection Handshake

```csharp
private async Task SendConnectionHandshakeAsync(CancellationToken ct)
{
    var header = new byte[7];
    // Magic: "hrpc"
    header[0] = (byte)'h';
    header[1] = (byte)'r';
    header[2] = (byte)'p';
    header[3] = (byte)'c';
    // Version: 9
    header[4] = 9;
    // Service class: 0
    header[5] = 0;
    // Auth method: 0 (SIMPLE) -- Phase 2 will support KERBEROS
    header[6] = 0;

    await _stream!.WriteAsync(header, ct);

    // Send IpcConnectionContextProto
    var context = new IpcConnectionContextProto
    {
        UserInfo = new UserInformationProto
        {
            EffectiveUser = _options.ClientName,
        },
        Protocol = "org.apache.hadoop.hdfs.protocol.ClientProtocol",
    };

    // Write as: [4 bytes BE length][RpcRequestHeaderProto][IpcConnectionContextProto]
    // The first call bundles the connection context with callId = -3
    await SendRpcFrameAsync(callId: -3, context, ct);
}
```

### 12.5 RPC Call Frame Serialization

```csharp
private async Task SendRpcFrameAsync(int callId, IMessage payload, CancellationToken ct)
{
    var rpcHeader = new RpcRequestHeaderProto
    {
        RpcKind = RpcKindProto.RpcProtocolBuffer,
        RpcOp = RpcRequestHeaderProto.Types.OperationProto.RpcFinalPacket,
        CallId = callId,
        ClientId = _clientIdBytes,
    };

    // Serialize all parts
    byte[] rpcHeaderBytes = rpcHeader.ToByteArray();
    byte[] payloadBytes = payload.ToByteArray();

    // Frame: [4 bytes total length][varint + rpcHeader][varint + payload]
    using var ms = new MemoryStream();
    ms.Position = 4; // Reserve space for length
    WriteDelimited(ms, rpcHeaderBytes);
    WriteDelimited(ms, payloadBytes);

    // Write total length at position 0
    int totalLen = (int)ms.Position - 4;
    ms.Position = 0;
    var lenBytes = new byte[4];
    BinaryPrimitives.WriteInt32BigEndian(lenBytes, totalLen);
    ms.Write(lenBytes);

    await _stream!.WriteAsync(ms.ToArray(), ct);
}
```

### 12.6 Response Parsing

```csharp
private async Task<TResponse> ReadResponseAsync<TResponse>(CancellationToken ct)
    where TResponse : IMessage<TResponse>, new()
{
    // Read 4-byte frame length
    var lenBuf = new byte[4];
    await _stream!.ReadExactlyAsync(lenBuf, ct);
    int frameLen = BinaryPrimitives.ReadInt32BigEndian(lenBuf);

    // Read full frame
    var frame = new byte[frameLen];
    await _stream!.ReadExactlyAsync(frame, ct);

    using var ms = new MemoryStream(frame);

    // Parse RpcResponseHeaderProto
    var responseHeader = RpcResponseHeaderProto.Parser.ParseDelimitedFrom(ms);

    if (responseHeader.Status != RpcResponseHeaderProto.Types.RpcStatusProto.Success)
    {
        throw new HdfsProtocolException(
            $"NameNode RPC error: {responseHeader.Status}, " +
            $"exception={responseHeader.ExceptionClassName}, " +
            $"message={responseHeader.ErrorMsg}");
    }

    // Parse response body
    var parser = new MessageParser<TResponse>(() => new TResponse());
    return parser.ParseDelimitedFrom(ms);
}
```

### 12.7 `GetBlockLocations` Implementation

```csharp
public async Task<IReadOnlyList<LocatedBlock>> GetBlockLocationsAsync(
    string path, long offset, long length, CancellationToken ct = default)
{
    var request = new GetBlockLocationsRequestProto
    {
        Src = path,
        Offset = (ulong)offset,
        Length = (ulong)length,
    };

    var response = await CallAsync<GetBlockLocationsRequestProto,
        GetBlockLocationsResponseProto>("getBlockLocations", request, ct);

    var locations = response.Locations;
    if (locations == null)
        return [];

    return locations.Blocks
        .Select(block => new LocatedBlock
        {
            Block = ExtendedBlock.FromProto(block.B),
            Offset = (long)block.Offset,
            Locations = block.Locs
                .Select(DatanodeInfo.FromProto)
                .ToList(),
            Token = BlockToken.FromProto(block.BlockToken),
            IsLastBlock = block == locations.Blocks.Last() && locations.UnderConstruction == false,
        })
        .ToList();
}
```

---

## Design Decisions

- **Single TCP connection:** NameNode RPC uses a long-lived connection with multiplexed
  call IDs. One connection per `NameNodeRpcClient` instance.
- **Synchronous call IDs:** For simplicity in Phase 2, use sequential call IDs and
  wait for each response before sending the next call. Concurrent calls can be added
  later with a `callId → TaskCompletionSource` map.
- **SIMPLE auth only in Phase 2 initial:** Kerberos auth is handled in Step 14.

---

## Acceptance Criteria

- [ ] Connects to a NameNode and completes the IPC handshake
- [ ] `GetBlockLocationsAsync` returns correct block locations for a known file
- [ ] `GetFileInfoAsync` returns correct file metadata
- [ ] RPC errors throw `HdfsProtocolException` with the NameNode's error message
- [ ] Call ID increments correctly across multiple calls
- [ ] Connection is cleanly closed on `DisposeAsync`
- [ ] Integration test: resolve block locations via RPC, then read blocks via
      `RemoteBlockReader` (end-to-end path resolution)
