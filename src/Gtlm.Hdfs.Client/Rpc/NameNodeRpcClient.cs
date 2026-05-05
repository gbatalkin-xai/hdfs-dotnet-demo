namespace Gtlm.Hdfs.Client.Rpc;

using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using Google.Protobuf;
using Gtlm.Hdfs.Client.Configuration;
using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Protocol;
using Gtlm.Hdfs.Client.Proto;
using Microsoft.Extensions.Logging;

/// <summary>
/// RPC client for communicating with the HDFS NameNode using Hadoop IPC protocol.
/// Implements getBlockLocations, getFileInfo, getListing.
/// </summary>
public sealed class NameNodeRpcClient : IAsyncDisposable
{
    private const string ClientProtocol =
        "org.apache.hadoop.hdfs.protocol.ClientProtocol";
    private const byte IpcVersion = 9;
    private const byte AuthMethodSimple = 0x80; // SIMPLE auth
    private const int ConnectionContextCallId = -3;

    private readonly string _host;
    private readonly int _port;
    private readonly HdfsClientOptions _options;
    private readonly ILogger? _logger;
    private readonly byte[] _clientId;

    private Socket? _socket;
    private NetworkStream? _stream;
    private int _callId;
    private bool _disposed;

    public NameNodeRpcClient(
        string host, int port,
        HdfsClientOptions options,
        ILogger? logger = null)
    {
        _host = host;
        _port = port;
        _options = options;
        _logger = logger;
        _clientId = Guid.NewGuid().ToByteArray();
    }

    public NameNodeRpcClient(HdfsClientOptions options, ILogger? logger = null)
        : this(options.NameNodeHost, options.NameNodePort, options, logger) { }

    /// <summary>Connect and send the IPC handshake.</summary>
    public async Task ConnectAsync(CancellationToken ct = default)
    {
        _socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
        _socket.NoDelay = true;

        using var connectCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        connectCts.CancelAfter(_options.ConnectTimeout);

        var endpoint = new IPEndPoint(
            (await Dns.GetHostAddressesAsync(_host, ct))[0], _port);
        await _socket.ConnectAsync(endpoint, connectCts.Token);

        _stream = new NetworkStream(_socket, ownsSocket: false);

        await SendConnectionHeaderAsync(ct);
        await SendConnectionContextAsync(ct);

        _logger?.LogDebug("Connected to NameNode {Host}:{Port}", _host, _port);
    }

    // --- Public RPC methods ---

    public async Task<IReadOnlyList<LocatedBlock>> GetBlockLocationsAsync(
        string path, long offset, long length, CancellationToken ct = default)
    {
        var request = new GetBlockLocationsRequestProto
        {
            Src = path,
            Offset = (ulong)offset,
            Length = (ulong)length,
        };

        var response = await CallAsync<GetBlockLocationsResponseProto>(
            "getBlockLocations", request, ct);

        if (response.Locations == null)
            return [];

        return response.Locations.Blocks
            .Select(LocatedBlock.FromProto)
            .ToList();
    }

    public async Task<HdfsFileStatus?> GetFileInfoAsync(
        string path, CancellationToken ct = default)
    {
        var request = new GetFileInfoRequestProto { Src = path };
        var response = await CallAsync<GetFileInfoResponseProto>(
            "getFileInfo", request, ct);

        if (response.Fs == null)
            return null;

        return ConvertFileStatus(response.Fs, path);
    }

    public async Task<IReadOnlyList<HdfsFileStatus>> GetListingAsync(
        string path, CancellationToken ct = default)
    {
        var request = new GetListingRequestProto
        {
            Src = path,
            StartAfter = ByteString.Empty,
            NeedLocation = false,
        };

        var response = await CallAsync<GetListingResponseProto>(
            "getListing", request, ct);

        if (response.DirList == null)
            return [];

        return response.DirList.PartialListing
            .Select(fs => ConvertFileStatus(fs, path))
            .ToList();
    }

    // --- IPC protocol implementation ---

    /// <summary>
    /// Send the 7-byte IPC connection header:
    ///   [4 bytes: "hrpc" magic]
    ///   [1 byte: version (9)]
    ///   [1 byte: service class (0)]
    ///   [1 byte: auth method (0x80 = SIMPLE)]
    /// </summary>
    private async Task SendConnectionHeaderAsync(CancellationToken ct)
    {
        var header = new byte[]
        {
            (byte)'h', (byte)'r', (byte)'p', (byte)'c',
            IpcVersion,
            0, // service class
            AuthMethodSimple,
        };
        await _stream!.WriteAsync(header, ct);
        await _stream.FlushAsync(ct);
    }

    /// <summary>
    /// Send the connection context as a special RPC call with callId = -3.
    /// </summary>
    private async Task SendConnectionContextAsync(CancellationToken ct)
    {
        var context = new IpcConnectionContextProto
        {
            UserInfo = new UserInformationProto
            {
                EffectiveUser = _options.ClientName,
            },
            Protocol = ClientProtocol,
        };

        var rpcHeader = new RpcRequestHeaderProto
        {
            RpcKind = RpcKindProto.RpcProtocolBuffer,
            RpcOp = RpcRequestHeaderProto.Types.OperationProto.RpcFinalPacket,
            CallId = ConnectionContextCallId,
            ClientId = ByteString.CopyFrom(_clientId),
        };

        await SendRpcFrameAsync(rpcHeader, context, ct);
    }

    /// <summary>
    /// Send an RPC call and wait for the response.
    /// </summary>
    private async Task<TResponse> CallAsync<TResponse>(
        string methodName, IMessage request, CancellationToken ct)
        where TResponse : IMessage<TResponse>, new()
    {
        int callId = _callId++;

        // Build RPC request header
        var rpcHeader = new RpcRequestHeaderProto
        {
            RpcKind = RpcKindProto.RpcProtocolBuffer,
            RpcOp = RpcRequestHeaderProto.Types.OperationProto.RpcFinalPacket,
            CallId = callId,
            ClientId = ByteString.CopyFrom(_clientId),
        };

        // Build protobuf RPC engine header
        var rpcEngineHeader = new RequestHeaderProto
        {
            MethodName = methodName,
            DeclaringClassProtocolName = ClientProtocol,
            ClientProtocolVersion = 1,
        };

        // Send: [4B frame len][rpcHeader delimited][rpcEngineHeader delimited][request delimited]
        await SendRpcCallAsync(rpcHeader, rpcEngineHeader, request, ct);

        // Read response
        return await ReadResponseAsync<TResponse>(callId, ct);
    }

    private async Task SendRpcFrameAsync(
        RpcRequestHeaderProto rpcHeader, IMessage payload, CancellationToken ct)
    {
        using var ms = new MemoryStream();
        ms.Position = 4; // Reserve for length prefix

        rpcHeader.WriteDelimitedTo(ms);
        payload.WriteDelimitedTo(ms);

        // Write total length at position 0
        int totalLen = (int)ms.Position - 4;
        ms.Position = 0;
        var lenBuf = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(lenBuf, totalLen);
        ms.Write(lenBuf);

        await _stream!.WriteAsync(ms.ToArray(), ct);
        await _stream.FlushAsync(ct);
    }

    private async Task SendRpcCallAsync(
        RpcRequestHeaderProto rpcHeader,
        RequestHeaderProto engineHeader,
        IMessage request,
        CancellationToken ct)
    {
        using var ms = new MemoryStream();
        ms.Position = 4; // Reserve for length prefix

        rpcHeader.WriteDelimitedTo(ms);
        engineHeader.WriteDelimitedTo(ms);
        request.WriteDelimitedTo(ms);

        int totalLen = (int)ms.Position - 4;
        ms.Position = 0;
        var lenBuf = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(lenBuf, totalLen);
        ms.Write(lenBuf);

        await _stream!.WriteAsync(ms.ToArray(), ct);
        await _stream.FlushAsync(ct);
    }

    private async Task<TResponse> ReadResponseAsync<TResponse>(
        int expectedCallId, CancellationToken ct)
        where TResponse : IMessage<TResponse>, new()
    {
        // Read 4-byte frame length
        var lenBuf = new byte[4];
        await _stream!.ReadExactlyAsync(lenBuf, ct);
        int frameLen = BinaryPrimitives.ReadInt32BigEndian(lenBuf);

        // Read full frame
        var frame = new byte[frameLen];
        await _stream.ReadExactlyAsync(frame, ct);

        using var ms = new MemoryStream(frame);

        // Parse RpcResponseHeaderProto
        var responseHeader = RpcResponseHeaderProto.Parser.ParseDelimitedFrom(ms);

        if (responseHeader.CallId != (uint)expectedCallId)
        {
            throw new HdfsProtocolException(
                $"NameNode RPC call ID mismatch: expected {expectedCallId}, " +
                $"got {responseHeader.CallId}");
        }

        if (responseHeader.Status != RpcResponseHeaderProto.Types.RpcStatusProto.Success)
        {
            throw new HdfsProtocolException(
                $"NameNode RPC error: status={responseHeader.Status}, " +
                $"exception={responseHeader.ExceptionClassName}, " +
                $"message={responseHeader.ErrorMsg}");
        }

        // Parse response body
        return new MessageParser<TResponse>(() => new TResponse())
            .ParseDelimitedFrom(ms);
    }

    // --- Helpers ---

    private static HdfsFileStatus ConvertFileStatus(HdfsFileStatusProto fs, string parentPath)
    {
        string name = System.Text.Encoding.UTF8.GetString(fs.Path.Span);
        string fullPath = string.IsNullOrEmpty(name)
            ? parentPath
            : parentPath.TrimEnd('/') + "/" + name;

        return new HdfsFileStatus
        {
            Path = fullPath,
            Length = (long)fs.Length,
            IsDirectory = fs.FileType == HdfsFileStatusProto.Types.FileType.IsDir,
            BlockSize = (long)fs.Blocksize,
            Replication = (short)fs.BlockReplication,
            ModificationTime = (long)fs.ModificationTime,
            AccessTime = (long)fs.AccessTime,
            Owner = fs.Owner,
            Group = fs.Group,
            Permission = (int)fs.Permission.Perm,
        };
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        if (_stream is not null)
            await _stream.DisposeAsync();

        if (_socket is not null)
        {
            try { _socket.Shutdown(SocketShutdown.Both); } catch { }
            _socket.Dispose();
        }
    }
}
