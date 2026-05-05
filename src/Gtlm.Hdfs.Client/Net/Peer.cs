namespace Gtlm.Hdfs.Client.Net;

using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using Gtlm.Hdfs.Client.Configuration;
using Gtlm.Hdfs.Client.Models;

/// <summary>
/// A managed TCP connection to an HDFS DataNode.
/// Provides PipeReader/PipeWriter for the packet streaming phase
/// and raw Stream access for the protobuf handshake phase.
/// </summary>
public sealed class Peer : IAsyncDisposable
{
    private readonly Socket? _socket;
    private readonly Stream _readStream;
    private readonly Stream _writeStream;
    private bool _disposed;

    /// <summary>PipeReader for zero-copy packet parsing (streaming phase).</summary>
    public PipeReader Input { get; }

    /// <summary>PipeWriter for sending data (streaming phase).</summary>
    public PipeWriter Output { get; }

    /// <summary>The DataNode this peer is connected to.</summary>
    public DatanodeInfo DataNode { get; }

    /// <summary>Whether the underlying connection is closed or disposed.</summary>
    public bool IsClosed => _disposed || (_socket is not null && !_socket.Connected);

    private Peer(
        Socket? socket,
        Stream readStream,
        Stream writeStream,
        PipeReader input,
        PipeWriter output,
        DatanodeInfo dataNode)
    {
        _socket = socket;
        _readStream = readStream;
        _writeStream = writeStream;
        Input = input;
        Output = output;
        DataNode = dataNode;
    }

    /// <summary>
    /// Connect to a DataNode and return a new Peer.
    /// </summary>
    public static async Task<Peer> ConnectAsync(
        DatanodeInfo dataNode,
        HdfsClientOptions options,
        CancellationToken ct = default)
    {
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
        socket.NoDelay = true;

        try
        {
            using var connectCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            connectCts.CancelAfter(options.ConnectTimeout);

            var endpoint = new IPEndPoint(IPAddress.Parse(dataNode.IpAddress), dataNode.XferPort);
            await socket.ConnectAsync(endpoint, connectCts.Token);

            socket.ReceiveBufferSize = options.RemoteBufferSize;
            socket.SendBufferSize = options.RemoteBufferSize;

            var stream = new NetworkStream(socket, ownsSocket: false);

            var readerOptions = new StreamPipeReaderOptions(
                bufferSize: options.RemoteBufferSize,
                minimumReadSize: 4096,
                leaveOpen: true);

            var input = PipeReader.Create(stream, readerOptions);
            var output = PipeWriter.Create(stream, new StreamPipeWriterOptions(leaveOpen: true));

            return new Peer(socket, stream, stream, input, output, dataNode);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Create a Peer from an existing stream (for testing with in-memory pipes).
    /// </summary>
    internal static Peer CreateForTest(
        Stream stream,
        DatanodeInfo dataNode,
        int bufferSize = 65536)
    {
        var readerOptions = new StreamPipeReaderOptions(
            bufferSize: bufferSize,
            minimumReadSize: 1,
            leaveOpen: true);

        var input = PipeReader.Create(stream, readerOptions);
        var output = PipeWriter.Create(stream, new StreamPipeWriterOptions(leaveOpen: true));

        return new Peer(socket: null, stream, stream, input, output, dataNode);
    }

    /// <summary>
    /// Create a Peer from separate read/write streams (for testing).
    /// </summary>
    internal static Peer CreateForTest(
        Stream readStream,
        Stream writeStream,
        DatanodeInfo dataNode,
        int bufferSize = 65536)
    {
        var readerOptions = new StreamPipeReaderOptions(
            bufferSize: bufferSize,
            minimumReadSize: 1,
            leaveOpen: true);

        var input = PipeReader.Create(readStream, readerOptions);
        var output = PipeWriter.Create(writeStream, new StreamPipeWriterOptions(leaveOpen: true));

        return new Peer(socket: null, readStream, writeStream, input, output, dataNode);
    }

    /// <summary>
    /// Get the raw output stream for protobuf serialization during handshake.
    /// Must NOT be used concurrently with Output (PipeWriter).
    /// </summary>
    internal Stream GetOutputStream() => _writeStream;

    /// <summary>
    /// Get the raw input stream for protobuf deserialization during handshake.
    /// Must NOT be used concurrently with Input (PipeReader).
    /// </summary>
    internal Stream GetInputStream() => _readStream;

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        try { await Input.CompleteAsync(); } catch { /* best-effort */ }
        try { await Output.CompleteAsync(); } catch { /* best-effort */ }

        await _readStream.DisposeAsync();
        if (!ReferenceEquals(_readStream, _writeStream))
            await _writeStream.DisposeAsync();

        if (_socket is not null)
        {
            try { _socket.Shutdown(SocketShutdown.Both); } catch { /* may already be disconnected */ }
            _socket.Dispose();
        }
    }
}
