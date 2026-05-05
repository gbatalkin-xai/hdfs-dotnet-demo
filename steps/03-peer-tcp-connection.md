# Step 03: Peer (TCP Connection Wrapper)

**Phase:** 1 (MVP)
**Prerequisites:** Step 01 (project compiles), Step 02 (DatanodeInfo model)
**Produces:** `Net/Peer.cs` -- socket wrapper with `PipeReader`/`PipeWriter`

---

## Objective

Implement the `Peer` class that manages a TCP connection to a single DataNode. This is
the I/O foundation for all protocol communication. It wraps a raw `Socket` with
`System.IO.Pipelines` for efficient, zero-copy binary reads.

---

## Tasks

### 3.1 `Peer` Class

**File:** `src/Gtlm.Hdfs.Client/Net/Peer.cs`

```csharp
namespace Gtlm.Hdfs.Client.Net;

using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using Gtlm.Hdfs.Client.Configuration;
using Gtlm.Hdfs.Client.Models;

/// <summary>
/// A managed TCP connection to an HDFS DataNode.
/// Provides PipeReader/PipeWriter for the data transfer protocol.
/// </summary>
public sealed class Peer : IAsyncDisposable
{
    private readonly Socket _socket;
    private readonly NetworkStream _stream;
    private bool _disposed;

    public PipeReader Input { get; }
    public PipeWriter Output { get; }
    public DatanodeInfo DataNode { get; }
    public bool IsClosed => _disposed || !_socket.Connected;

    private Peer(Socket socket, NetworkStream stream, DatanodeInfo dataNode,
                 StreamPipeReaderOptions readerOptions)
    {
        _socket = socket;
        _stream = stream;
        DataNode = dataNode;
        Input = PipeReader.Create(_stream, readerOptions);
        Output = PipeWriter.Create(_stream, new StreamPipeWriterOptions(leaveOpen: true));
    }

    // ... (see detailed method specs below)
}
```

### 3.2 `ConnectAsync` -- Factory Method

```csharp
public static async Task<Peer> ConnectAsync(
    DatanodeInfo dataNode,
    HdfsClientOptions options,
    CancellationToken ct = default)
{
    var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
    socket.NoDelay = true;  // Disable Nagle for protocol messages

    try
    {
        var endpoint = new IPEndPoint(IPAddress.Parse(dataNode.IpAddress), dataNode.XferPort);

        using var connectCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        connectCts.CancelAfter(options.ConnectTimeout);

        await socket.ConnectAsync(endpoint, connectCts.Token);

        socket.ReceiveTimeout = (int)options.ReadTimeout.TotalMilliseconds;
        socket.SendTimeout = (int)options.ReadTimeout.TotalMilliseconds;

        // Set receive buffer to match expected packet sizes
        socket.ReceiveBufferSize = options.RemoteBufferSize;

        var stream = new NetworkStream(socket, ownsSocket: false);

        var readerOptions = new StreamPipeReaderOptions(
            bufferSize: options.RemoteBufferSize,
            minimumReadSize: 4096,
            leaveOpen: true);

        return new Peer(socket, stream, dataNode, readerOptions);
    }
    catch
    {
        socket.Dispose();
        throw;
    }
}
```

### 3.3 Helper: `ReadExactlyAsync`

For cases where we need an exact number of bytes (e.g., reading the 6-byte packet
length + header length prefix). The `PipeReader` API naturally handles this, but a
helper simplifies call sites.

```csharp
/// <summary>
/// Read exactly <paramref name="count"/> bytes from the pipe.
/// Blocks until all bytes are available or throws on premature EOF.
/// </summary>
public async ValueTask<ReadOnlySequence<byte>> ReadExactlyAsync(
    int count, CancellationToken ct = default)
{
    while (true)
    {
        var result = await Input.ReadAsync(ct);
        if (result.Buffer.Length >= count)
        {
            var slice = result.Buffer.Slice(0, count);
            // Don't advance yet -- caller will examine then advance
            Input.AdvanceTo(slice.Start, result.Buffer.GetPosition(count));
            return slice;
        }

        if (result.IsCompleted)
            throw new EndOfStreamException(
                $"DataNode {DataNode} closed connection. Expected {count} bytes, got {result.Buffer.Length}.");

        // Tell the pipe we've examined everything but consumed nothing -- need more
        Input.AdvanceTo(result.Buffer.Start, result.Buffer.End);
    }
}
```

**Note:** The actual read loop in `PacketReader` (Step 08) will use the standard
`PipeReader.ReadAsync` / `AdvanceTo` pattern directly for maximum efficiency. This
helper is for the handshake phase where we read small, known-size messages.

### 3.4 Helper: `WriteAsync` (Raw Bytes)

```csharp
/// <summary>
/// Write raw bytes to the output pipe and flush.
/// Used for version/opcode bytes and serialized protobuf messages.
/// </summary>
public async ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default)
{
    var result = await Output.WriteAsync(data, ct);
    if (result.IsCompleted)
        throw new IOException($"DataNode {DataNode} closed the write channel.");
}

/// <summary>
/// Flush any buffered output to the socket.
/// </summary>
public async ValueTask FlushAsync(CancellationToken ct = default)
{
    await Output.FlushAsync(ct);
}
```

### 3.5 `DisposeAsync`

```csharp
public async ValueTask DisposeAsync()
{
    if (_disposed) return;
    _disposed = true;

    try
    {
        await Input.CompleteAsync();
        await Output.CompleteAsync();
    }
    catch
    {
        // Best-effort cleanup
    }

    _stream.Dispose();

    try
    {
        _socket.Shutdown(SocketShutdown.Both);
    }
    catch
    {
        // Socket may already be disconnected
    }

    _socket.Dispose();
}
```

### 3.6 `GetOutputStream` -- For Proto Serialization

The `DataTransferSender` needs a `Stream` to write protobuf messages with length
prefixes. Expose the underlying `NetworkStream` for this:

```csharp
/// <summary>
/// Get the raw output stream for protobuf serialization.
/// Caller must NOT use Output (PipeWriter) concurrently with this.
/// </summary>
internal Stream GetOutputStream() => _stream;

/// <summary>
/// Get the raw input stream for protobuf deserialization (varint-prefixed reads).
/// Caller must NOT use Input (PipeReader) concurrently with this.
/// </summary>
internal Stream GetInputStream() => _stream;
```

**Important:** During the handshake phase (send request, read response), we use the raw
stream for protobuf serialization/deserialization. After the handshake, we switch to
`PipeReader` for the packet streaming phase. The two must not be used concurrently.

---

## Design Decisions

- **`PipeReader.Create(stream)` vs `IDuplexPipe`:** Using `PipeReader.Create` from a
  `NetworkStream` is simpler than managing a `Pipe` + fill loop. The `PipeReader`
  internally handles buffering and OS read calls. Sufficient for our read-heavy workload.
- **`NoDelay = true`:** Disable Nagle's algorithm since we send small protocol messages
  (request, status ack) that must be delivered immediately.
- **Separate handshake vs streaming phases:** The handshake uses protobuf
  `WriteDelimitedTo` / `ParseDelimitedFrom` on the raw stream. The packet streaming
  phase uses `PipeReader` for efficient binary parsing. This matches the Java
  implementation's separation between `DataOutputStream` (for the request) and the
  `ReadableByteChannel` (for packets).

---

## Acceptance Criteria

- [ ] `Peer.ConnectAsync` establishes a TCP connection to a given IP:port
- [ ] `Peer.Input` (PipeReader) can read data sent to the socket
- [ ] `Peer.Output` (PipeWriter) / `WriteAsync` can send data
- [ ] `DisposeAsync` cleanly shuts down the socket
- [ ] Connect timeout triggers `OperationCanceledException` after configured duration
- [ ] Premature socket close throws `EndOfStreamException` from `ReadExactlyAsync`
