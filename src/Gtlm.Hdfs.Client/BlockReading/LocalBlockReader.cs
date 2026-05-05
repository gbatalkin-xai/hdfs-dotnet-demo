namespace Gtlm.Hdfs.Client.BlockReading;

using System.Net.Sockets;
using Gtlm.Hdfs.Client.Checksum;
using Gtlm.Hdfs.Client.Configuration;
using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Protocol;
using Microsoft.Extensions.Logging;

/// <summary>
/// Reads HDFS block data from the local filesystem using short-circuit access.
/// Requires the client to be co-located with the DataNode.
///
/// Short-circuit reads bypass TCP by requesting file descriptors from the DataNode
/// via a Unix domain socket, then reading block data and checksums directly from
/// the local filesystem.
///
/// This implementation handles the domain socket protocol. The actual FD passing
/// (SCM_RIGHTS) requires platform-specific P/Invoke on Linux.
/// </summary>
public sealed class LocalBlockReader : Stream, IBlockReader
{
    private readonly Stream _dataStream;
    private readonly DataChecksum? _checksum;
    private readonly bool _verifyChecksum;
    private readonly long _bytesToRead;
    private long _bytesRead;
    private bool _disposed;

    private LocalBlockReader(
        Stream dataStream,
        DataChecksum? checksum,
        bool verifyChecksum,
        long bytesToRead)
    {
        _dataStream = dataStream;
        _checksum = checksum;
        _verifyChecksum = verifyChecksum;
        _bytesToRead = bytesToRead;
    }

    /// <summary>
    /// Try to create a LocalBlockReader for the given block.
    /// Returns null if short-circuit reads are not available (wrong platform,
    /// domain socket not configured, DataNode is remote, etc.).
    /// </summary>
    public static Task<LocalBlockReader?> TryCreateAsync(
        ExtendedBlock block,
        DatanodeInfo dataNode,
        long startOffset,
        long length,
        bool verifyChecksum,
        HdfsClientOptions options,
        ILogger? logger = null,
        CancellationToken ct = default)
    {
        // Short-circuit reads require:
        // 1. Linux or macOS (Unix domain sockets)
        // 2. A configured domain socket path
        // 3. Client co-located with the DataNode

        if (!OperatingSystem.IsLinux() && !OperatingSystem.IsMacOS())
        {
            logger?.LogDebug("Short-circuit reads not supported on this platform");
            return Task.FromResult<LocalBlockReader?>(null);
        }

        if (string.IsNullOrEmpty(options.DomainSocketPath))
        {
            logger?.LogDebug("No domain socket path configured, skipping short-circuit");
            return Task.FromResult<LocalBlockReader?>(null);
        }

        if (!IsLocalDataNode(dataNode))
        {
            logger?.LogDebug("DataNode {DN} is not local, skipping short-circuit", dataNode);
            return Task.FromResult<LocalBlockReader?>(null);
        }

        // TODO: Implement the full short-circuit protocol:
        // 1. Connect to domain socket at options.DomainSocketPath
        // 2. Send OpRequestShortCircuitAccessProto with block ID
        // 3. Receive file descriptors via SCM_RIGHTS (P/Invoke recvmsg)
        // 4. Open FileStream from the received FDs
        // 5. Seek to startOffset and construct LocalBlockReader

        logger?.LogDebug(
            "Short-circuit reads not yet fully implemented for block {BlockId} on {DN}",
            block.BlockId, dataNode);

        return Task.FromResult<LocalBlockReader?>(null);
    }

    /// <summary>
    /// Create a LocalBlockReader from an already-opened data stream.
    /// Used for testing or when block file paths are known.
    /// </summary>
    internal static LocalBlockReader CreateFromStream(
        Stream dataStream, long length, bool verifyChecksum = false)
    {
        return new LocalBlockReader(dataStream, checksum: null, verifyChecksum, length);
    }

    public override async ValueTask<int> ReadAsync(
        Memory<byte> buffer, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_bytesRead >= _bytesToRead)
            return 0;

        int toRead = (int)Math.Min(buffer.Length, _bytesToRead - _bytesRead);
        int bytesRead = await _dataStream.ReadAsync(buffer[..toRead], ct);

        _bytesRead += bytesRead;
        return bytesRead;
    }

    /// <summary>
    /// Check if a DataNode is running on the local machine.
    /// Compares the DataNode's IP against local network interfaces.
    /// </summary>
    internal static bool IsLocalDataNode(DatanodeInfo dataNode)
    {
        if (dataNode.IpAddress is "127.0.0.1" or "::1" or "localhost")
            return true;

        try
        {
            var hostName = System.Net.Dns.GetHostName();
            var hostAddresses = System.Net.Dns.GetHostAddresses(hostName);
            return hostAddresses.Any(addr => addr.ToString() == dataNode.IpAddress);
        }
        catch
        {
            return false;
        }
    }

    public override async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        await _dataStream.DisposeAsync();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing && !_disposed)
            DisposeAsync().AsTask().GetAwaiter().GetResult();
        base.Dispose(disposing);
    }

    // --- Stream properties ---
    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => _bytesToRead;
    public override long Position
    {
        get => _bytesRead;
        set => throw new NotSupportedException();
    }
    public bool IsComplete => _bytesRead >= _bytesToRead;

    public override void Flush() { }
    public override int Read(byte[] buffer, int offset, int count) =>
        ReadAsync(buffer.AsMemory(offset, count)).AsTask().GetAwaiter().GetResult();
    public override long Seek(long offset, SeekOrigin origin) =>
        throw new NotSupportedException();
    public override void SetLength(long value) =>
        throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) =>
        throw new NotSupportedException();
}
