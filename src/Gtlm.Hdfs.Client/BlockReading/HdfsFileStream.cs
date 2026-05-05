namespace Gtlm.Hdfs.Client.BlockReading;

using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Rpc;
using Microsoft.Extensions.Logging;

/// <summary>
/// A Stream that reads an HDFS file transparently, resolving paths to blocks
/// and chaining RemoteBlockReader instances across block boundaries.
/// </summary>
public sealed class HdfsFileStream : Stream, IAsyncDisposable
{
    private readonly string _path;
    private readonly IReadOnlyList<LocatedBlock> _blocks;
    private readonly BlockReaderFactory _readerFactory;
    private readonly long _fileLength;
    private readonly ILogger? _logger;

    private int _currentBlockIndex;
    private RemoteBlockReader? _currentReader;
    private long _position;
    private bool _disposed;

    private HdfsFileStream(
        string path,
        IReadOnlyList<LocatedBlock> blocks,
        long fileLength,
        BlockReaderFactory readerFactory,
        ILogger? logger)
    {
        _path = path;
        _blocks = blocks;
        _fileLength = fileLength;
        _readerFactory = readerFactory;
        _logger = logger;
    }

    /// <summary>
    /// Open an HDFS file for reading.
    /// Resolves block locations from the NameNode.
    /// </summary>
    public static async Task<HdfsFileStream> OpenAsync(
        string path,
        NameNodeRpcClient nameNode,
        BlockReaderFactory readerFactory,
        ILogger? logger = null,
        CancellationToken ct = default)
    {
        var fileStatus = await nameNode.GetFileInfoAsync(path, ct)
            ?? throw new FileNotFoundException($"HDFS file not found: {path}");

        if (fileStatus.IsDirectory)
            throw new IOException($"Cannot read directory as file: {path}");

        var blocks = await nameNode.GetBlockLocationsAsync(path, 0, fileStatus.Length, ct);

        if (blocks.Count == 0 && fileStatus.Length > 0)
            throw new IOException($"No block locations returned for non-empty file: {path}");

        logger?.LogDebug("Opened {Path}: {Length} bytes, {Blocks} blocks",
            path, fileStatus.Length, blocks.Count);

        return new HdfsFileStream(path, blocks, fileStatus.Length, readerFactory, logger);
    }

    /// <summary>
    /// Create an HdfsFileStream from pre-resolved blocks (for testing or
    /// when block locations are already known).
    /// </summary>
    internal static HdfsFileStream CreateFromBlocks(
        string path,
        IReadOnlyList<LocatedBlock> blocks,
        long fileLength,
        BlockReaderFactory readerFactory)
    {
        return new HdfsFileStream(path, blocks, fileLength, readerFactory, logger: null);
    }

    public override async ValueTask<int> ReadAsync(
        Memory<byte> buffer, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_position >= _fileLength)
            return 0;

        if (_currentReader is null || _currentReader.IsComplete)
        {
            if (!await AdvanceToNextBlockAsync(ct))
                return 0;
        }

        int bytesRead = await _currentReader!.ReadAsync(buffer, ct);

        if (bytesRead == 0 && !_currentReader.IsComplete)
        {
            if (!await AdvanceToNextBlockAsync(ct))
                return 0;
            bytesRead = await _currentReader!.ReadAsync(buffer, ct);
        }

        _position += bytesRead;
        return bytesRead;
    }

    private async ValueTask<bool> AdvanceToNextBlockAsync(CancellationToken ct)
    {
        if (_currentReader is not null)
        {
            await _currentReader.DisposeAsync();
            _currentReader = null;
            _currentBlockIndex++;
        }

        if (_currentBlockIndex >= _blocks.Count)
            return false;

        var block = _blocks[_currentBlockIndex];
        long offsetInBlock = _position - block.Offset;
        long remainingInBlock = block.Block.NumBytes - offsetInBlock;

        if (remainingInBlock <= 0)
        {
            _currentBlockIndex++;
            if (_currentBlockIndex >= _blocks.Count)
                return false;
            block = _blocks[_currentBlockIndex];
            offsetInBlock = 0;
            remainingInBlock = block.Block.NumBytes;
        }

        var dataNode = block.Locations[0];

        _logger?.LogDebug("Opening block {Index}/{Total} (id={BlockId}) on {DN}, offset={Offset}, len={Len}",
            _currentBlockIndex + 1, _blocks.Count, block.Block.BlockId,
            dataNode, offsetInBlock, remainingInBlock);

        _currentReader = await _readerFactory.CreateRemoteReaderAsync(
            _path, block, dataNode, offsetInBlock, remainingInBlock, ct);

        return true;
    }

    public override async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        if (_currentReader is not null)
        {
            await _currentReader.DisposeAsync();
            _currentReader = null;
        }
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
    public override long Length => _fileLength;
    public override long Position
    {
        get => _position;
        set => throw new NotSupportedException();
    }

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
