namespace Gtlm.Hdfs.Client.BlockReading;

using System.Buffers;
using Gtlm.Hdfs.Client.Checksum;
using Gtlm.Hdfs.Client.Configuration;
using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Net;
using Gtlm.Hdfs.Client.Protocol;
using Gtlm.Hdfs.Client.Proto;
using Microsoft.Extensions.Logging;

/// <summary>
/// Reads a single HDFS block from a remote DataNode via the Data Transfer Protocol.
/// Implements Stream for consumer compatibility.
///
/// Equivalent to Java's org.apache.hadoop.hdfs.client.impl.BlockReaderRemote.
/// </summary>
public sealed class RemoteBlockReader : Stream, IBlockReader
{
    private readonly Peer _peer;
    private readonly DataChecksum _checksum;
    private readonly bool _verifyChecksum;
    private readonly long _bytesToRead;
    private readonly string _filename;
    private readonly long _blockId;
    private readonly PeerCache? _peerCache;
    private readonly PacketReader _packetReader;
    private readonly ILogger? _logger;

    private long _bytesRead;
    private byte[]? _currentPacketBuf;
    private int _packetDataOffset;
    private int _packetDataRemaining;
    private bool _lastPacketReceived;
    private bool _disposed;
    private int _skipBytesInFirstPacket;

    private RemoteBlockReader(
        Peer peer,
        DataChecksum checksum,
        bool verifyChecksum,
        long bytesToRead,
        string filename,
        long blockId,
        PeerCache? peerCache,
        int skipBytesInFirstPacket,
        ILogger? logger)
    {
        _peer = peer;
        _checksum = checksum;
        _verifyChecksum = verifyChecksum;
        _bytesToRead = bytesToRead;
        _filename = filename;
        _blockId = blockId;
        _peerCache = peerCache;
        _skipBytesInFirstPacket = skipBytesInFirstPacket;
        _logger = logger;

        _packetReader = new PacketReader(peer.Input, checksum);
    }

    /// <summary>
    /// Create a new RemoteBlockReader and perform the OP_READ_BLOCK handshake.
    /// On success, the reader is ready to stream data via ReadAsync.
    /// </summary>
    public static async Task<RemoteBlockReader> CreateAsync(
        string file,
        ExtendedBlock block,
        BlockToken token,
        long startOffset,
        long length,
        bool verifyChecksum,
        string clientName,
        Peer peer,
        PeerCache? peerCache,
        HdfsClientOptions options,
        ILogger? logger = null,
        CancellationToken ct = default)
    {
        // 1. Send OP_READ_BLOCK request
        await DataTransferSender.SendReadBlockAsync(
            peer, block, token, clientName, startOffset, length,
            sendChecksums: verifyChecksum, ct);

        // 2. Receive and parse response
        var response = DataTransferReceiver.ReceiveBlockOpResponse(peer);

        // 3. Validate status and extract checksum info
        var checksumInfo = DataTransferReceiver.ValidateAndExtractChecksumInfo(
            response, peer.DataNode, block, file);

        // 4. Validate chunk alignment
        DataTransferReceiver.ValidateChunkOffset(
            checksumInfo.ChunkOffset, startOffset, checksumInfo.BytesPerChecksum, file);

        // 5. Create checksum verifier
        var checksum = DataChecksum.Create(checksumInfo.ChecksumType, checksumInfo.BytesPerChecksum);

        // 6. Calculate bytes to skip in first packet (chunk alignment)
        int skipBytes = (int)(startOffset - checksumInfo.ChunkOffset);

        logger?.LogDebug(
            "Created RemoteBlockReader for {File} block {BlockId} at offset {Offset}, " +
            "length {Length}, checksum {Type}/{BPC}, skip {Skip}",
            file, block.BlockId, startOffset, length,
            checksumInfo.ChecksumType, checksumInfo.BytesPerChecksum, skipBytes);

        return new RemoteBlockReader(
            peer, checksum, verifyChecksum, length, file,
            block.BlockId, peerCache, skipBytes, logger);
    }

    // --- Stream ReadAsync ---

    public override async ValueTask<int> ReadAsync(
        Memory<byte> buffer, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_bytesRead >= _bytesToRead)
            return 0;

        if (_packetDataRemaining == 0)
        {
            if (_lastPacketReceived)
                return 0;

            await ReadNextPacketInternalAsync(ct);

            if (_packetDataRemaining == 0)
                return 0;
        }

        int toCopy = Math.Min(buffer.Length, _packetDataRemaining);
        toCopy = (int)Math.Min(toCopy, _bytesToRead - _bytesRead);

        if (toCopy == 0)
            return 0;

        new ReadOnlySpan<byte>(_currentPacketBuf, _packetDataOffset, toCopy)
            .CopyTo(buffer.Span);

        _packetDataOffset += toCopy;
        _packetDataRemaining -= toCopy;
        _bytesRead += toCopy;

        return toCopy;
    }

    private async ValueTask ReadNextPacketInternalAsync(CancellationToken ct)
    {
        ReturnCurrentPacketBuffer();

        PacketData packet;
        do
        {
            packet = await _packetReader.ReadNextPacketAsync(ct);
        } while (packet.SeqNo == -1); // Skip heartbeat packets

        _lastPacketReceived = packet.IsLastPacket;

        if (packet.DataLength == 0)
        {
            _packetDataRemaining = 0;
            return;
        }

        // Verify checksums before exposing data to caller
        if (_verifyChecksum && _checksum.ChecksumSize > 0)
        {
            _checksum.VerifyChunks(
                packet.Data.Span,
                packet.Checksums.Span,
                packet.OffsetInBlock);
        }

        // Handle first-packet alignment skip
        int dataOffset = 0;
        int dataLength = packet.DataLength;

        if (_skipBytesInFirstPacket > 0)
        {
            dataOffset = _skipBytesInFirstPacket;
            dataLength -= _skipBytesInFirstPacket;
            _skipBytesInFirstPacket = 0;
        }

        // Rent buffer and copy usable data
        _currentPacketBuf = ArrayPool<byte>.Shared.Rent(dataLength);
        packet.Data.Span.Slice(dataOffset, dataLength).CopyTo(_currentPacketBuf);
        _packetDataOffset = 0;
        _packetDataRemaining = dataLength;
    }

    private void ReturnCurrentPacketBuffer()
    {
        if (_currentPacketBuf is not null)
        {
            ArrayPool<byte>.Shared.Return(_currentPacketBuf);
            _currentPacketBuf = null;
        }
    }

    // --- Dispose ---

    public override async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        ReturnCurrentPacketBuffer();

        try
        {
            var status = _bytesRead >= _bytesToRead || _lastPacketReceived
                ? Status.ChecksumOk
                : Status.Error;

            await DataTransferSender.SendClientReadStatusAsync(_peer, status);
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to send final read status to DataNode {DN}", _peer.DataNode);
        }

        bool cleanClose = (_bytesRead >= _bytesToRead || _lastPacketReceived) && !_peer.IsClosed;
        if (cleanClose && _peerCache is not null)
        {
            _peerCache.Return(_peer);
        }
        else
        {
            await _peer.DisposeAsync();
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing && !_disposed)
        {
            DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
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

    // --- Unsupported ---

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
