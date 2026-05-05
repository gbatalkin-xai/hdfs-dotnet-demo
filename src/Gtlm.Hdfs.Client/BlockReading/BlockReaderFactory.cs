namespace Gtlm.Hdfs.Client.BlockReading;

using System.Net.Sockets;
using Gtlm.Hdfs.Client.Configuration;
using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Net;
using Gtlm.Hdfs.Client.Protocol;
using Gtlm.Hdfs.Client.Security;
using Microsoft.Extensions.Logging;

/// <summary>
/// Creates block readers. Manages Peer lifecycle (connect or reuse from cache).
/// </summary>
public sealed class BlockReaderFactory
{
    private readonly HdfsClientOptions _options;
    private readonly PeerCache _peerCache;
    private readonly KerberosCredentialProvider? _credentials;
    private readonly ILoggerFactory? _loggerFactory;
    private readonly DeadNodeTracker _deadNodes;

    public BlockReaderFactory(
        HdfsClientOptions options,
        PeerCache peerCache,
        ILoggerFactory? loggerFactory = null,
        KerberosCredentialProvider? credentials = null,
        DeadNodeTracker? deadNodeTracker = null)
    {
        _options = options;
        _peerCache = peerCache;
        _loggerFactory = loggerFactory;
        _credentials = credentials;
        _deadNodes = deadNodeTracker ?? new DeadNodeTracker(options.DeadNodeExpiry);
    }

    /// <summary>
    /// Create a block reader, trying short-circuit local reads first,
    /// then falling back to remote TCP reads.
    /// </summary>
    public async Task<IBlockReader> CreateReaderAsync(
        string file,
        LocatedBlock locatedBlock,
        DatanodeInfo dataNode,
        long offsetInBlock,
        long length,
        CancellationToken ct = default)
    {
        // Try short-circuit local read first
        var localReader = await LocalBlockReader.TryCreateAsync(
            locatedBlock.Block, dataNode, offsetInBlock, length,
            _options.VerifyChecksum, _options,
            _loggerFactory?.CreateLogger<LocalBlockReader>(), ct);

        if (localReader is not null)
            return localReader;

        // Fall back to remote read
        return await CreateRemoteReaderAsync(
            file, locatedBlock, dataNode, offsetInBlock, length, ct);
    }

    /// <summary>
    /// Create a remote block reader for the given block on the given DataNode.
    /// Reuses a cached peer if available, otherwise opens a new connection.
    /// </summary>
    public async Task<RemoteBlockReader> CreateRemoteReaderAsync(
        string file,
        LocatedBlock locatedBlock,
        DatanodeInfo dataNode,
        long offsetInBlock,
        long length,
        CancellationToken ct = default)
    {
        var peer = _peerCache.TryGet(dataNode)
            ?? await ConnectAndAuthenticateAsync(dataNode, ct);

        try
        {
            return await RemoteBlockReader.CreateAsync(
                file: file,
                block: locatedBlock.Block,
                token: locatedBlock.Token,
                startOffset: offsetInBlock,
                length: length,
                verifyChecksum: _options.VerifyChecksum,
                clientName: _options.ClientName,
                peer: peer,
                peerCache: _peerCache,
                options: _options,
                logger: _loggerFactory?.CreateLogger<RemoteBlockReader>(),
                ct: ct);
        }
        catch
        {
            await peer.DisposeAsync();
            throw;
        }
    }

    /// <summary>
    /// Create a block reader with automatic failover across replicas.
    /// Tries each DataNode location in order (deprioritizing dead nodes).
    /// </summary>
    public async Task<RemoteBlockReader> CreateRemoteReaderWithFailoverAsync(
        string file,
        LocatedBlock locatedBlock,
        long offsetInBlock,
        long length,
        CancellationToken ct = default)
    {
        var locations = _deadNodes.PrioritizeLocations(locatedBlock.Locations);
        int maxRetries = Math.Min(_options.MaxBlockReadRetries, locations.Count);
        var exceptions = new List<Exception>();

        for (int i = 0; i < maxRetries; i++)
        {
            var dataNode = locations[i];
            try
            {
                return await CreateRemoteReaderAsync(
                    file, locatedBlock, dataNode, offsetInBlock, length, ct);
            }
            catch (Exception ex) when (IsRetryable(ex))
            {
                _loggerFactory?.CreateLogger<BlockReaderFactory>()?.LogWarning(ex,
                    "Failed to read block {BlockId} from {DataNode} (attempt {Attempt}/{Max}), trying next replica",
                    locatedBlock.Block.BlockId, dataNode, i + 1, maxRetries);

                _deadNodes.MarkDead(dataNode);
                exceptions.Add(ex);
            }
        }

        throw new AggregateException(
            $"All {maxRetries} replicas failed for block {locatedBlock.Block.BlockId} of {file}",
            exceptions);
    }

    private static bool IsRetryable(Exception ex) =>
        ex is IOException or SocketException or ChecksumException or TimeoutException
            or OperationCanceledException;

    private async Task<Peer> ConnectAndAuthenticateAsync(
        DatanodeInfo dataNode, CancellationToken ct)
    {
        var peer = await Peer.ConnectAsync(dataNode, _options, ct);

        try
        {
            if (!string.IsNullOrEmpty(_options.DataTransferProtection) && _credentials is not null)
            {
                await SaslDataTransferHandler.NegotiateAsync(
                    peer, _credentials, dataNode,
                    _options.DataTransferProtection,
                    _loggerFactory?.CreateLogger("SaslDataTransferHandler"),
                    ct);
            }

            return peer;
        }
        catch
        {
            await peer.DisposeAsync();
            throw;
        }
    }
}
