namespace Gtlm.Hdfs.Client;

using Gtlm.Hdfs.Client.BlockReading;
using Gtlm.Hdfs.Client.Configuration;
using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Net;
using Gtlm.Hdfs.Client.Rpc;
using Microsoft.Extensions.Logging;

/// <summary>
/// High-level HDFS client. Opens files for reading.
///
/// Usage:
///   await using var client = new HdfsClient(options);
///   await client.ConnectAsync();
///   await using var stream = await client.OpenReadAsync("/data/file.parquet");
///   await stream.CopyToAsync(localFile);
/// </summary>
public sealed class HdfsClient : IAsyncDisposable
{
    private readonly HdfsClientOptions _options;
    private readonly ILoggerFactory? _loggerFactory;
    private readonly ILogger? _logger;
    private NameNodeRpcClient? _nameNode;
    private PeerCache? _peerCache;
    private BlockReaderFactory? _readerFactory;
    private bool _disposed;

    public HdfsClient(HdfsClientOptions options, ILoggerFactory? loggerFactory = null)
    {
        _options = options;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory?.CreateLogger<HdfsClient>();
    }

    /// <summary>Connect to the NameNode.</summary>
    public async Task ConnectAsync(CancellationToken ct = default)
    {
        _peerCache = new PeerCache(
            _options.PeerCacheMaxPerDataNode,
            _options.PeerCacheIdleTimeout,
            _loggerFactory?.CreateLogger<PeerCache>());

        _readerFactory = new BlockReaderFactory(_options, _peerCache, _loggerFactory);

        _nameNode = new NameNodeRpcClient(_options,
            _loggerFactory?.CreateLogger<NameNodeRpcClient>());
        await _nameNode.ConnectAsync(ct);

        _logger?.LogInformation("Connected to HDFS at {Host}:{Port}",
            _options.NameNodeHost, _options.NameNodePort);
    }

    /// <summary>Open a file for reading. Returns a Stream.</summary>
    public async Task<Stream> OpenReadAsync(string path, CancellationToken ct = default)
    {
        EnsureConnected();
        return await HdfsFileStream.OpenAsync(
            path, _nameNode!, _readerFactory!,
            _loggerFactory?.CreateLogger<HdfsFileStream>(), ct);
    }

    /// <summary>Get file metadata.</summary>
    public async Task<HdfsFileStatus?> GetFileInfoAsync(
        string path, CancellationToken ct = default)
    {
        EnsureConnected();
        return await _nameNode!.GetFileInfoAsync(path, ct);
    }

    /// <summary>List directory contents.</summary>
    public async Task<IReadOnlyList<HdfsFileStatus>> ListDirectoryAsync(
        string path, CancellationToken ct = default)
    {
        EnsureConnected();
        return await _nameNode!.GetListingAsync(path, ct);
    }

    private void EnsureConnected()
    {
        if (_nameNode is null)
            throw new InvalidOperationException("Not connected. Call ConnectAsync first.");
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        if (_nameNode is not null)
            await _nameNode.DisposeAsync();
        if (_peerCache is not null)
            await _peerCache.DisposeAsync();
    }
}
