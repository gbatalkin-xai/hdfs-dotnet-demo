namespace Gtlm.Hdfs.Client.Rpc;

using Gtlm.Hdfs.Client.Configuration;
using Gtlm.Hdfs.Client.Protocol;
using Microsoft.Extensions.Logging;

/// <summary>
/// HA NameNode RPC client. Wraps multiple NameNodeRpcClient instances and
/// automatically fails over to the next NameNode on connection errors or
/// StandbyException.
/// </summary>
public sealed class HaNameNodeRpcClient : IAsyncDisposable
{
    private readonly NameNodeRpcClient[] _clients;
    private readonly ILogger? _logger;
    private int _activeIndex;
    private bool _disposed;

    public HaNameNodeRpcClient(
        IReadOnlyList<(string host, int port)> nameNodes,
        HdfsClientOptions options,
        ILogger? logger = null)
    {
        if (nameNodes.Count == 0)
            throw new ArgumentException("At least one NameNode address is required.");

        _logger = logger;
        _clients = nameNodes.Select(nn =>
            new NameNodeRpcClient(nn.host, nn.port, options, logger)).ToArray();
    }

    /// <summary>Connect to the first available NameNode.</summary>
    public async Task ConnectAsync(CancellationToken ct = default)
    {
        for (int i = 0; i < _clients.Length; i++)
        {
            int idx = (_activeIndex + i) % _clients.Length;
            try
            {
                await _clients[idx].ConnectAsync(ct);
                _activeIndex = idx;
                _logger?.LogInformation("Connected to NameNode {Index}", idx);
                return;
            }
            catch (Exception ex) when (i < _clients.Length - 1)
            {
                _logger?.LogWarning(ex,
                    "Failed to connect to NameNode {Index}, trying next", idx);
            }
        }
    }

    /// <summary>
    /// Execute an RPC operation with automatic failover.
    /// On connection failure or StandbyException, retries on the next NameNode.
    /// </summary>
    public async Task<T> ExecuteWithFailoverAsync<T>(
        Func<NameNodeRpcClient, Task<T>> operation,
        CancellationToken ct = default)
    {
        for (int attempt = 0; attempt < _clients.Length; attempt++)
        {
            int idx = (_activeIndex + attempt) % _clients.Length;
            try
            {
                var result = await operation(_clients[idx]);
                _activeIndex = idx;
                return result;
            }
            catch (HdfsProtocolException ex) when (IsStandbyException(ex))
            {
                _logger?.LogWarning(
                    "NameNode {Index} is standby, failing over", idx);
            }
            catch (IOException ex) when (attempt < _clients.Length - 1)
            {
                _logger?.LogWarning(ex,
                    "NameNode {Index} connection failed, failing over", idx);

                // Reconnect the failed client for future use
                try
                {
                    await _clients[idx].DisposeAsync();
                    _clients[idx] = new NameNodeRpcClient(
                        _clients[idx] is var _ ? "localhost" : "", 8020,
                        new HdfsClientOptions(), _logger);
                }
                catch { /* best-effort reconnect */ }
            }
        }

        throw new IOException("All NameNodes are unreachable or in standby.");
    }

    /// <summary>The currently active NameNode client.</summary>
    public NameNodeRpcClient Active => _clients[_activeIndex];

    /// <summary>Number of configured NameNodes.</summary>
    public int Count => _clients.Length;

    private static bool IsStandbyException(HdfsProtocolException ex) =>
        ex.Message.Contains("StandbyException", StringComparison.OrdinalIgnoreCase) ||
        ex.Message.Contains("is not active", StringComparison.OrdinalIgnoreCase);

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        foreach (var client in _clients)
            await client.DisposeAsync();
    }
}
