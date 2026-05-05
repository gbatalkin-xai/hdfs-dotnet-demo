namespace Gtlm.Hdfs.Client.Diagnostics;

using System.Diagnostics.Metrics;

/// <summary>
/// HDFS client metrics exposed via System.Diagnostics.Metrics.
/// Compatible with OpenTelemetry, dotnet-counters, Prometheus exporters, etc.
/// </summary>
public static class HdfsMetrics
{
    public static readonly Meter Meter = new("Gtlm.Hdfs.Client", "1.0");

    /// <summary>Total bytes read from DataNodes.</summary>
    public static readonly Counter<long> BytesRead =
        Meter.CreateCounter<long>("hdfs.client.bytes_read", "bytes",
            "Total bytes read from HDFS DataNodes");

    /// <summary>Total block read operations initiated.</summary>
    public static readonly Counter<long> BlockReads =
        Meter.CreateCounter<long>("hdfs.client.block_reads", description:
            "Total block read operations");

    /// <summary>Block reads that failed and triggered replica failover.</summary>
    public static readonly Counter<long> BlockReadFailovers =
        Meter.CreateCounter<long>("hdfs.client.block_read_failovers", description:
            "Block reads that failed over to another replica");

    /// <summary>Checksum verification failures.</summary>
    public static readonly Counter<long> ChecksumErrors =
        Meter.CreateCounter<long>("hdfs.client.checksum_errors", description:
            "Checksum verification failures");

    /// <summary>Active cached peer connections.</summary>
    public static readonly UpDownCounter<int> CachedPeers =
        Meter.CreateUpDownCounter<int>("hdfs.client.cached_peers", description:
            "Active connections in the peer cache");

    /// <summary>Block read duration in milliseconds.</summary>
    public static readonly Histogram<double> BlockReadDuration =
        Meter.CreateHistogram<double>("hdfs.client.block_read_duration", "ms",
            "Time to read a complete block");

    /// <summary>NameNode RPC call duration in milliseconds.</summary>
    public static readonly Histogram<double> NamenodeRpcDuration =
        Meter.CreateHistogram<double>("hdfs.client.namenode_rpc_duration", "ms",
            "NameNode RPC call latency");

    /// <summary>Peer connections opened.</summary>
    public static readonly Counter<long> PeerConnections =
        Meter.CreateCounter<long>("hdfs.client.peer_connections", description:
            "TCP connections opened to DataNodes");

    /// <summary>Peer connections reused from cache.</summary>
    public static readonly Counter<long> PeerCacheHits =
        Meter.CreateCounter<long>("hdfs.client.peer_cache_hits", description:
            "Peer connections reused from cache");
}
