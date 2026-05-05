# Step 17: HA NameNode, Metrics, Config Parsing, Benchmarks

**Phase:** 3 (Hardening)
**Prerequisites:** Phase 2 complete
**Produces:** Production-readiness features

---

## Objective

Final hardening steps to make the library production-ready: HA NameNode failover,
observability via `System.Diagnostics.Metrics`, Hadoop XML config file parsing,
and a performance benchmark suite.

---

## Part A: HA NameNode Failover

### A.1 Problem

Production HDFS clusters run two NameNodes (Active + Standby). The client must:
- Know both NameNode addresses
- Detect when the Active NameNode is unreachable or returns a StandbyException
- Automatically retry the request on the Standby (which may have become Active)

### A.2 Implementation

**File:** `src/Gtlm.Hdfs.Client/Rpc/HaNameNodeRpcClient.cs`

```csharp
namespace Gtlm.Hdfs.Client.Rpc;

public sealed class HaNameNodeRpcClient : IAsyncDisposable
{
    private readonly NameNodeRpcClient[] _clients;  // One per NameNode
    private int _activeIndex = 0;

    public HaNameNodeRpcClient(IReadOnlyList<(string host, int port)> nameNodes,
                                HdfsClientOptions options);

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
                _activeIndex = idx;  // Remember which NN is active
                return result;
            }
            catch (HdfsProtocolException ex) when (IsStandbyException(ex))
            {
                // This NN is standby, try the other
                continue;
            }
            catch (IOException)
            {
                // Connection failed, try the other
                continue;
            }
        }

        throw new IOException("All NameNodes are unreachable or in standby.");
    }
}
```

### A.3 Configuration

```csharp
/// <summary>List of NameNode addresses for HA mode.</summary>
public IReadOnlyList<(string Host, int Port)> NameNodes { get; set; } = [];
```

### A.4 Acceptance Criteria

- [ ] Automatic failover when Active NameNode goes down
- [ ] StandbyException triggers retry on the other NameNode
- [ ] Remembers which NameNode was last active (avoids unnecessary retries)
- [ ] Non-HA clusters continue to work with single NameNode config

---

## Part B: Observability via System.Diagnostics.Metrics

### B.1 Meter and Instruments

**File:** `src/Gtlm.Hdfs.Client/Diagnostics/HdfsMetrics.cs`

```csharp
namespace Gtlm.Hdfs.Client.Diagnostics;

using System.Diagnostics.Metrics;

public static class HdfsMetrics
{
    private static readonly Meter Meter = new("Gtlm.Hdfs.Client", "1.0");

    /// <summary>Total bytes read from DataNodes.</summary>
    public static readonly Counter<long> BytesRead =
        Meter.CreateCounter<long>("hdfs.client.bytes_read", "bytes");

    /// <summary>Total block read operations.</summary>
    public static readonly Counter<long> BlockReads =
        Meter.CreateCounter<long>("hdfs.client.block_reads");

    /// <summary>Block reads that failed and triggered failover.</summary>
    public static readonly Counter<long> BlockReadFailovers =
        Meter.CreateCounter<long>("hdfs.client.block_read_failovers");

    /// <summary>Checksum verification failures.</summary>
    public static readonly Counter<long> ChecksumErrors =
        Meter.CreateCounter<long>("hdfs.client.checksum_errors");

    /// <summary>Active connections in the peer cache.</summary>
    public static readonly UpDownCounter<int> CachedPeers =
        Meter.CreateUpDownCounter<int>("hdfs.client.cached_peers");

    /// <summary>Block read duration.</summary>
    public static readonly Histogram<double> BlockReadDuration =
        Meter.CreateHistogram<double>("hdfs.client.block_read_duration", "ms");

    /// <summary>NameNode RPC call duration.</summary>
    public static readonly Histogram<double> NamenodeRpcDuration =
        Meter.CreateHistogram<double>("hdfs.client.namenode_rpc_duration", "ms");
}
```

### B.2 Instrumentation Points

Add metric recording to:
- `RemoteBlockReader.ReadAsync` -- increment `BytesRead`
- `RemoteBlockReader.CreateAsync` -- increment `BlockReads`, record `BlockReadDuration`
- `PeerCache.Return` / `PeerCache.TryGet` -- update `CachedPeers`
- `BlockReaderFactory` failover logic -- increment `BlockReadFailovers`
- `DataChecksum.VerifyChunks` on failure -- increment `ChecksumErrors`
- `NameNodeRpcClient.CallAsync` -- record `NamenodeRpcDuration`

### B.3 Acceptance Criteria

- [ ] Metrics are emitted via `System.Diagnostics.Metrics` (compatible with
      OpenTelemetry, dotnet-counters, Prometheus exporters)
- [ ] All counters and histograms record correct values
- [ ] No performance overhead when no listener is attached

---

## Part C: Hadoop XML Configuration Parsing

### C.1 Problem

Existing Hadoop deployments have `core-site.xml` and `hdfs-site.xml` configuration
files. The library should be able to read these files and populate `HdfsClientOptions`.

### C.2 Implementation

**File:** `src/Gtlm.Hdfs.Client/Configuration/HadoopConfigParser.cs`

```csharp
namespace Gtlm.Hdfs.Client.Configuration;

/// <summary>
/// Parses Hadoop XML configuration files (core-site.xml, hdfs-site.xml).
/// </summary>
public static class HadoopConfigParser
{
    /// <summary>
    /// Parse a Hadoop configuration XML file into a dictionary.
    /// </summary>
    public static Dictionary<string, string> ParseFile(string path);

    /// <summary>
    /// Create HdfsClientOptions from Hadoop configuration files.
    /// </summary>
    public static HdfsClientOptions FromHadoopConfig(
        string? coreSitePath = null,
        string? hdfsSitePath = null)
    {
        var props = new Dictionary<string, string>();

        if (coreSitePath is not null)
            foreach (var (k, v) in ParseFile(coreSitePath))
                props[k] = v;

        if (hdfsSitePath is not null)
            foreach (var (k, v) in ParseFile(hdfsSitePath))
                props[k] = v;

        return new HdfsClientOptions
        {
            NameNodeHost = ExtractNameNodeHost(props),
            NameNodePort = ExtractNameNodePort(props),
            VerifyChecksum = GetBool(props, "dfs.client.read.checksum", true),
            RemoteBufferSize = GetInt(props,
                "dfs.client.block.reader.remote.buffer.size", 512 * 1024),
            DataTransferProtection = GetString(props,
                "dfs.data.transfer.protection", ""),
            EncryptDataTransfer = GetBool(props,
                "dfs.encrypt.data.transfer", false),
        };
    }
}
```

Hadoop XML format:

```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://namenode:8020</value>
  </property>
</configuration>
```

Parse using `System.Xml.Linq.XDocument` (in-box).

### C.3 Acceptance Criteria

- [ ] Parses standard Hadoop XML config files
- [ ] Extracts NameNode address from `fs.defaultFS`
- [ ] Handles HA nameservice references (`dfs.nameservices`, `dfs.ha.namenodes.*`)
- [ ] Unknown properties are silently ignored
- [ ] Missing files handled gracefully (optional parameter)

---

## Part D: BenchmarkDotNet Performance Suite

### D.1 Benchmark Project

```bash
dotnet new console -n Gtlm.Hdfs.Client.Benchmarks -f net10.0 \
  -o benchmarks/Gtlm.Hdfs.Client.Benchmarks
dotnet add benchmarks/Gtlm.Hdfs.Client.Benchmarks package BenchmarkDotNet
```

### D.2 Benchmarks

**File:** `benchmarks/Gtlm.Hdfs.Client.Benchmarks/BlockReaderBenchmarks.cs`

```csharp
[MemoryDiagnoser]
[SimpleJob(RuntimeMoniker.Net100)]
public class BlockReaderBenchmarks
{
    [Benchmark]
    public async Task ReadBlock_1MB_WithChecksum()
    {
        // Read a 1 MB block from a mock DataNode server
    }

    [Benchmark]
    public async Task ReadBlock_1MB_NoChecksum()
    {
        // Same but with verifyChecksum=false
    }

    [Benchmark]
    public void Crc32C_512Bytes()
    {
        // Checksum computation throughput
    }

    [Benchmark]
    public void Crc32C_64KB()
    {
        // Checksum computation throughput for large buffer
    }

    [Benchmark]
    public void PacketParsing_1024BytePacket()
    {
        // Parse a pre-built packet from a byte array
    }
}
```

### D.3 Mock DataNode Server for Benchmarks

A simple TCP server that speaks the data transfer protocol, serving pre-generated
block data. This allows benchmarks to run without a real Hadoop cluster.

### D.4 Acceptance Criteria

- [ ] Benchmarks run via `dotnet run -c Release`
- [ ] Reports throughput (MB/s), allocations, and latency
- [ ] CRC32C benchmark shows hardware acceleration is active
- [ ] No regressions when compared to baseline numbers
- [ ] Results can be exported for tracking over time

---

## Summary: Phase 3 Deliverables

| Component | Status | Validates |
|-----------|--------|-----------|
| HA NameNode | Must-have | Production cluster support |
| Metrics | Must-have | Observability |
| Config parsing | Nice-to-have | Drop-in compatibility |
| Benchmarks | Must-have | Performance confidence |
