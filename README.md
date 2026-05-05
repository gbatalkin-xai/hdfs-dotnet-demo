# Gtlm.Hdfs.Client

A native .NET 10 client for reading files from Apache HDFS. Communicates directly with
NameNodes and DataNodes using the Hadoop IPC and Data Transfer protocols -- no JVM, no
WebHDFS, no REST overhead.

Built for high-throughput sequential reads in data pipelines where every byte flows
through the native HDFS binary protocol with hardware-accelerated CRC32C checksum
verification.

## Features

- **Native binary protocol** -- Reads blocks directly from DataNodes over TCP (port 9866),
  matching the wire format of Java's `BlockReaderRemote`
- **NameNode RPC** -- Resolves file paths to block locations via the Hadoop IPC protocol
- **Zero-copy I/O** -- `System.IO.Pipelines` for packet parsing, `ArrayPool<byte>` for
  buffer management, `Span<T>` throughout
- **Hardware-accelerated checksums** -- CRC32C via SSE4.2 (x64) / ARM CRC32 intrinsics
- **Connection pooling** -- Reuses TCP connections to DataNodes across block reads
- **Replica failover** -- Automatically retries with alternate DataNodes on read errors
- **HA NameNode** -- Transparent failover between Active and Standby NameNodes
- **Kerberos/SASL** -- Authentication for secure clusters via `Kerberos.NET`
- **Standard Stream API** -- Returns a `System.IO.Stream` that works with `CopyToAsync`,
  `StreamReader`, `JsonSerializer`, or any stream consumer
- **Hadoop config compatibility** -- Reads `core-site.xml` and `hdfs-site.xml` directly

## Quick Start

### Read a file from HDFS

```csharp
using Gtlm.Hdfs.Client;
using Gtlm.Hdfs.Client.Configuration;

var options = new HdfsClientOptions
{
    NameNodeHost = "namenode.cluster.local",
    NameNodePort = 8020,
};

await using var client = new HdfsClient(options);
await client.ConnectAsync();

// Read a file as a Stream
await using var hdfsStream = await client.OpenReadAsync("/data/input.parquet");
await using var localFile = File.Create("/tmp/output.parquet");
await hdfsStream.CopyToAsync(localFile);
```

### Read from an existing Hadoop configuration

If you have `core-site.xml` and `hdfs-site.xml` from your cluster:

```csharp
using Gtlm.Hdfs.Client;
using Gtlm.Hdfs.Client.Configuration;

var options = HadoopConfigParser.FromHadoopConfig(
    coreSitePath: "/etc/hadoop/conf/core-site.xml",
    hdfsSitePath: "/etc/hadoop/conf/hdfs-site.xml");

await using var client = new HdfsClient(options);
await client.ConnectAsync();

// Check if a file exists
var info = await client.GetFileInfoAsync("/data/input.csv");
if (info is not null)
    Console.WriteLine($"{info.Path}: {info.Length} bytes, owner={info.Owner}");

// List a directory
var listing = await client.ListDirectoryAsync("/data/");
foreach (var entry in listing)
    Console.WriteLine($"  {(entry.IsDirectory ? "d" : "-")} {entry.Path} ({entry.Length} bytes)");

// Stream a file
await using var stream = await client.OpenReadAsync("/data/input.csv");
using var reader = new StreamReader(stream);
while (await reader.ReadLineAsync() is { } line)
    ProcessLine(line);
```

### HA NameNode

For clusters with two NameNodes (Active + Standby):

```csharp
using Gtlm.Hdfs.Client.Configuration;
using Gtlm.Hdfs.Client.Rpc;

// Option 1: Parse from hdfs-site.xml (reads dfs.nameservices, dfs.ha.namenodes.*)
var props = HadoopConfigParser.ParseFile("/etc/hadoop/conf/hdfs-site.xml");
var nameNodes = HadoopConfigParser.ExtractHaNameNodes(props);

// Option 2: Specify manually
var nameNodes = new List<(string host, int port)>
{
    ("namenode1.cluster.local", 8020),
    ("namenode2.cluster.local", 8020),
};

var options = new HdfsClientOptions();
await using var ha = new HaNameNodeRpcClient(nameNodes, options);
await ha.ConnectAsync();

// Operations automatically fail over on StandbyException
var blocks = await ha.ExecuteWithFailoverAsync(
    nn => nn.GetBlockLocationsAsync("/data/file.dat", 0, long.MaxValue));
```

### Secure clusters (Kerberos)

```csharp
var options = new HdfsClientOptions
{
    NameNodeHost = "namenode.cluster.local",
    NameNodePort = 8020,
    KerberosPrincipal = "myuser@REALM.COM",
    KeytabPath = "/etc/security/keytabs/myuser.keytab",
    DataTransferProtection = "authentication", // or "integrity", "privacy"
};

await using var client = new HdfsClient(options);
await client.ConnectAsync();
```

### Low-level: Read a single block directly

If you already know the block location (e.g., from a job scheduler):

```csharp
using Gtlm.Hdfs.Client.BlockReading;
using Gtlm.Hdfs.Client.Configuration;
using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Net;

var dataNode = new DatanodeInfo
{
    IpAddress = "10.0.1.5",
    HostName = "datanode5.cluster.local",
    DatanodeUuid = "uuid-from-namenode",
    XferPort = 9866,
};

var block = new ExtendedBlock(
    PoolId: "BP-123456789-10.0.1.1-1700000000000",
    BlockId: 1073742345,
    GenerationStamp: 1234,
    NumBytes: 128 * 1024 * 1024);

var options = new HdfsClientOptions();
var peer = await Peer.ConnectAsync(dataNode, options);

await using var reader = await RemoteBlockReader.CreateAsync(
    file: "/data/file.dat",
    block: block,
    token: BlockToken.Empty,  // non-secure cluster
    startOffset: 0,
    length: block.NumBytes,
    verifyChecksum: true,
    clientName: "my-app",
    peer: peer,
    peerCache: null,
    options: options);

// reader is a standard Stream
await using var output = File.Create("/tmp/block.dat");
await reader.CopyToAsync(output);
```

## Configuration Reference

| Property | Default | Hadoop Equivalent |
|----------|---------|-------------------|
| `NameNodeHost` | `"localhost"` | `fs.defaultFS` (host part) |
| `NameNodePort` | `8020` | `fs.defaultFS` (port part) |
| `VerifyChecksum` | `true` | `dfs.client.read.checksum` |
| `RemoteBufferSize` | `524288` (512 KB) | `dfs.client.block.reader.remote.buffer.size` |
| `ConnectTimeout` | `60s` | -- |
| `ReadTimeout` | `60s` | -- |
| `PeerCacheMaxPerDataNode` | `64` | -- |
| `PeerCacheIdleTimeout` | `300s` | -- |
| `MaxBlockReadRetries` | `3` | `dfs.client.max.block.acquire.failures` |
| `DeadNodeExpiry` | `10m` | -- |
| `DataTransferProtection` | `""` | `dfs.data.transfer.protection` |
| `EncryptDataTransfer` | `false` | `dfs.encrypt.data.transfer` |
| `KerberosPrincipal` | `null` | -- |
| `KeytabPath` | `null` | -- |
| `DomainSocketPath` | `null` | `dfs.domain.socket.path` |
| `ClientName` | auto | -- |

## Observability

Metrics are exposed via `System.Diagnostics.Metrics` (meter name: `Gtlm.Hdfs.Client`):

| Metric | Type | Description |
|--------|------|-------------|
| `hdfs.client.bytes_read` | Counter | Total bytes read from DataNodes |
| `hdfs.client.block_reads` | Counter | Block read operations |
| `hdfs.client.block_read_failovers` | Counter | Replica failovers |
| `hdfs.client.checksum_errors` | Counter | Checksum verification failures |
| `hdfs.client.cached_peers` | UpDownCounter | Active cached connections |
| `hdfs.client.block_read_duration` | Histogram | Block read latency (ms) |
| `hdfs.client.namenode_rpc_duration` | Histogram | NameNode RPC latency (ms) |

Works with [OpenTelemetry .NET](https://opentelemetry.io/docs/languages/dotnet/),
`dotnet-counters`, Prometheus exporters, or any `MeterListener`.

## Project Structure

```
dotnet/
├── src/Gtlm.Hdfs.Client/          # Main library
│   ├── BlockReading/               # RemoteBlockReader, HdfsFileStream, LocalBlockReader
│   ├── Checksum/                   # CRC32C (hw-accelerated), CRC32, Null
│   ├── Configuration/              # HdfsClientOptions, HadoopConfigParser
│   ├── Diagnostics/                # HdfsMetrics (System.Diagnostics.Metrics)
│   ├── Models/                     # ExtendedBlock, DatanodeInfo, BlockToken, etc.
│   ├── Net/                        # Peer, PeerCache, DeadNodeTracker, SASL
│   ├── Protocol/                   # DataTransferSender/Receiver, PacketReader
│   ├── Proto/                      # Generated C# from Hadoop .proto files
│   ├── Rpc/                        # NameNodeRpcClient, HaNameNodeRpcClient
│   ├── Security/                   # KerberosCredentialProvider
│   └── HdfsClient.cs              # Top-level API
├── tests/Gtlm.Hdfs.Client.Tests/   # 191 unit + mock integration tests
│   └── IntegrationTests/           # Docker Compose Hadoop cluster tests
├── benchmarks/                     # BenchmarkDotNet performance suite
└── steps/                          # Implementation step documentation
```

## Requirements

- .NET 10 SDK
- Target cluster: Hadoop 2.6+ / 3.x (DataTransfer protocol version 28)
- For secure clusters: keytab file for the Kerberos principal

## Building

```bash
cd dotnet
dotnet build Gtlm.Hdfs.slnx
```

## Testing

```bash
# Unit + mock tests (no cluster needed)
dotnet test Gtlm.Hdfs.slnx --filter "Category!=Integration"

# Integration tests (requires Docker Compose Hadoop cluster)
cd tests/Gtlm.Hdfs.Client.Tests/IntegrationTests
./setup-test-data.sh
HDFS_TEST_ENABLED=true dotnet test ../../.. --filter "Category=Integration"
```

## Benchmarks

```bash
dotnet run -c Release --project benchmarks/Gtlm.Hdfs.Client.Benchmarks -- --filter "*Crc32C*"
```

## Protocol Compatibility

This client implements the same wire protocol as the Java Hadoop client:

- **Data Transfer Protocol v28** (Hadoop 2.6+) -- `OP_READ_BLOCK` with protobuf-encoded
  requests/responses and length-prefixed binary packet streaming
- **Hadoop IPC v9** -- NameNode RPC with protobuf payloads (`ClientProtocol`)
- **Checksums** -- CRC32C (default) and CRC32 (IEEE), verified per 512-byte chunk

The protobuf definitions are sourced directly from the
[Apache Hadoop repository](https://github.com/apache/hadoop) (Apache 2.0 license).
