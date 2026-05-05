# Step 11: Live Cluster Integration Tests

**Phase:** 1 (MVP)
**Prerequisites:** Steps 01-10 (all Phase 1 components + unit tests pass)
**Produces:** Integration tests against a real Hadoop cluster

---

## Objective

Validate the `RemoteBlockReader` against a real HDFS DataNode. This catches protocol
version mismatches, real checksum formats, and edge cases that synthetic tests miss.

---

## Tasks

### 11.1 Docker Compose Hadoop Cluster

**File:** `tests/Gtlm.Hdfs.Client.Tests/IntegrationTests/docker-compose.yml`

```yaml
version: "3.8"

services:
  namenode:
    image: apache/hadoop:3.3.6
    hostname: namenode
    command: ["hdfs", "namenode"]
    ports:
      - "9870:9870"    # NameNode Web UI
      - "8020:8020"    # NameNode RPC
    environment:
      HADOOP_CONF_DIR: /etc/hadoop
    volumes:
      - ./hadoop-conf:/etc/hadoop
      - namenode-data:/hadoop/dfs/name

  datanode:
    image: apache/hadoop:3.3.6
    hostname: datanode
    command: ["hdfs", "datanode"]
    ports:
      - "9866:9866"    # DataNode data transfer port
      - "9864:9864"    # DataNode Web UI
    environment:
      HADOOP_CONF_DIR: /etc/hadoop
    volumes:
      - ./hadoop-conf:/etc/hadoop
      - datanode-data:/hadoop/dfs/data
    depends_on:
      - namenode

volumes:
  namenode-data:
  datanode-data:
```

### 11.2 Hadoop Configuration

**File:** `tests/Gtlm.Hdfs.Client.Tests/IntegrationTests/hadoop-conf/core-site.xml`

```xml
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://namenode:8020</value>
  </property>
</configuration>
```

**File:** `tests/Gtlm.Hdfs.Client.Tests/IntegrationTests/hadoop-conf/hdfs-site.xml`

```xml
<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/hadoop/dfs/name</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/hadoop/dfs/data</value>
  </property>
  <!-- Disable security for testing -->
  <property>
    <name>dfs.data.transfer.protection</name>
    <value></value>
  </property>
  <!-- Use CRC32C (default) -->
  <property>
    <name>dfs.checksum.type</name>
    <value>CRC32C</value>
  </property>
  <property>
    <name>dfs.bytes-per-checksum</name>
    <value>512</value>
  </property>
</configuration>
```

### 11.3 Test Setup -- Upload Test Files

Create a script or test fixture that:

1. Starts the Docker Compose cluster
2. Waits for NameNode to leave safe mode
3. Uploads test files of various sizes via `hdfs dfs -put`
4. Records the block locations (DataNode IP, block ID, offsets) via
   `hdfs fsck /testfile -files -blocks -locations`

```bash
#!/bin/bash
# tests/Gtlm.Hdfs.Client.Tests/IntegrationTests/setup-test-data.sh

docker compose up -d
sleep 30  # Wait for HDFS to initialize

# Create test files
dd if=/dev/urandom of=/tmp/testfile-small bs=1024 count=1       # 1 KB
dd if=/dev/urandom of=/tmp/testfile-medium bs=1048576 count=10  # 10 MB
dd if=/dev/urandom of=/tmp/testfile-exact bs=512 count=1        # 512 bytes (one checksum chunk)

# Upload to HDFS
docker exec namenode hdfs dfs -mkdir -p /test
docker exec namenode hdfs dfs -put /tmp/testfile-small /test/small.dat
docker exec namenode hdfs dfs -put /tmp/testfile-medium /test/medium.dat
docker exec namenode hdfs dfs -put /tmp/testfile-exact /test/exact.dat

# Get block locations
docker exec namenode hdfs fsck /test/small.dat -files -blocks -locations
docker exec namenode hdfs fsck /test/medium.dat -files -blocks -locations
```

### 11.4 Integration Test Class

**File:** `tests/Gtlm.Hdfs.Client.Tests/IntegrationTests/LiveClusterTests.cs`

```csharp
namespace Gtlm.Hdfs.Client.Tests.IntegrationTests;

/// <summary>
/// Tests against a real Hadoop cluster (Docker Compose).
/// These tests are skipped in CI unless HDFS_TEST_ENABLED=true.
/// </summary>
[Collection("LiveCluster")]
[Trait("Category", "Integration")]
public class LiveClusterTests : IAsyncLifetime
{
    // Datanode address (mapped port from docker-compose)
    private const string DataNodeIp = "127.0.0.1";
    private const int DataNodePort = 9866;

    // Block info obtained from setup script (or fetched via WebHDFS)
    // For Phase 1, these are hardcoded or read from a config file.
    // Phase 2 will use the NameNode RPC client to resolve them.

    [SkippableFact]
    public async Task ReadSmallFile_MatchesUploadedContent()
    {
        Skip.IfNot(IsClusterAvailable());

        // 1. Get block info (hardcoded or via WebHDFS REST for now)
        // 2. Connect to DataNode
        // 3. CreateAsync RemoteBlockReader
        // 4. Read all bytes
        // 5. Compare with original file byte-for-byte
    }

    [SkippableFact]
    public async Task ReadMediumFile_AllBlocks_MatchesContent()
    {
        // Same but for a multi-block file (10 MB)
        // Read each block separately and concatenate
    }

    [SkippableFact]
    public async Task ReadWithOffset_ReadsCorrectSubset()
    {
        // Read starting at offset 256 (non-chunk-aligned)
        // Verify firstChunkOffset alignment handling
    }

    [SkippableFact]
    public async Task ReadWithPeerCache_ReusesConnection()
    {
        // Read two blocks from the same DataNode
        // Verify second read reuses the cached Peer
    }

    private static bool IsClusterAvailable()
    {
        return Environment.GetEnvironmentVariable("HDFS_TEST_ENABLED") == "true";
    }
}
```

### 11.5 Phase 1 Block Discovery (WebHDFS Workaround)

Since Phase 1 does not include the NameNode RPC client, use the WebHDFS REST API
to get block locations for test files:

```
GET http://namenode:9870/webhdfs/v1/test/small.dat?op=GET_BLOCK_LOCATIONS
```

This returns JSON with block IDs, DataNode addresses, and offsets. Parse this in the
test fixture to set up the `LocatedBlock` / `ExtendedBlock` / `DatanodeInfo` needed
by `RemoteBlockReader.CreateAsync`.

### 11.6 Test Matrix

| Test | File Size | Offset | Length | Validates |
|------|-----------|--------|--------|-----------|
| Small file | 1 KB | 0 | full | Basic read, single packet |
| Medium file | 10 MB | 0 | full | Multi-packet, multi-block |
| Offset read | 1 KB | 256 | 512 | Chunk alignment handling |
| Exact chunk | 512 B | 0 | full | Single checksum chunk |
| Partial read | 10 MB | 0 | 4096 | Reading less than full block |
| Peer reuse | any | -- | -- | PeerCache integration |

---

## Acceptance Criteria

- [ ] Docker Compose cluster starts and enters operational state
- [ ] Small file read matches uploaded content byte-for-byte
- [ ] Multi-block file read is correct across all blocks
- [ ] Non-aligned offset reads work correctly
- [ ] CRC32C checksum verification passes on real DataNode packets
- [ ] Tests are skipped gracefully when cluster is not available
- [ ] Cleanup: Docker Compose cluster can be torn down cleanly
