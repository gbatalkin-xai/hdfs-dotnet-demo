# Step 15: Replica Failover

**Phase:** 2 (Full Client)
**Prerequisites:** Step 09 (RemoteBlockReader), Step 13 (HdfsFileStream)
**Produces:** Automatic retry with alternate DataNode replicas on read failures

---

## Objective

When a block read fails (checksum error, timeout, connection refused), automatically
retry with the next DataNode in the `LocatedBlock.Locations` list. HDFS stores each
block on multiple DataNodes (typically 3 replicas), so transient failures on one
DataNode should not fail the read.

---

## Tasks

### 15.1 Retry Logic in `BlockReaderFactory`

Update `BlockReaderFactory.CreateRemoteReaderAsync` to accept the full `LocatedBlock`
and try each DataNode location in order:

```csharp
public async Task<RemoteBlockReader> CreateRemoteReaderWithFailoverAsync(
    string file,
    LocatedBlock locatedBlock,
    long offsetInBlock,
    long length,
    CancellationToken ct = default)
{
    var exceptions = new List<Exception>();

    foreach (var dataNode in locatedBlock.Locations)
    {
        try
        {
            return await CreateRemoteReaderAsync(
                file, locatedBlock, dataNode, offsetInBlock, length, ct);
        }
        catch (Exception ex) when (IsRetryableException(ex))
        {
            _logger?.LogWarning(ex,
                "Failed to read block {BlockId} from {DataNode}, trying next replica",
                locatedBlock.Block.BlockId, dataNode);
            exceptions.Add(ex);
        }
    }

    throw new AggregateException(
        $"All {locatedBlock.Locations.Count} replicas failed for block " +
        $"{locatedBlock.Block.BlockId} of {file}", exceptions);
}

private static bool IsRetryableException(Exception ex) =>
    ex is IOException or SocketException or ChecksumException or TimeoutException;
```

### 15.2 Mid-Read Failover in `HdfsFileStream`

If a `RemoteBlockReader` fails mid-read (not during creation), `HdfsFileStream`
must catch the exception, open a new reader on the next replica starting at the
current position within the block, and continue.

```csharp
// In HdfsFileStream.ReadAsync:
try
{
    bytesRead = await _currentReader.ReadAsync(buffer, ct);
}
catch (Exception ex) when (IsRetryableException(ex))
{
    _logger?.LogWarning(ex, "Mid-read failure on block {BlockIndex}, failing over",
        _currentBlockIndex);
    await _currentReader.DisposeAsync();
    _currentReader = await OpenNextReplicaAsync(_currentBlockIndex, ct);
    bytesRead = await _currentReader.ReadAsync(buffer, ct);
}
```

### 15.3 Dead Node Tracking

**File:** `src/Gtlm.Hdfs.Client/Net/DeadNodeTracker.cs`

Track DataNodes that recently failed. Deprioritize them when choosing replicas:

```csharp
public sealed class DeadNodeTracker
{
    private readonly ConcurrentDictionary<string, long> _deadNodes = new();
    private readonly TimeSpan _deadNodeExpiry = TimeSpan.FromMinutes(10);

    public void MarkDead(DatanodeInfo dataNode);
    public bool IsDead(DatanodeInfo dataNode);

    /// <summary>
    /// Sort locations with dead nodes moved to the end.
    /// </summary>
    public IReadOnlyList<DatanodeInfo> PrioritizeLocations(
        IReadOnlyList<DatanodeInfo> locations);
}
```

### 15.4 Retry Configuration

Add to `HdfsClientOptions`:

```csharp
/// <summary>Maximum number of replica attempts per block read.</summary>
public int MaxBlockReadRetries { get; set; } = 3;

/// <summary>Duration to mark a DataNode as dead after failure.</summary>
public TimeSpan DeadNodeExpiry { get; set; } = TimeSpan.FromMinutes(10);
```

---

## Acceptance Criteria

- [ ] Read succeeds when first DataNode is unreachable but second is healthy
- [ ] `ChecksumException` triggers failover to next replica
- [ ] Connection timeout triggers failover
- [ ] Mid-read failure resumes from correct position on next replica
- [ ] Dead nodes are deprioritized in subsequent reads
- [ ] `AggregateException` thrown only when all replicas fail
- [ ] Retries respect `MaxBlockReadRetries` limit
