# HDFS BlockReader .NET -- Implementation Steps

Detailed implementation steps for `Gtlm.Hdfs.Client`. Each step is self-contained
with code examples, acceptance criteria, and dependency information.

See [hdfs-block-reader-plan.md](../hdfs-block-reader-plan.md) for the high-level
architecture and design rationale.

---

## Phase 1: Core Block Reader (MVP)

Sequential dependencies -- each step builds on the previous.

| Step | File | Summary | Dependencies |
|------|------|---------|-------------|
| 01 | [01-project-scaffold-and-proto-setup.md](01-project-scaffold-and-proto-setup.md) | Solution, NuGet deps, proto codegen | None |
| 02 | [02-models-and-domain-types.md](02-models-and-domain-types.md) | ExtendedBlock, DatanodeInfo, BlockToken | 01 |
| 03 | [03-peer-tcp-connection.md](03-peer-tcp-connection.md) | TCP socket with PipeReader/PipeWriter | 01, 02 |
| 04 | [04-peer-cache.md](04-peer-cache.md) | Connection pool for DataNode reuse | 03 |
| 05 | [05-data-transfer-sender.md](05-data-transfer-sender.md) | OP_READ_BLOCK request serialization | 01, 02, 03 |
| 06 | [06-data-transfer-receiver.md](06-data-transfer-receiver.md) | BlockOpResponseProto parsing, validation | 01, 05 |
| 07 | [07-data-checksum.md](07-data-checksum.md) | CRC32/CRC32C with hardware intrinsics | 01 |
| 08 | [08-packet-reader.md](08-packet-reader.md) | Binary packet frame parser | 01, 03, 07 |
| 09 | [09-remote-block-reader.md](09-remote-block-reader.md) | RemoteBlockReader : Stream (the core class) | 02-08 |
| 10 | [10-unit-and-mock-tests.md](10-unit-and-mock-tests.md) | Unit tests, mock integration tests | 01-09 |
| 11 | [11-live-cluster-tests.md](11-live-cluster-tests.md) | Docker Hadoop integration tests | 01-10 |

**Phase 1 delivers:** A `Stream` that reads a single HDFS block from a DataNode.
Consumers provide the block location (obtained externally). Checksums are verified.
Connections are pooled.

---

## Phase 2: Full Client

Can be worked on in parallel after Phase 1 is stable.

| Step | File | Summary | Dependencies |
|------|------|---------|-------------|
| 12 | [12-namenode-rpc-client.md](12-namenode-rpc-client.md) | Hadoop IPC for getBlockLocations | Phase 1 |
| 13 | [13-hdfs-file-stream.md](13-hdfs-file-stream.md) | HdfsFileStream + HdfsClient top-level API | 09, 12 |
| 14 | [14-sasl-kerberos-auth.md](14-sasl-kerberos-auth.md) | SASL/GSSAPI for secure clusters | 03, 12 |
| 15 | [15-replica-failover.md](15-replica-failover.md) | Auto-retry with alternate DataNodes | 09, 13 |
| 16 | [16-short-circuit-local-reads.md](16-short-circuit-local-reads.md) | Local file reads for co-located clients | 09, 12 |

**Phase 2 delivers:** `HdfsClient.OpenReadAsync("/path")` -- full path-to-stream
resolution, Kerberos auth, and replica failover.

---

## Phase 3: Hardening

| Step | File | Summary | Dependencies |
|------|------|---------|-------------|
| 17 | [17-ha-namenode-metrics-config-benchmarks.md](17-ha-namenode-metrics-config-benchmarks.md) | HA failover, metrics, XML config, benchmarks | Phase 2 |

**Phase 3 delivers:** Production-readiness -- HA NameNode, observability, Hadoop
config compatibility, and performance validation.

---

## Dependency Graph

```
01 ──► 02 ──► 03 ──► 04
               │       │
               ▼       │
       05 ◄────┘       │
        │               │
        ▼               │
       06               │
                        │
       07 ──► 08 ◄─────┘
               │
               ▼
       09 ◄── (02-08)
        │
        ▼
       10 ──► 11
        │
        ├──► 12 ──► 13 ──► 15
        │     │
        │     ▼
        │    14
        │
        └──► 16
              │
              ▼
             17
```
