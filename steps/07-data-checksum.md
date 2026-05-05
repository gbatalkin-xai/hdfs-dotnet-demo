# Step 07: DataChecksum (CRC32 / CRC32C)

**Phase:** 1 (MVP)
**Prerequisites:** Step 01 (proto -- `ChecksumTypeProto` enum)
**Produces:** `Checksum/` directory with checksum verification

---

## Objective

Implement chunk-level checksum verification for HDFS block data. Each packet from the
DataNode contains checksum bytes (one CRC per `bytesPerChecksum` chunk of data). This
step provides the `DataChecksum` abstraction and concrete CRC32/CRC32C implementations.

HDFS defaults to CRC32C with 512-byte chunks. Performance is critical -- hardware
intrinsics must be used when available.

---

## Tasks

### 7.1 `DataChecksum` Base Class

**File:** `src/Gtlm.Hdfs.Client/Checksum/DataChecksum.cs`

```csharp
namespace Gtlm.Hdfs.Client.Checksum;

using Gtlm.Hdfs.Client.Proto;
using Gtlm.Hdfs.Client.Protocol;

/// <summary>
/// HDFS data checksum calculator and verifier.
/// Each instance is configured for a specific algorithm and chunk size.
/// </summary>
public abstract class DataChecksum
{
    /// <summary>Bytes of data covered by each checksum.</summary>
    public int BytesPerChecksum { get; }

    /// <summary>Size of each checksum value in bytes (always 4 for CRC32/CRC32C).</summary>
    public int ChecksumSize { get; }

    protected DataChecksum(int bytesPerChecksum, int checksumSize)
    {
        BytesPerChecksum = bytesPerChecksum;
        ChecksumSize = checksumSize;
    }

    /// <summary>
    /// Factory: create a DataChecksum from the proto checksum type.
    /// </summary>
    public static DataChecksum Create(ChecksumTypeProto type, int bytesPerChecksum)
    {
        return type switch
        {
            ChecksumTypeProto.ChecksumCrc32 => new Crc32Checksum(bytesPerChecksum),
            ChecksumTypeProto.ChecksumCrc32C => new Crc32CChecksum(bytesPerChecksum),
            ChecksumTypeProto.ChecksumNull => new NullChecksum(bytesPerChecksum),
            _ => throw new ArgumentException($"Unsupported checksum type: {type}"),
        };
    }

    /// <summary>
    /// Compute the checksum of a single chunk.
    /// </summary>
    protected abstract uint ComputeChecksum(ReadOnlySpan<byte> data);

    /// <summary>
    /// Verify checksums for a data buffer.
    /// Data is split into bytesPerChecksum-sized chunks.
    /// Each chunk's computed CRC must match the corresponding 4-byte value
    /// in the checksums buffer.
    ///
    /// Throws ChecksumException on mismatch.
    /// </summary>
    public void VerifyChunks(
        ReadOnlySpan<byte> data,
        ReadOnlySpan<byte> checksums,
        long offsetInBlock)
    {
        int numChunks = (data.Length + BytesPerChecksum - 1) / BytesPerChecksum;
        int expectedChecksumBytes = numChunks * ChecksumSize;

        if (checksums.Length < expectedChecksumBytes)
        {
            throw new ChecksumException(
                $"Not enough checksum bytes: expected {expectedChecksumBytes}, got {checksums.Length}",
                offsetInBlock);
        }

        for (int i = 0; i < numChunks; i++)
        {
            int dataStart = i * BytesPerChecksum;
            int dataEnd = Math.Min(dataStart + BytesPerChecksum, data.Length);
            var chunk = data[dataStart..dataEnd];

            uint computed = ComputeChecksum(chunk);

            int csOffset = i * ChecksumSize;
            uint expected = BinaryPrimitives.ReadUInt32BigEndian(checksums[csOffset..]);

            if (computed != expected)
            {
                throw new ChecksumException(
                    $"Checksum mismatch at chunk {i} (offset {offsetInBlock + dataStart}): " +
                    $"expected 0x{expected:X8}, computed 0x{computed:X8}",
                    offsetInBlock + dataStart);
            }
        }
    }

    /// <summary>
    /// Calculate how many checksum bytes correspond to the given data length.
    /// </summary>
    public int GetChecksumBytesForDataLength(int dataLength)
    {
        int numChunks = (dataLength + BytesPerChecksum - 1) / BytesPerChecksum;
        return numChunks * ChecksumSize;
    }
}
```

### 7.2 CRC32C (Castagnoli) -- Hardware Accelerated

**File:** `src/Gtlm.Hdfs.Client/Checksum/Crc32CChecksum.cs`

```csharp
namespace Gtlm.Hdfs.Client.Checksum;

using System.IO.Hashing;

/// <summary>
/// CRC32C (Castagnoli) checksum. This is HDFS's default checksum algorithm.
///
/// Uses System.IO.Hashing.Crc32 with iSCSI polynomial (Castagnoli).
/// On x64, .NET automatically uses SSE4.2 CRC32 intrinsics.
/// On ARM64, .NET uses the CRC32 extension instructions.
/// </summary>
internal sealed class Crc32CChecksum : DataChecksum
{
    public Crc32CChecksum(int bytesPerChecksum) : base(bytesPerChecksum, checksumSize: 4) { }

    protected override uint ComputeChecksum(ReadOnlySpan<byte> data)
    {
        // System.IO.Hashing.Crc32 uses the Castagnoli polynomial (CRC32C)
        return Crc32.HashToUInt32(data);
    }
}
```

**Important:** Verify that `System.IO.Hashing.Crc32` uses the Castagnoli (iSCSI)
polynomial, NOT the IEEE polynomial. In .NET's `System.IO.Hashing`:
- `Crc32` = IEEE (standard) polynomial
- There is no built-in `Crc32C` class

If `System.IO.Hashing.Crc32` uses IEEE, implement CRC32C manually with intrinsics:

```csharp
using System.Runtime.Intrinsics.X86;
using System.Runtime.Intrinsics.Arm;

protected override uint ComputeChecksum(ReadOnlySpan<byte> data)
{
    uint crc = 0xFFFFFFFF;

    if (Sse42.IsSupported)
    {
        int i = 0;
        // Process 8 bytes at a time on x64
        if (Sse42.X64.IsSupported)
        {
            for (; i + 7 < data.Length; i += 8)
            {
                crc = (uint)Sse42.X64.Crc32(crc, BinaryPrimitives.ReadUInt64LittleEndian(data[i..]));
            }
        }
        for (; i + 3 < data.Length; i += 4)
        {
            crc = Sse42.Crc32(crc, BinaryPrimitives.ReadUInt32LittleEndian(data[i..]));
        }
        for (; i < data.Length; i++)
        {
            crc = Sse42.Crc32(crc, data[i]);
        }
    }
    else if (System.Runtime.Intrinsics.Arm.Crc32.IsSupported)
    {
        int i = 0;
        if (System.Runtime.Intrinsics.Arm.Crc32.Arm64.IsSupported)
        {
            for (; i + 7 < data.Length; i += 8)
            {
                crc = System.Runtime.Intrinsics.Arm.Crc32.Arm64.ComputeCrc32C(
                    crc, BinaryPrimitives.ReadUInt64LittleEndian(data[i..]));
            }
        }
        for (; i + 3 < data.Length; i += 4)
        {
            crc = System.Runtime.Intrinsics.Arm.Crc32.ComputeCrc32C(
                crc, BinaryPrimitives.ReadUInt32LittleEndian(data[i..]));
        }
        for (; i < data.Length; i++)
        {
            crc = System.Runtime.Intrinsics.Arm.Crc32.ComputeCrc32C(crc, data[i]);
        }
    }
    else
    {
        // Software fallback with lookup table
        crc = Crc32CSoftware.Compute(data, crc);
    }

    return crc ^ 0xFFFFFFFF;
}
```

**Resolution:** Check the actual polynomial used by `System.IO.Hashing.Crc32` at
implementation time. The `Crc32` class in `System.IO.Hashing` as of .NET 9+ does use
IEEE. For CRC32C, either use the intrinsics approach above or add a NuGet package
like `CommunityToolkit.HighPerformance` or `Crc32C.NET`.

### 7.3 CRC32 (IEEE) -- Standard

**File:** `src/Gtlm.Hdfs.Client/Checksum/Crc32Checksum.cs`

```csharp
namespace Gtlm.Hdfs.Client.Checksum;

using System.IO.Hashing;

/// <summary>
/// CRC32 (IEEE 802.3) checksum. Used by older HDFS configurations.
/// </summary>
internal sealed class Crc32Checksum : DataChecksum
{
    public Crc32Checksum(int bytesPerChecksum) : base(bytesPerChecksum, checksumSize: 4) { }

    protected override uint ComputeChecksum(ReadOnlySpan<byte> data)
    {
        return Crc32.HashToUInt32(data);
    }
}
```

### 7.4 Null Checksum

**File:** `src/Gtlm.Hdfs.Client/Checksum/NullChecksum.cs`

```csharp
namespace Gtlm.Hdfs.Client.Checksum;

/// <summary>
/// No-op checksum. Used when CHECKSUM_NULL is negotiated.
/// VerifyChunks is a no-op.
/// </summary>
internal sealed class NullChecksum : DataChecksum
{
    public NullChecksum(int bytesPerChecksum) : base(bytesPerChecksum, checksumSize: 0) { }

    protected override uint ComputeChecksum(ReadOnlySpan<byte> data) => 0;

    public new void VerifyChunks(ReadOnlySpan<byte> data, ReadOnlySpan<byte> checksums,
        long offsetInBlock)
    {
        // No verification
    }
}
```

---

## Checksum Byte Order

HDFS stores and transmits checksums in **big-endian** byte order. The `VerifyChunks`
method reads expected values using `BinaryPrimitives.ReadUInt32BigEndian`. Ensure the
computed checksum matches this convention.

Java's `DataChecksum.update()` produces values compatible with `DataOutputStream.writeInt()`
which is big-endian. The intrinsics produce native-endian values, so no byte-swap is
needed on the computed side -- just compare as uint32 values after reading the expected
value as big-endian.

---

## Performance Considerations

- On x64 with SSE4.2, CRC32C processes 8 bytes per instruction (~3 GB/s throughput).
- On ARM64 with CRC extensions, similar throughput.
- Software fallback is ~300-500 MB/s (table-based), acceptable but slower.
- `bytesPerChecksum` is typically 512 bytes, so each chunk is small. The intrinsics
  loop handles 512 bytes in ~64 iterations (8 bytes each).

---

## Acceptance Criteria

- [ ] CRC32C computed values match known test vectors:
  - `CRC32C("")` = `0x00000000`
  - `CRC32C("123456789")` = `0xE3069283`
- [ ] CRC32 (IEEE) computed values match known test vectors:
  - `CRC32("123456789")` = `0xCBF43926`
- [ ] `VerifyChunks` passes when checksums are correct
- [ ] `VerifyChunks` throws `ChecksumException` on corrupted data
- [ ] `VerifyChunks` handles partial last chunk (data.Length not divisible by bytesPerChecksum)
- [ ] `GetChecksumBytesForDataLength` returns correct values:
  - 512 bytes data, 512 bytesPerChecksum = 4 checksum bytes
  - 1024 bytes data, 512 bytesPerChecksum = 8 checksum bytes
  - 513 bytes data, 512 bytesPerChecksum = 8 checksum bytes (2 chunks)
- [ ] NullChecksum.VerifyChunks never throws
