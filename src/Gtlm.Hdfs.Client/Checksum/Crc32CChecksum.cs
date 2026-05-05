namespace Gtlm.Hdfs.Client.Checksum;

using System.Buffers.Binary;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics.X86;

/// <summary>
/// CRC32C (Castagnoli) checksum. This is HDFS's default checksum algorithm.
/// Uses hardware intrinsics (SSE4.2 on x64, CRC32 on ARM64) when available,
/// falls back to a software lookup table.
/// </summary>
internal sealed class Crc32CChecksum : DataChecksum
{
    public Crc32CChecksum(int bytesPerChecksum) : base(bytesPerChecksum, checksumSize: 4) { }

    protected override uint ComputeChecksum(ReadOnlySpan<byte> data)
    {
        return ComputeCrc32C(data);
    }

    internal static uint ComputeCrc32C(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFF;
        int i = 0;

        if (Sse42.IsSupported)
        {
            if (Sse42.X64.IsSupported)
            {
                for (; i + 7 < data.Length; i += 8)
                    crc = (uint)Sse42.X64.Crc32(crc, BinaryPrimitives.ReadUInt64LittleEndian(data[i..]));
            }
            for (; i + 3 < data.Length; i += 4)
                crc = Sse42.Crc32(crc, BinaryPrimitives.ReadUInt32LittleEndian(data[i..]));
            for (; i < data.Length; i++)
                crc = Sse42.Crc32(crc, data[i]);
        }
        else if (System.Runtime.Intrinsics.Arm.Crc32.IsSupported)
        {
            if (System.Runtime.Intrinsics.Arm.Crc32.Arm64.IsSupported)
            {
                for (; i + 7 < data.Length; i += 8)
                    crc = System.Runtime.Intrinsics.Arm.Crc32.Arm64.ComputeCrc32C(
                        crc, BinaryPrimitives.ReadUInt64LittleEndian(data[i..]));
            }
            for (; i + 3 < data.Length; i += 4)
                crc = System.Runtime.Intrinsics.Arm.Crc32.ComputeCrc32C(
                    crc, BinaryPrimitives.ReadUInt32LittleEndian(data[i..]));
            for (; i < data.Length; i++)
                crc = System.Runtime.Intrinsics.Arm.Crc32.ComputeCrc32C(crc, data[i]);
        }
        else
        {
            crc = SoftwareCrc32C(data, crc);
            return crc ^ 0xFFFFFFFF;
        }

        return crc ^ 0xFFFFFFFF;
    }

    private static uint SoftwareCrc32C(ReadOnlySpan<byte> data, uint crc)
    {
        foreach (byte b in data)
        {
            crc = (crc >> 8) ^ Crc32CTable[(crc ^ b) & 0xFF];
        }
        return crc;
    }

    // CRC32C (Castagnoli) lookup table, polynomial 0x82F63B78 (reflected)
    private static readonly uint[] Crc32CTable = GenerateTable(0x82F63B78);

    private static uint[] GenerateTable(uint polynomial)
    {
        var table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            uint crc = i;
            for (int j = 0; j < 8; j++)
            {
                if ((crc & 1) != 0)
                    crc = (crc >> 1) ^ polynomial;
                else
                    crc >>= 1;
            }
            table[i] = crc;
        }
        return table;
    }
}
