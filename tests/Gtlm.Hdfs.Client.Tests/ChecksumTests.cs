namespace Gtlm.Hdfs.Client.Tests;

using System.Buffers.Binary;
using System.Text;
using Gtlm.Hdfs.Client.Checksum;
using Gtlm.Hdfs.Client.Protocol;
using Gtlm.Hdfs.Client.Proto;

public class ChecksumTests
{
    // --- CRC32 (IEEE) known vectors ---

    [Fact]
    public void CRC32_KnownVector_123456789()
    {
        var data = Encoding.ASCII.GetBytes("123456789");
        var checksum = DataChecksum.Create(ChecksumTypeProto.ChecksumCrc32, 512);
        // IEEE CRC32 of "123456789" = 0xCBF43926
        // Access via reflection or test VerifyChunks
        var crc = Crc32Checksum.ComputeIeeeCrc32(data);
        Assert.Equal(0xCBF43926U, crc);
    }

    [Fact]
    public void CRC32_EmptyData()
    {
        var crc = Crc32Checksum.ComputeIeeeCrc32([]);
        Assert.Equal(0x00000000U, crc);
    }

    // --- CRC32C (Castagnoli) known vectors ---

    [Fact]
    public void CRC32C_KnownVector_123456789()
    {
        var data = Encoding.ASCII.GetBytes("123456789");
        var crc = Crc32CChecksum.ComputeCrc32C(data);
        Assert.Equal(0xE3069283U, crc);
    }

    [Fact]
    public void CRC32C_EmptyData()
    {
        var crc = Crc32CChecksum.ComputeCrc32C([]);
        Assert.Equal(0x00000000U, crc);
    }

    [Fact]
    public void CRC32C_SingleByte()
    {
        // CRC32C of [0x00] = 0x527D5351
        var crc = Crc32CChecksum.ComputeCrc32C([0x00]);
        Assert.Equal(0x527D5351U, crc);
    }

    // --- Factory ---

    [Fact]
    public void Factory_CRC32_CreatesCorrectType()
    {
        var cs = DataChecksum.Create(ChecksumTypeProto.ChecksumCrc32, 512);
        Assert.Equal(512, cs.BytesPerChecksum);
        Assert.Equal(4, cs.ChecksumSize);
    }

    [Fact]
    public void Factory_CRC32C_CreatesCorrectType()
    {
        var cs = DataChecksum.Create(ChecksumTypeProto.ChecksumCrc32C, 512);
        Assert.Equal(512, cs.BytesPerChecksum);
        Assert.Equal(4, cs.ChecksumSize);
    }

    [Fact]
    public void Factory_Null_CreatesCorrectType()
    {
        var cs = DataChecksum.Create(ChecksumTypeProto.ChecksumNull, 512);
        Assert.Equal(512, cs.BytesPerChecksum);
        Assert.Equal(0, cs.ChecksumSize);
    }

    // --- GetChecksumBytesForDataLength ---

    [Theory]
    [InlineData(512, 512, 4)]     // 1 chunk, 1 CRC
    [InlineData(1024, 512, 8)]    // 2 chunks, 2 CRCs
    [InlineData(513, 512, 8)]     // 2 chunks (partial last), 2 CRCs
    [InlineData(1, 512, 4)]       // 1 byte = 1 chunk
    [InlineData(0, 512, 0)]       // 0 bytes = 0 checksums
    [InlineData(2048, 1024, 8)]   // 2 chunks with larger bpc
    public void GetChecksumBytesForDataLength(int dataLen, int bpc, int expected)
    {
        var cs = DataChecksum.Create(ChecksumTypeProto.ChecksumCrc32C, bpc);
        Assert.Equal(expected, cs.GetChecksumBytesForDataLength(dataLen));
    }

    [Fact]
    public void GetChecksumBytesForDataLength_NullChecksum_AlwaysZero()
    {
        var cs = DataChecksum.Create(ChecksumTypeProto.ChecksumNull, 512);
        Assert.Equal(0, cs.GetChecksumBytesForDataLength(1024));
    }

    // --- VerifyChunks ---

    [Fact]
    public void VerifyChunks_Valid_CRC32C_NoThrow()
    {
        var cs = DataChecksum.Create(ChecksumTypeProto.ChecksumCrc32C, 4);
        var data = Encoding.ASCII.GetBytes("test");
        uint crc = Crc32CChecksum.ComputeCrc32C(data);

        var checksums = new byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(checksums, crc);

        cs.VerifyChunks(data, checksums, offsetInBlock: 0); // should not throw
    }

    [Fact]
    public void VerifyChunks_Valid_MultipleChunks()
    {
        int bpc = 4;
        var cs = DataChecksum.Create(ChecksumTypeProto.ChecksumCrc32C, bpc);

        // 8 bytes of data = 2 chunks of 4 bytes each
        var data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };

        uint crc1 = Crc32CChecksum.ComputeCrc32C(data.AsSpan(0, 4));
        uint crc2 = Crc32CChecksum.ComputeCrc32C(data.AsSpan(4, 4));

        var checksums = new byte[8];
        BinaryPrimitives.WriteUInt32BigEndian(checksums.AsSpan(0), crc1);
        BinaryPrimitives.WriteUInt32BigEndian(checksums.AsSpan(4), crc2);

        cs.VerifyChunks(data, checksums, offsetInBlock: 0);
    }

    [Fact]
    public void VerifyChunks_PartialLastChunk()
    {
        int bpc = 4;
        var cs = DataChecksum.Create(ChecksumTypeProto.ChecksumCrc32C, bpc);

        // 6 bytes = 2 chunks: [0..3] and [4..5]
        var data = new byte[] { 10, 20, 30, 40, 50, 60 };

        uint crc1 = Crc32CChecksum.ComputeCrc32C(data.AsSpan(0, 4));
        uint crc2 = Crc32CChecksum.ComputeCrc32C(data.AsSpan(4, 2)); // partial

        var checksums = new byte[8];
        BinaryPrimitives.WriteUInt32BigEndian(checksums.AsSpan(0), crc1);
        BinaryPrimitives.WriteUInt32BigEndian(checksums.AsSpan(4), crc2);

        cs.VerifyChunks(data, checksums, offsetInBlock: 0);
    }

    [Fact]
    public void VerifyChunks_Corrupt_ThrowsChecksumException()
    {
        var cs = DataChecksum.Create(ChecksumTypeProto.ChecksumCrc32C, 4);
        var data = new byte[] { 1, 2, 3, 4 };

        // Provide wrong checksum
        var checksums = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };

        var ex = Assert.Throws<ChecksumException>(() =>
            cs.VerifyChunks(data, checksums, offsetInBlock: 100));

        Assert.Contains("mismatch", ex.Message);
        Assert.Equal(100, ex.Offset);
    }

    [Fact]
    public void VerifyChunks_Corrupt_SecondChunk_ReportsCorrectOffset()
    {
        int bpc = 4;
        var cs = DataChecksum.Create(ChecksumTypeProto.ChecksumCrc32C, bpc);
        var data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };

        uint crc1 = Crc32CChecksum.ComputeCrc32C(data.AsSpan(0, 4));

        var checksums = new byte[8];
        BinaryPrimitives.WriteUInt32BigEndian(checksums.AsSpan(0), crc1); // correct
        BinaryPrimitives.WriteUInt32BigEndian(checksums.AsSpan(4), 0xBADBAD00); // wrong

        var ex = Assert.Throws<ChecksumException>(() =>
            cs.VerifyChunks(data, checksums, offsetInBlock: 1000));

        Assert.Equal(1004, ex.Offset); // 1000 + 4 (second chunk start)
    }

    [Fact]
    public void VerifyChunks_NotEnoughChecksumBytes_Throws()
    {
        var cs = DataChecksum.Create(ChecksumTypeProto.ChecksumCrc32C, 4);
        var data = new byte[] { 1, 2, 3, 4 };

        // Only 2 bytes of checksum instead of 4
        var checksums = new byte[] { 0x00, 0x00 };

        Assert.Throws<ChecksumException>(() =>
            cs.VerifyChunks(data, checksums, offsetInBlock: 0));
    }

    [Fact]
    public void VerifyChunks_NullChecksum_NeverThrows()
    {
        var cs = DataChecksum.Create(ChecksumTypeProto.ChecksumNull, 512);
        var data = new byte[1024];
        Random.Shared.NextBytes(data);

        // Empty checksums with NULL checksum type
        cs.VerifyChunks(data, [], offsetInBlock: 0);
    }

    [Fact]
    public void VerifyChunks_CRC32_IEEE_Works()
    {
        var cs = DataChecksum.Create(ChecksumTypeProto.ChecksumCrc32, 512);
        var data = new byte[512];
        Random.Shared.NextBytes(data);

        uint crc = Crc32Checksum.ComputeIeeeCrc32(data);
        var checksums = new byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(checksums, crc);

        cs.VerifyChunks(data, checksums, offsetInBlock: 0);
    }
}
