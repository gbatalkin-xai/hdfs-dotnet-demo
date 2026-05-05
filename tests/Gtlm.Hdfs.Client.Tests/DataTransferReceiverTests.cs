namespace Gtlm.Hdfs.Client.Tests;

using Google.Protobuf;
using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Net;
using Gtlm.Hdfs.Client.Protocol;
using Gtlm.Hdfs.Client.Proto;

public class DataTransferReceiverTests
{
    private static readonly DatanodeInfo TestDn = new()
    {
        IpAddress = "10.0.0.1",
        HostName = "dn1",
        DatanodeUuid = "u1",
        XferPort = 9866,
    };

    private static readonly ExtendedBlock TestBlock = new("BP-1", 42, 100);

    private static Peer PeerWithResponse(BlockOpResponseProto response)
    {
        var ms = new MemoryStream();
        response.WriteDelimitedTo(ms);
        ms.Position = 0;
        return Peer.CreateForTest(ms, TestDn);
    }

    // --- ReceiveBlockOpResponse ---

    [Fact]
    public void ReceiveBlockOpResponse_Success_ParsesCorrectly()
    {
        var response = new BlockOpResponseProto
        {
            Status = Status.Success,
            ReadOpChecksumInfo = new ReadOpChecksumInfoProto
            {
                Checksum = new ChecksumProto
                {
                    Type = ChecksumTypeProto.ChecksumCrc32C,
                    BytesPerChecksum = 512,
                },
                ChunkOffset = 0,
            },
        };

        var peer = PeerWithResponse(response);
        var parsed = DataTransferReceiver.ReceiveBlockOpResponse(peer);

        Assert.Equal(Status.Success, parsed.Status);
        Assert.Equal(ChecksumTypeProto.ChecksumCrc32C,
            parsed.ReadOpChecksumInfo.Checksum.Type);
        Assert.Equal(512U, parsed.ReadOpChecksumInfo.Checksum.BytesPerChecksum);
    }

    [Fact]
    public void ReceiveBlockOpResponse_Error_ParsesStatus()
    {
        var response = new BlockOpResponseProto
        {
            Status = Status.Error,
            Message = "block not found",
        };

        var peer = PeerWithResponse(response);
        var parsed = DataTransferReceiver.ReceiveBlockOpResponse(peer);

        Assert.Equal(Status.Error, parsed.Status);
        Assert.Equal("block not found", parsed.Message);
    }

    // --- ValidateAndExtractChecksumInfo ---

    [Fact]
    public void Validate_Success_ReturnsChecksumInfo()
    {
        var response = new BlockOpResponseProto
        {
            Status = Status.Success,
            ReadOpChecksumInfo = new ReadOpChecksumInfoProto
            {
                Checksum = new ChecksumProto
                {
                    Type = ChecksumTypeProto.ChecksumCrc32C,
                    BytesPerChecksum = 512,
                },
                ChunkOffset = 256,
            },
        };

        var info = DataTransferReceiver.ValidateAndExtractChecksumInfo(
            response, TestDn, TestBlock, "/test/file");

        Assert.Equal(ChecksumTypeProto.ChecksumCrc32C, info.ChecksumType);
        Assert.Equal(512, info.BytesPerChecksum);
        Assert.Equal(256, info.ChunkOffset);
    }

    [Fact]
    public void Validate_CRC32_ReturnsCorrectType()
    {
        var response = new BlockOpResponseProto
        {
            Status = Status.Success,
            ReadOpChecksumInfo = new ReadOpChecksumInfoProto
            {
                Checksum = new ChecksumProto
                {
                    Type = ChecksumTypeProto.ChecksumCrc32,
                    BytesPerChecksum = 1024,
                },
                ChunkOffset = 0,
            },
        };

        var info = DataTransferReceiver.ValidateAndExtractChecksumInfo(
            response, TestDn, TestBlock, "/test/file");

        Assert.Equal(ChecksumTypeProto.ChecksumCrc32, info.ChecksumType);
        Assert.Equal(1024, info.BytesPerChecksum);
    }

    [Fact]
    public void Validate_ErrorStatus_ThrowsHdfsProtocolException()
    {
        var response = new BlockOpResponseProto
        {
            Status = Status.Error,
            Message = "something went wrong",
        };

        var ex = Assert.Throws<HdfsProtocolException>(() =>
            DataTransferReceiver.ValidateAndExtractChecksumInfo(
                response, TestDn, TestBlock, "/test/file"));

        Assert.Contains("Error", ex.Message);
        Assert.Contains("something went wrong", ex.Message);
        Assert.Contains("42", ex.Message); // block ID
    }

    [Fact]
    public void Validate_ErrorStatus_NoMessage_StillThrows()
    {
        var response = new BlockOpResponseProto
        {
            Status = Status.ErrorInvalid,
        };

        var ex = Assert.Throws<HdfsProtocolException>(() =>
            DataTransferReceiver.ValidateAndExtractChecksumInfo(
                response, TestDn, TestBlock, "/test/file"));

        Assert.Contains("ErrorInvalid", ex.Message);
    }

    [Fact]
    public void Validate_AccessTokenError_ThrowsAccessTokenException()
    {
        var response = new BlockOpResponseProto
        {
            Status = Status.ErrorAccessToken,
            Message = "token expired",
        };

        var ex = Assert.Throws<AccessTokenException>(() =>
            DataTransferReceiver.ValidateAndExtractChecksumInfo(
                response, TestDn, TestBlock, "/test/file"));

        Assert.Contains("token expired", ex.Message);
        Assert.Contains("42", ex.Message);
    }

    [Fact]
    public void Validate_AccessTokenError_NoMessage()
    {
        var response = new BlockOpResponseProto
        {
            Status = Status.ErrorAccessToken,
        };

        var ex = Assert.Throws<AccessTokenException>(() =>
            DataTransferReceiver.ValidateAndExtractChecksumInfo(
                response, TestDn, TestBlock, "/test/file"));

        Assert.Contains("no details", ex.Message);
    }

    [Fact]
    public void Validate_MissingChecksumInfo_Throws()
    {
        var response = new BlockOpResponseProto
        {
            Status = Status.Success,
            // no ReadOpChecksumInfo
        };

        var ex = Assert.Throws<HdfsProtocolException>(() =>
            DataTransferReceiver.ValidateAndExtractChecksumInfo(
                response, TestDn, TestBlock, "/test/file"));

        Assert.Contains("ReadOpChecksumInfo", ex.Message);
    }

    // --- ValidateChunkOffset ---

    [Fact]
    public void ValidateChunkOffset_Valid_AlignedStart()
    {
        // startOffset=0, chunkOffset=0: perfectly aligned
        DataTransferReceiver.ValidateChunkOffset(
            firstChunkOffset: 0, startOffset: 0, bytesPerChecksum: 512, file: "/f");
    }

    [Fact]
    public void ValidateChunkOffset_Valid_MidChunk()
    {
        // startOffset=300, chunkOffset=0: reading from middle of first chunk
        DataTransferReceiver.ValidateChunkOffset(
            firstChunkOffset: 0, startOffset: 300, bytesPerChecksum: 512, file: "/f");
    }

    [Fact]
    public void ValidateChunkOffset_Valid_ChunkBoundary()
    {
        // startOffset=1024, chunkOffset=1024: perfectly aligned at chunk boundary
        DataTransferReceiver.ValidateChunkOffset(
            firstChunkOffset: 1024, startOffset: 1024, bytesPerChecksum: 512, file: "/f");
    }

    [Fact]
    public void ValidateChunkOffset_Valid_OneChunkBack()
    {
        // startOffset=700, chunkOffset=512: first chunk starts within one bpc of startOffset
        DataTransferReceiver.ValidateChunkOffset(
            firstChunkOffset: 512, startOffset: 700, bytesPerChecksum: 512, file: "/f");
    }

    [Fact]
    public void ValidateChunkOffset_Valid_ExactMatch()
    {
        // startOffset=512, chunkOffset=512: chunk starts at startOffset
        DataTransferReceiver.ValidateChunkOffset(
            firstChunkOffset: 512, startOffset: 512, bytesPerChecksum: 512, file: "/f");
    }

    [Fact]
    public void ValidateChunkOffset_Negative_Throws()
    {
        Assert.Throws<IOException>(() =>
            DataTransferReceiver.ValidateChunkOffset(
                firstChunkOffset: -1, startOffset: 0, bytesPerChecksum: 512, file: "/f"));
    }

    [Fact]
    public void ValidateChunkOffset_BeyondStartOffset_Throws()
    {
        // chunkOffset > startOffset is invalid
        Assert.Throws<IOException>(() =>
            DataTransferReceiver.ValidateChunkOffset(
                firstChunkOffset: 100, startOffset: 50, bytesPerChecksum: 512, file: "/f"));
    }

    [Fact]
    public void ValidateChunkOffset_TooFarBack_Throws()
    {
        // chunkOffset is more than one chunk behind startOffset
        // startOffset=1536, chunkOffset=512, bpc=512 → 512 <= 1536-512=1024 → throws
        Assert.Throws<IOException>(() =>
            DataTransferReceiver.ValidateChunkOffset(
                firstChunkOffset: 512, startOffset: 1536, bytesPerChecksum: 512, file: "/f"));
    }
}
