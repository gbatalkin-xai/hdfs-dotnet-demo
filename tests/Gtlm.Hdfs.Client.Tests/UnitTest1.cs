namespace Gtlm.Hdfs.Client.Tests;

using Google.Protobuf;
using Gtlm.Hdfs.Client.Proto;

/// <summary>
/// Verifies that all required protobuf types were generated correctly
/// and can be instantiated and serialized.
/// </summary>
public class ProtoGenerationTests
{
    [Fact]
    public void OpReadBlockProto_CanBeConstructed()
    {
        var proto = new OpReadBlockProto
        {
            Header = new ClientOperationHeaderProto
            {
                BaseHeader = new BaseHeaderProto
                {
                    Block = new ExtendedBlockProto
                    {
                        PoolId = "BP-123",
                        BlockId = 42,
                        GenerationStamp = 1000,
                    },
                },
                ClientName = "test-client",
            },
            Offset = 0,
            Len = 1024,
            SendChecksums = true,
        };

        Assert.Equal("test-client", proto.Header.ClientName);
        Assert.Equal(42UL, proto.Header.BaseHeader.Block.BlockId);
        Assert.Equal(1024UL, proto.Len);
    }

    [Fact]
    public void PacketHeaderProto_CanBeConstructedAndSerialized()
    {
        var header = new PacketHeaderProto
        {
            OffsetInBlock = 0,
            Seqno = 1,
            LastPacketInBlock = false,
            DataLen = 512,
        };

        byte[] bytes = header.ToByteArray();
        Assert.NotEmpty(bytes);

        var parsed = PacketHeaderProto.Parser.ParseFrom(bytes);
        Assert.Equal(0L, parsed.OffsetInBlock);
        Assert.Equal(1L, parsed.Seqno);
        Assert.False(parsed.LastPacketInBlock);
        Assert.Equal(512, parsed.DataLen);
    }

    [Fact]
    public void BlockOpResponseProto_SuccessWithChecksumInfo()
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

        byte[] bytes = response.ToByteArray();
        var parsed = BlockOpResponseProto.Parser.ParseFrom(bytes);

        Assert.Equal(Status.Success, parsed.Status);
        Assert.Equal(ChecksumTypeProto.ChecksumCrc32C,
            parsed.ReadOpChecksumInfo.Checksum.Type);
        Assert.Equal(512U, parsed.ReadOpChecksumInfo.Checksum.BytesPerChecksum);
    }

    [Fact]
    public void ClientReadStatusProto_RoundTrip()
    {
        var status = new ClientReadStatusProto { Status = Status.ChecksumOk };

        byte[] bytes = status.ToByteArray();
        var parsed = ClientReadStatusProto.Parser.ParseFrom(bytes);

        Assert.Equal(Status.ChecksumOk, parsed.Status);
    }

    [Fact]
    public void ExtendedBlockProto_RoundTrip()
    {
        var block = new ExtendedBlockProto
        {
            PoolId = "BP-test-pool",
            BlockId = 12345678,
            GenerationStamp = 9999,
            NumBytes = 128 * 1024 * 1024,
        };

        byte[] bytes = block.ToByteArray();
        var parsed = ExtendedBlockProto.Parser.ParseFrom(bytes);

        Assert.Equal("BP-test-pool", parsed.PoolId);
        Assert.Equal(12345678UL, parsed.BlockId);
        Assert.Equal(9999UL, parsed.GenerationStamp);
        Assert.Equal(128UL * 1024 * 1024, parsed.NumBytes);
    }

    [Fact]
    public void DatanodeIDProto_RoundTrip()
    {
        var dn = new DatanodeIDProto
        {
            IpAddr = "10.0.0.1",
            HostName = "datanode1.cluster",
            DatanodeUuid = "uuid-abc-123",
            XferPort = 9866,
            InfoPort = 9864,
            IpcPort = 9867,
        };

        byte[] bytes = dn.ToByteArray();
        var parsed = DatanodeIDProto.Parser.ParseFrom(bytes);

        Assert.Equal("10.0.0.1", parsed.IpAddr);
        Assert.Equal(9866U, parsed.XferPort);
        Assert.Equal("uuid-abc-123", parsed.DatanodeUuid);
    }

    [Fact]
    public void TokenProto_RoundTrip()
    {
        var token = new TokenProto
        {
            Identifier = ByteString.CopyFromUtf8("id-bytes"),
            Password = ByteString.CopyFromUtf8("pw-bytes"),
            Kind = "HDFS_BLOCK_TOKEN",
            Service = "BP-test",
        };

        byte[] bytes = token.ToByteArray();
        var parsed = TokenProto.Parser.ParseFrom(bytes);

        Assert.Equal("HDFS_BLOCK_TOKEN", parsed.Kind);
        Assert.Equal("BP-test", parsed.Service);
    }

    [Fact]
    public void ChecksumTypeProto_AllValuesExist()
    {
        Assert.Equal(0, (int)ChecksumTypeProto.ChecksumNull);
        Assert.Equal(1, (int)ChecksumTypeProto.ChecksumCrc32);
        Assert.Equal(2, (int)ChecksumTypeProto.ChecksumCrc32C);
    }

    [Fact]
    public void Status_AllRequiredValuesExist()
    {
        Assert.Equal(0, (int)Status.Success);
        Assert.Equal(1, (int)Status.Error);
        Assert.Equal(2, (int)Status.ErrorChecksum);
        Assert.Equal(5, (int)Status.ErrorAccessToken);
        Assert.Equal(6, (int)Status.ChecksumOk);
    }

    [Fact]
    public void LocatedBlockProto_CanBeConstructed()
    {
        var located = new LocatedBlockProto
        {
            B = new ExtendedBlockProto
            {
                PoolId = "BP-1",
                BlockId = 100,
                GenerationStamp = 1,
            },
            Offset = 0,
            Corrupt = false,
            BlockToken = new TokenProto
            {
                Identifier = ByteString.Empty,
                Password = ByteString.Empty,
                Kind = "",
                Service = "",
            },
        };
        located.Locs.Add(new DatanodeInfoProto
        {
            Id = new DatanodeIDProto
            {
                IpAddr = "10.0.0.1",
                HostName = "dn1",
                DatanodeUuid = "u1",
                XferPort = 9866,
                InfoPort = 9864,
                IpcPort = 9867,
            },
        });

        Assert.Single(located.Locs);
        Assert.Equal(100UL, located.B.BlockId);
    }
}
