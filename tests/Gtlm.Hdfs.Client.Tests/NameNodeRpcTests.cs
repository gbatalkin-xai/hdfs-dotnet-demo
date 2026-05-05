namespace Gtlm.Hdfs.Client.Tests;

using System.Buffers.Binary;
using Google.Protobuf;
using Gtlm.Hdfs.Client.Configuration;
using Gtlm.Hdfs.Client.Proto;
using Gtlm.Hdfs.Client.Rpc;

/// <summary>
/// Tests for the NameNode RPC protocol serialization and response parsing.
/// These test the wire format without a live NameNode connection.
/// </summary>
public class NameNodeRpcTests
{
    [Fact]
    public void RpcRequestHeader_SerializesCorrectly()
    {
        var header = new RpcRequestHeaderProto
        {
            RpcKind = RpcKindProto.RpcProtocolBuffer,
            RpcOp = RpcRequestHeaderProto.Types.OperationProto.RpcFinalPacket,
            CallId = 0,
            ClientId = ByteString.CopyFrom(Guid.NewGuid().ToByteArray()),
        };

        var bytes = header.ToByteArray();
        var parsed = RpcRequestHeaderProto.Parser.ParseFrom(bytes);

        Assert.Equal(RpcKindProto.RpcProtocolBuffer, parsed.RpcKind);
        Assert.Equal(0, parsed.CallId);
    }

    [Fact]
    public void IpcConnectionContext_SerializesCorrectly()
    {
        var context = new IpcConnectionContextProto
        {
            UserInfo = new UserInformationProto
            {
                EffectiveUser = "test-user",
            },
            Protocol = "org.apache.hadoop.hdfs.protocol.ClientProtocol",
        };

        var bytes = context.ToByteArray();
        var parsed = IpcConnectionContextProto.Parser.ParseFrom(bytes);

        Assert.Equal("test-user", parsed.UserInfo.EffectiveUser);
        Assert.Equal("org.apache.hadoop.hdfs.protocol.ClientProtocol", parsed.Protocol);
    }

    [Fact]
    public void RequestHeaderProto_SerializesCorrectly()
    {
        var header = new RequestHeaderProto
        {
            MethodName = "getBlockLocations",
            DeclaringClassProtocolName = "org.apache.hadoop.hdfs.protocol.ClientProtocol",
            ClientProtocolVersion = 1,
        };

        var bytes = header.ToByteArray();
        var parsed = RequestHeaderProto.Parser.ParseFrom(bytes);

        Assert.Equal("getBlockLocations", parsed.MethodName);
    }

    [Fact]
    public void GetBlockLocationsRequest_RoundTrip()
    {
        var request = new GetBlockLocationsRequestProto
        {
            Src = "/data/input.parquet",
            Offset = 0,
            Length = 128 * 1024 * 1024,
        };

        using var ms = new MemoryStream();
        request.WriteDelimitedTo(ms);
        ms.Position = 0;
        var parsed = GetBlockLocationsRequestProto.Parser.ParseDelimitedFrom(ms);

        Assert.Equal("/data/input.parquet", parsed.Src);
        Assert.Equal(0UL, parsed.Offset);
        Assert.Equal(128UL * 1024 * 1024, parsed.Length);
    }

    [Fact]
    public void GetBlockLocationsResponse_WithBlocks_Parses()
    {
        var response = new GetBlockLocationsResponseProto
        {
            Locations = new LocatedBlocksProto
            {
                FileLength = 1024,
                UnderConstruction = false,
                IsLastBlockComplete = true,
            },
        };
        response.Locations.Blocks.Add(new LocatedBlockProto
        {
            B = new ExtendedBlockProto
            {
                PoolId = "BP-1",
                BlockId = 100,
                GenerationStamp = 50,
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
        });

        using var ms = new MemoryStream();
        response.WriteDelimitedTo(ms);
        ms.Position = 0;
        var parsed = GetBlockLocationsResponseProto.Parser.ParseDelimitedFrom(ms);

        Assert.Single(parsed.Locations.Blocks);
        Assert.Equal(100UL, parsed.Locations.Blocks[0].B.BlockId);
    }

    [Fact]
    public void GetFileInfoResponse_WithFile_Parses()
    {
        var response = new GetFileInfoResponseProto
        {
            Fs = new HdfsFileStatusProto
            {
                FileType = HdfsFileStatusProto.Types.FileType.IsFile,
                Path = ByteString.CopyFromUtf8("test.dat"),
                Length = 2048,
                Permission = new FsPermissionProto { Perm = 0x1A4 },
                Owner = "hdfs",
                Group = "supergroup",
                ModificationTime = 1700000000000,
                AccessTime = 1700000000000,
                BlockReplication = 3,
                Blocksize = 128 * 1024 * 1024,
            },
        };

        using var ms = new MemoryStream();
        response.WriteDelimitedTo(ms);
        ms.Position = 0;
        var parsed = GetFileInfoResponseProto.Parser.ParseDelimitedFrom(ms);

        Assert.Equal(HdfsFileStatusProto.Types.FileType.IsFile, parsed.Fs.FileType);
        Assert.Equal(2048UL, parsed.Fs.Length);
        Assert.Equal("hdfs", parsed.Fs.Owner);
    }

    [Fact]
    public void GetFileInfoResponse_NotFound_HasNullFs()
    {
        var response = new GetFileInfoResponseProto(); // no Fs field

        using var ms = new MemoryStream();
        response.WriteDelimitedTo(ms);
        ms.Position = 0;
        var parsed = GetFileInfoResponseProto.Parser.ParseDelimitedFrom(ms);

        Assert.Null(parsed.Fs);
    }

    [Fact]
    public void RpcResponseHeader_Success_Parses()
    {
        var header = new RpcResponseHeaderProto
        {
            CallId = 5,
            Status = RpcResponseHeaderProto.Types.RpcStatusProto.Success,
        };

        using var ms = new MemoryStream();
        header.WriteDelimitedTo(ms);
        ms.Position = 0;
        var parsed = RpcResponseHeaderProto.Parser.ParseDelimitedFrom(ms);

        Assert.Equal(5U, parsed.CallId);
        Assert.Equal(RpcResponseHeaderProto.Types.RpcStatusProto.Success, parsed.Status);
    }

    [Fact]
    public void RpcResponseHeader_Error_Parses()
    {
        var header = new RpcResponseHeaderProto
        {
            CallId = 3,
            Status = RpcResponseHeaderProto.Types.RpcStatusProto.Error,
            ExceptionClassName = "java.io.FileNotFoundException",
            ErrorMsg = "File does not exist: /nonexistent",
        };

        using var ms = new MemoryStream();
        header.WriteDelimitedTo(ms);
        ms.Position = 0;
        var parsed = RpcResponseHeaderProto.Parser.ParseDelimitedFrom(ms);

        Assert.Equal(RpcResponseHeaderProto.Types.RpcStatusProto.Error, parsed.Status);
        Assert.Contains("FileNotFoundException", parsed.ExceptionClassName);
        Assert.Contains("/nonexistent", parsed.ErrorMsg);
    }

    [Fact]
    public void FullRpcFrame_CanBeConstructedAndParsed()
    {
        // Simulate a complete RPC response frame as NameNode would send it
        var responseHeader = new RpcResponseHeaderProto
        {
            CallId = 0,
            Status = RpcResponseHeaderProto.Types.RpcStatusProto.Success,
        };

        var responseBody = new GetBlockLocationsResponseProto
        {
            Locations = new LocatedBlocksProto
            {
                FileLength = 4096,
                UnderConstruction = false,
                IsLastBlockComplete = true,
            },
        };

        // Build frame: [4B length][header delimited][body delimited]
        using var frameContent = new MemoryStream();
        responseHeader.WriteDelimitedTo(frameContent);
        responseBody.WriteDelimitedTo(frameContent);

        var frameBytes = frameContent.ToArray();
        var fullFrame = new byte[4 + frameBytes.Length];
        BinaryPrimitives.WriteInt32BigEndian(fullFrame, frameBytes.Length);
        frameBytes.CopyTo(fullFrame, 4);

        // Parse it back
        using var readStream = new MemoryStream(fullFrame);
        var lenBuf = new byte[4];
        readStream.ReadExactly(lenBuf);
        int frameLen = BinaryPrimitives.ReadInt32BigEndian(lenBuf);

        var frame = new byte[frameLen];
        readStream.ReadExactly(frame);

        using var ms = new MemoryStream(frame);
        var parsedHeader = RpcResponseHeaderProto.Parser.ParseDelimitedFrom(ms);
        Assert.Equal(RpcResponseHeaderProto.Types.RpcStatusProto.Success, parsedHeader.Status);

        var parsedBody = GetBlockLocationsResponseProto.Parser.ParseDelimitedFrom(ms);
        Assert.Equal(4096UL, parsedBody.Locations.FileLength);
    }

    [Fact]
    public async Task NameNodeRpcClient_CanBeConstructed()
    {
        var options = new HdfsClientOptions
        {
            NameNodeHost = "namenode.test",
            NameNodePort = 8020,
        };

        await using var client = new NameNodeRpcClient(options);
        // Just verify construction doesn't throw
        Assert.NotNull(client);
    }

    [Fact]
    public async Task NameNodeRpcClient_ConnectAsync_UnreachableHost_Throws()
    {
        var options = new HdfsClientOptions
        {
            NameNodeHost = "192.0.2.1",
            NameNodePort = 8020,
            ConnectTimeout = TimeSpan.FromMilliseconds(200),
        };

        await using var client = new NameNodeRpcClient(options);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => client.ConnectAsync());
    }

    [Fact]
    public void GetListingRequest_SerializesCorrectly()
    {
        var request = new GetListingRequestProto
        {
            Src = "/data",
            StartAfter = ByteString.Empty,
            NeedLocation = false,
        };

        var bytes = request.ToByteArray();
        var parsed = GetListingRequestProto.Parser.ParseFrom(bytes);

        Assert.Equal("/data", parsed.Src);
        Assert.False(parsed.NeedLocation);
    }
}
