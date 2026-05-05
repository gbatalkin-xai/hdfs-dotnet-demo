namespace Gtlm.Hdfs.Client.Tests;

using System.Buffers.Binary;
using Google.Protobuf;
using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Net;
using Gtlm.Hdfs.Client.Protocol;
using Gtlm.Hdfs.Client.Proto;

public class DataTransferSenderTests
{
    private static DatanodeInfo TestDn => new()
    {
        IpAddress = "127.0.0.1",
        HostName = "test-dn",
        DatanodeUuid = "test-uuid",
        XferPort = 9866,
    };

    private static (Peer peer, MemoryStream output) CreateTestPeer()
    {
        var output = new MemoryStream();
        var peer = Peer.CreateForTest(new MemoryStream(), output, TestDn);
        return (peer, output);
    }

    [Fact]
    public async Task SendReadBlock_WritesCorrectVersionAndOpCode()
    {
        var (peer, output) = CreateTestPeer();

        await DataTransferSender.SendReadBlockAsync(
            peer,
            new ExtendedBlock("BP-1", 42, 100),
            BlockToken.Empty,
            "test-client",
            offset: 0,
            length: 1024,
            sendChecksums: true);

        output.Position = 0;
        var bytes = output.ToArray();

        // First 2 bytes: version 28 big-endian
        Assert.True(bytes.Length >= 3);
        short version = BinaryPrimitives.ReadInt16BigEndian(bytes.AsSpan(0, 2));
        Assert.Equal(28, version);

        // Third byte: OP_READ_BLOCK = 81
        Assert.Equal(81, bytes[2]);

        await peer.DisposeAsync();
    }

    [Fact]
    public async Task SendReadBlock_ProtobufIsVarintPrefixed()
    {
        var (peer, output) = CreateTestPeer();

        await DataTransferSender.SendReadBlockAsync(
            peer,
            new ExtendedBlock("BP-1", 42, 100),
            BlockToken.Empty,
            "test-client",
            offset: 0,
            length: 1024,
            sendChecksums: true);

        output.Position = 0;
        var bytes = output.ToArray();

        // Skip the 3-byte header (version + opcode)
        var protoStream = new MemoryStream(bytes, 3, bytes.Length - 3);

        // ParseDelimitedFrom reads the varint length prefix then the message
        var parsed = OpReadBlockProto.Parser.ParseDelimitedFrom(protoStream);

        Assert.NotNull(parsed);
        Assert.Equal("test-client", parsed.Header.ClientName);

        await peer.DisposeAsync();
    }

    [Fact]
    public async Task SendReadBlock_FieldsMatchInput()
    {
        var (peer, output) = CreateTestPeer();
        var block = new ExtendedBlock("BP-test-pool", 12345, 9999, 128 * 1024);
        var token = new BlockToken
        {
            Identifier = [1, 2, 3],
            Password = [4, 5, 6],
            Kind = "HDFS_BLOCK_TOKEN",
            Service = "BP-test-pool",
        };

        await DataTransferSender.SendReadBlockAsync(
            peer, block, token, "my-client",
            offset: 512,
            length: 2048,
            sendChecksums: false);

        output.Position = 0;
        var bytes = output.ToArray();
        var protoStream = new MemoryStream(bytes, 3, bytes.Length - 3);
        var parsed = OpReadBlockProto.Parser.ParseDelimitedFrom(protoStream);

        // Block fields
        Assert.Equal("BP-test-pool", parsed.Header.BaseHeader.Block.PoolId);
        Assert.Equal(12345UL, parsed.Header.BaseHeader.Block.BlockId);
        Assert.Equal(9999UL, parsed.Header.BaseHeader.Block.GenerationStamp);

        // Token fields
        Assert.Equal(new byte[] { 1, 2, 3 }, parsed.Header.BaseHeader.Token.Identifier.ToByteArray());
        Assert.Equal(new byte[] { 4, 5, 6 }, parsed.Header.BaseHeader.Token.Password.ToByteArray());
        Assert.Equal("HDFS_BLOCK_TOKEN", parsed.Header.BaseHeader.Token.Kind);

        // Client name
        Assert.Equal("my-client", parsed.Header.ClientName);

        // Offset, length, checksums
        Assert.Equal(512UL, parsed.Offset);
        Assert.Equal(2048UL, parsed.Len);
        Assert.False(parsed.SendChecksums);

        await peer.DisposeAsync();
    }

    [Fact]
    public async Task SendReadBlock_EmptyToken_SerializesCorrectly()
    {
        var (peer, output) = CreateTestPeer();

        await DataTransferSender.SendReadBlockAsync(
            peer,
            new ExtendedBlock("BP-1", 1, 1),
            BlockToken.Empty,
            "client",
            offset: 0,
            length: 100,
            sendChecksums: true);

        output.Position = 0;
        var bytes = output.ToArray();
        var protoStream = new MemoryStream(bytes, 3, bytes.Length - 3);
        var parsed = OpReadBlockProto.Parser.ParseDelimitedFrom(protoStream);

        Assert.Empty(parsed.Header.BaseHeader.Token.Identifier);
        Assert.Empty(parsed.Header.BaseHeader.Token.Password);

        await peer.DisposeAsync();
    }

    [Fact]
    public async Task SendClientReadStatus_ChecksumOk()
    {
        var (peer, output) = CreateTestPeer();

        await DataTransferSender.SendClientReadStatusAsync(peer, Status.ChecksumOk);

        output.Position = 0;
        var parsed = ClientReadStatusProto.Parser.ParseDelimitedFrom(output);

        Assert.Equal(Status.ChecksumOk, parsed.Status);

        await peer.DisposeAsync();
    }

    [Fact]
    public async Task SendClientReadStatus_Error()
    {
        var (peer, output) = CreateTestPeer();

        await DataTransferSender.SendClientReadStatusAsync(peer, Status.Error);

        output.Position = 0;
        var parsed = ClientReadStatusProto.Parser.ParseDelimitedFrom(output);

        Assert.Equal(Status.Error, parsed.Status);

        await peer.DisposeAsync();
    }

    [Fact]
    public async Task SendReadBlock_WireFormat_StartsWithCorrectBytes()
    {
        var (peer, output) = CreateTestPeer();

        await DataTransferSender.SendReadBlockAsync(
            peer,
            new ExtendedBlock("BP-1", 1, 1),
            BlockToken.Empty,
            "c",
            offset: 0,
            length: 1,
            sendChecksums: true);

        var bytes = output.ToArray();

        // Byte 0-1: 0x00 0x1C (28 big-endian)
        Assert.Equal(0x00, bytes[0]);
        Assert.Equal(0x1C, bytes[1]);

        // Byte 2: 0x51 (81 decimal = OP_READ_BLOCK)
        Assert.Equal(0x51, bytes[2]);

        // Byte 3: varint length (should be > 0)
        Assert.True(bytes[3] > 0);

        await peer.DisposeAsync();
    }
}
