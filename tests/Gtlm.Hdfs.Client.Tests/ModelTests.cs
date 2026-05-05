namespace Gtlm.Hdfs.Client.Tests;

using Google.Protobuf;
using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Proto;

public class ModelTests
{
    [Fact]
    public void ExtendedBlock_ToProto_RoundTrip()
    {
        var block = new ExtendedBlock("BP-pool-1", 42, 1000, 128 * 1024 * 1024);

        var proto = block.ToProto();
        var roundTripped = ExtendedBlock.FromProto(proto);

        Assert.Equal(block, roundTripped);
    }

    [Fact]
    public void ExtendedBlock_ValueEquality()
    {
        var a = new ExtendedBlock("BP-1", 10, 100);
        var b = new ExtendedBlock("BP-1", 10, 100);
        var c = new ExtendedBlock("BP-1", 11, 100);

        Assert.Equal(a, b);
        Assert.NotEqual(a, c);
    }

    [Fact]
    public void ExtendedBlock_DefaultNumBytes_IsZero()
    {
        var block = new ExtendedBlock("BP-1", 1, 1);
        Assert.Equal(0, block.NumBytes);
    }

    [Fact]
    public void DatanodeInfo_Equality_ByUuid()
    {
        var a = new DatanodeInfo
        {
            IpAddress = "10.0.0.1",
            HostName = "dn1",
            DatanodeUuid = "uuid-abc",
            XferPort = 9866,
        };
        var b = new DatanodeInfo
        {
            IpAddress = "10.0.0.2",  // different IP
            HostName = "dn1-alias",  // different hostname
            DatanodeUuid = "uuid-abc", // same UUID
            XferPort = 9867,         // different port
        };

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void DatanodeInfo_Equality_DifferentUuid_NotEqual()
    {
        var a = new DatanodeInfo
        {
            IpAddress = "10.0.0.1",
            HostName = "dn1",
            DatanodeUuid = "uuid-1",
            XferPort = 9866,
        };
        var b = new DatanodeInfo
        {
            IpAddress = "10.0.0.1",
            HostName = "dn1",
            DatanodeUuid = "uuid-2",
            XferPort = 9866,
        };

        Assert.NotEqual(a, b);
    }

    [Fact]
    public void DatanodeInfo_XferAddress()
    {
        var dn = new DatanodeInfo
        {
            IpAddress = "10.0.0.1",
            HostName = "dn1",
            DatanodeUuid = "u1",
            XferPort = 9866,
        };

        Assert.Equal("10.0.0.1:9866", dn.XferAddress);
    }

    [Fact]
    public void DatanodeInfo_FromDatanodeInfoProto()
    {
        var proto = new DatanodeInfoProto
        {
            Id = new DatanodeIDProto
            {
                IpAddr = "192.168.1.10",
                HostName = "datanode3.cluster",
                DatanodeUuid = "uuid-xyz-789",
                XferPort = 9866,
                InfoPort = 9864,
                IpcPort = 9867,
            },
            Capacity = 1000000,
        };

        var dn = DatanodeInfo.FromProto(proto);

        Assert.Equal("192.168.1.10", dn.IpAddress);
        Assert.Equal("datanode3.cluster", dn.HostName);
        Assert.Equal("uuid-xyz-789", dn.DatanodeUuid);
        Assert.Equal(9866, dn.XferPort);
        Assert.Equal(9864, dn.InfoPort);
        Assert.Equal(9867, dn.IpcPort);
    }

    [Fact]
    public void DatanodeInfo_FromDatanodeIDProto()
    {
        var proto = new DatanodeIDProto
        {
            IpAddr = "10.0.0.5",
            HostName = "dn5",
            DatanodeUuid = "uid-5",
            XferPort = 9866,
            InfoPort = 9864,
            IpcPort = 9867,
        };

        var dn = DatanodeInfo.FromProto(proto);

        Assert.Equal("10.0.0.5", dn.IpAddress);
        Assert.Equal("uid-5", dn.DatanodeUuid);
    }

    [Fact]
    public void BlockToken_Empty_IsValid()
    {
        var empty = BlockToken.Empty;

        Assert.Empty(empty.Identifier);
        Assert.Empty(empty.Password);
        Assert.Equal("", empty.Kind);
        Assert.Equal("", empty.Service);
    }

    [Fact]
    public void BlockToken_ToProto_RoundTrip()
    {
        var token = new BlockToken
        {
            Identifier = [1, 2, 3, 4],
            Password = [5, 6, 7, 8],
            Kind = "HDFS_BLOCK_TOKEN",
            Service = "BP-test-pool",
        };

        var proto = token.ToProto();
        var roundTripped = BlockToken.FromProto(proto);

        Assert.Equal(token.Identifier, roundTripped.Identifier);
        Assert.Equal(token.Password, roundTripped.Password);
        Assert.Equal(token.Kind, roundTripped.Kind);
        Assert.Equal(token.Service, roundTripped.Service);
    }

    [Fact]
    public void BlockToken_Empty_ToProto_RoundTrip()
    {
        var proto = BlockToken.Empty.ToProto();
        var roundTripped = BlockToken.FromProto(proto);

        Assert.Empty(roundTripped.Identifier);
        Assert.Empty(roundTripped.Password);
    }

    [Fact]
    public void LocatedBlock_FromProto()
    {
        var proto = new LocatedBlockProto
        {
            B = new ExtendedBlockProto
            {
                PoolId = "BP-pool",
                BlockId = 999,
                GenerationStamp = 50,
                NumBytes = 64 * 1024 * 1024,
            },
            Offset = 128 * 1024 * 1024,
            Corrupt = false,
            BlockToken = new TokenProto
            {
                Identifier = ByteString.Empty,
                Password = ByteString.Empty,
                Kind = "",
                Service = "",
            },
        };
        proto.Locs.Add(new DatanodeInfoProto
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
        proto.Locs.Add(new DatanodeInfoProto
        {
            Id = new DatanodeIDProto
            {
                IpAddr = "10.0.0.2",
                HostName = "dn2",
                DatanodeUuid = "u2",
                XferPort = 9866,
                InfoPort = 9864,
                IpcPort = 9867,
            },
        });

        var located = LocatedBlock.FromProto(proto);

        Assert.Equal("BP-pool", located.Block.PoolId);
        Assert.Equal(999, located.Block.BlockId);
        Assert.Equal(64L * 1024 * 1024, located.Block.NumBytes);
        Assert.Equal(128L * 1024 * 1024, located.Offset);
        Assert.Equal(2, located.Locations.Count);
        Assert.Equal("10.0.0.1", located.Locations[0].IpAddress);
        Assert.Equal("10.0.0.2", located.Locations[1].IpAddress);
    }

    [Fact]
    public void HdfsFileStatus_CanBeConstructed()
    {
        var status = new HdfsFileStatus
        {
            Path = "/data/input.parquet",
            Length = 1024 * 1024 * 100,
            IsDirectory = false,
            BlockSize = 128 * 1024 * 1024,
            Replication = 3,
            ModificationTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            AccessTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Owner = "hdfs",
            Group = "supergroup",
            Permission = 0x1A4, // 0644
        };

        Assert.Equal("/data/input.parquet", status.Path);
        Assert.False(status.IsDirectory);
        Assert.Equal(3, status.Replication);
    }

    [Fact]
    public void HdfsFileStatus_DefaultPermission_Is0777()
    {
        var status = new HdfsFileStatus
        {
            Path = "/tmp",
            Length = 0,
            IsDirectory = true,
            BlockSize = 0,
            Replication = 0,
            ModificationTime = 0,
            AccessTime = 0,
        };

        Assert.Equal(0x1FF, status.Permission);
    }
}
