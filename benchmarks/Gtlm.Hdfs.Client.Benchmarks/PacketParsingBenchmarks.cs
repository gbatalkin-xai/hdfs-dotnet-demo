namespace Gtlm.Hdfs.Client.Benchmarks;

using System.Buffers.Binary;
using System.IO.Pipelines;
using BenchmarkDotNet.Attributes;
using Google.Protobuf;
using Gtlm.Hdfs.Client.Checksum;
using Gtlm.Hdfs.Client.Protocol;
using Gtlm.Hdfs.Client.Proto;

[MemoryDiagnoser]
public class PacketParsingBenchmarks
{
    private byte[] _packet1KB = null!;
    private byte[] _packet64KB = null!;

    [GlobalSetup]
    public void Setup()
    {
        _packet1KB = BuildPacket(1024);
        _packet64KB = BuildPacket(64 * 1024);
    }

    [Benchmark]
    public async Task ParsePacket_1KB()
    {
        var pipe = new Pipe(new PipeOptions(pauseWriterThreshold: long.MaxValue));
        await pipe.Writer.WriteAsync(_packet1KB);
        await pipe.Writer.CompleteAsync();

        var checksum = DataChecksum.Create(ChecksumTypeProto.ChecksumCrc32C, 512);
        var reader = new PacketReader(pipe.Reader, checksum);
        _ = await reader.ReadNextPacketAsync();
    }

    [Benchmark]
    public async Task ParsePacket_64KB()
    {
        var pipe = new Pipe(new PipeOptions(pauseWriterThreshold: long.MaxValue));
        await pipe.Writer.WriteAsync(_packet64KB);
        await pipe.Writer.CompleteAsync();

        var checksum = DataChecksum.Create(ChecksumTypeProto.ChecksumCrc32C, 512);
        var reader = new PacketReader(pipe.Reader, checksum);
        _ = await reader.ReadNextPacketAsync();
    }

    private static byte[] BuildPacket(int dataSize)
    {
        var data = new byte[dataSize];
        Random.Shared.NextBytes(data);

        var header = new PacketHeaderProto
        {
            OffsetInBlock = 0,
            Seqno = 0,
            LastPacketInBlock = false,
            DataLen = dataSize,
        };
        var headerBytes = header.ToByteArray();

        int bpc = 512;
        int numChunks = (dataSize + bpc - 1) / bpc;
        var checksums = new byte[numChunks * 4];
        for (int i = 0; i < numChunks; i++)
        {
            int start = i * bpc;
            int end = Math.Min(start + bpc, dataSize);
            uint crc = Crc32CChecksum.ComputeCrc32C(data.AsSpan(start, end - start));
            BinaryPrimitives.WriteUInt32BigEndian(checksums.AsSpan(i * 4), crc);
        }

        int packetLen = 2 + headerBytes.Length + checksums.Length + data.Length;
        var packet = new byte[4 + packetLen];
        int pos = 0;
        BinaryPrimitives.WriteInt32BigEndian(packet.AsSpan(pos), packetLen); pos += 4;
        BinaryPrimitives.WriteInt16BigEndian(packet.AsSpan(pos), (short)headerBytes.Length); pos += 2;
        headerBytes.CopyTo(packet, pos); pos += headerBytes.Length;
        checksums.CopyTo(packet, pos); pos += checksums.Length;
        data.CopyTo(packet, pos);

        return packet;
    }
}
