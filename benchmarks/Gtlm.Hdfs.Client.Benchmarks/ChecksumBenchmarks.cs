namespace Gtlm.Hdfs.Client.Benchmarks;

using BenchmarkDotNet.Attributes;
using Gtlm.Hdfs.Client.Checksum;
using Gtlm.Hdfs.Client.Proto;

[MemoryDiagnoser]
public class ChecksumBenchmarks
{
    private byte[] _data512 = null!;
    private byte[] _data64K = null!;

    [GlobalSetup]
    public void Setup()
    {
        _data512 = new byte[512];
        _data64K = new byte[64 * 1024];
        Random.Shared.NextBytes(_data512);
        Random.Shared.NextBytes(_data64K);
    }

    [Benchmark]
    public uint Crc32C_512Bytes() => Crc32CChecksum.ComputeCrc32C(_data512);

    [Benchmark]
    public uint Crc32C_64KB() => Crc32CChecksum.ComputeCrc32C(_data64K);

    [Benchmark]
    public uint Crc32_IEEE_512Bytes() => Crc32Checksum.ComputeIeeeCrc32(_data512);

    [Benchmark]
    public uint Crc32_IEEE_64KB() => Crc32Checksum.ComputeIeeeCrc32(_data64K);
}
