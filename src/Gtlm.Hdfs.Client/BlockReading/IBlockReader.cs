namespace Gtlm.Hdfs.Client.BlockReading;

/// <summary>
/// Interface for HDFS block readers.
/// Allows different implementations (remote, local short-circuit, etc.).
/// </summary>
public interface IBlockReader : IAsyncDisposable
{
    ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default);
    long Length { get; }
    long Position { get; }
    bool IsComplete { get; }
}
