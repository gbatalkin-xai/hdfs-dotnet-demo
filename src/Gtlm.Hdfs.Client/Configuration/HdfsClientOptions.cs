namespace Gtlm.Hdfs.Client.Configuration;

/// <summary>
/// Configuration options for the HDFS client.
/// </summary>
public sealed class HdfsClientOptions
{
    /// <summary>Buffer size for remote block reads (dfs.client.block.reader.remote.buffer.size).</summary>
    public int RemoteBufferSize { get; set; } = 512 * 1024;

    /// <summary>TCP connect timeout for DataNode connections.</summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>Socket read/write timeout.</summary>
    public TimeSpan ReadTimeout { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>Max cached peer connections per DataNode.</summary>
    public int PeerCacheMaxPerDataNode { get; set; } = 64;

    /// <summary>Idle timeout before cached peers are evicted.</summary>
    public TimeSpan PeerCacheIdleTimeout { get; set; } = TimeSpan.FromSeconds(300);

    /// <summary>Whether to verify data checksums on read.</summary>
    public bool VerifyChecksum { get; set; } = true;

    /// <summary>Client name sent to DataNodes and NameNode.</summary>
    public string ClientName { get; set; } = $"DotNetHdfsClient_{Environment.MachineName}";

    /// <summary>NameNode hostname or IP.</summary>
    public string NameNodeHost { get; set; } = "localhost";

    /// <summary>NameNode RPC port (default 8020).</summary>
    public int NameNodePort { get; set; } = 8020;

    /// <summary>Kerberos principal (e.g., "user@REALM.COM").</summary>
    public string? KerberosPrincipal { get; set; }

    /// <summary>Path to keytab file. If null, uses credential cache.</summary>
    public string? KeytabPath { get; set; }

    /// <summary>Whether to encrypt data transfer (dfs.encrypt.data.transfer).</summary>
    public bool EncryptDataTransfer { get; set; }

    /// <summary>
    /// Data transfer protection level (dfs.data.transfer.protection).
    /// Values: "" (none), "authentication", "integrity", "privacy".
    /// </summary>
    public string DataTransferProtection { get; set; } = "";

    /// <summary>
    /// Unix domain socket path for short-circuit local reads
    /// (dfs.domain.socket.path, e.g., "/var/run/hdfs-sockets/dn").
    /// Leave empty to disable short-circuit reads.
    /// </summary>
    public string? DomainSocketPath { get; set; }

    /// <summary>Maximum replica attempts per block read.</summary>
    public int MaxBlockReadRetries { get; set; } = 3;

    /// <summary>Duration to mark a DataNode as dead after failure.</summary>
    public TimeSpan DeadNodeExpiry { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>Whether Kerberos security is configured.</summary>
    public bool IsSecure => !string.IsNullOrEmpty(KerberosPrincipal);
}
