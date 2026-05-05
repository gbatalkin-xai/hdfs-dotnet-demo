namespace Gtlm.Hdfs.Client.Configuration;

using System.Xml.Linq;

/// <summary>
/// Parses Hadoop XML configuration files (core-site.xml, hdfs-site.xml)
/// and populates HdfsClientOptions for drop-in compatibility.
/// </summary>
public static class HadoopConfigParser
{
    /// <summary>
    /// Parse a Hadoop configuration XML file into a key-value dictionary.
    /// </summary>
    public static Dictionary<string, string> ParseFile(string path)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);

        if (!File.Exists(path))
            return result;

        var doc = XDocument.Load(path);
        var properties = doc.Root?.Elements("property") ?? [];

        foreach (var prop in properties)
        {
            var name = prop.Element("name")?.Value;
            var value = prop.Element("value")?.Value;

            if (!string.IsNullOrEmpty(name) && value is not null)
                result[name] = value;
        }

        return result;
    }

    /// <summary>
    /// Create HdfsClientOptions from Hadoop configuration files.
    /// </summary>
    public static HdfsClientOptions FromHadoopConfig(
        string? coreSitePath = null,
        string? hdfsSitePath = null)
    {
        var props = new Dictionary<string, string>(StringComparer.Ordinal);

        if (coreSitePath is not null)
            foreach (var (k, v) in ParseFile(coreSitePath))
                props[k] = v;

        if (hdfsSitePath is not null)
            foreach (var (k, v) in ParseFile(hdfsSitePath))
                props[k] = v;

        var options = new HdfsClientOptions();

        // NameNode address from fs.defaultFS (e.g., "hdfs://namenode:8020")
        if (props.TryGetValue("fs.defaultFS", out var defaultFs) &&
            Uri.TryCreate(defaultFs, UriKind.Absolute, out var uri) &&
            uri.Scheme == "hdfs")
        {
            options.NameNodeHost = uri.Host;
            if (uri.Port > 0)
                options.NameNodePort = uri.Port;
        }

        // Checksum verification
        if (props.TryGetValue("dfs.client.read.checksum", out var readChecksum))
            options.VerifyChecksum = ParseBool(readChecksum, true);

        // Remote buffer size
        if (props.TryGetValue("dfs.client.block.reader.remote.buffer.size", out var bufSize)
            && int.TryParse(bufSize, out var bs))
            options.RemoteBufferSize = bs;

        // Data transfer protection
        if (props.TryGetValue("dfs.data.transfer.protection", out var protection))
            options.DataTransferProtection = protection;

        // Data transfer encryption
        if (props.TryGetValue("dfs.encrypt.data.transfer", out var encrypt))
            options.EncryptDataTransfer = ParseBool(encrypt, false);

        // Domain socket path
        if (props.TryGetValue("dfs.domain.socket.path", out var socketPath))
            options.DomainSocketPath = socketPath;

        return options;
    }

    /// <summary>
    /// Extract NameNode addresses for HA configurations.
    /// Returns the list of (host, port) pairs from dfs.nameservices configuration.
    /// </summary>
    public static IReadOnlyList<(string host, int port)> ExtractHaNameNodes(
        Dictionary<string, string> props)
    {
        if (!props.TryGetValue("dfs.nameservices", out var nameservice))
            return [];

        // First nameservice (may be comma-separated)
        var ns = nameservice.Split(',')[0].Trim();

        if (!props.TryGetValue($"dfs.ha.namenodes.{ns}", out var nnIds))
            return [];

        var result = new List<(string host, int port)>();

        foreach (var nnId in nnIds.Split(',').Select(s => s.Trim()))
        {
            var key = $"dfs.namenode.rpc-address.{ns}.{nnId}";
            if (props.TryGetValue(key, out var address))
            {
                var parts = address.Split(':');
                string host = parts[0];
                int port = parts.Length > 1 && int.TryParse(parts[1], out var p) ? p : 8020;
                result.Add((host, port));
            }
        }

        return result;
    }

    private static bool ParseBool(string value, bool defaultValue) =>
        bool.TryParse(value, out var b) ? b : defaultValue;
}
