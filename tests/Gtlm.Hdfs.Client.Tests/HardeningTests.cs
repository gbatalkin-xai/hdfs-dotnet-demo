namespace Gtlm.Hdfs.Client.Tests;

using System.Diagnostics.Metrics;
using Gtlm.Hdfs.Client.Configuration;
using Gtlm.Hdfs.Client.Diagnostics;
using Gtlm.Hdfs.Client.Rpc;

public class HaNameNodeRpcClientTests
{
    [Fact]
    public void Constructor_NoNameNodes_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            new HaNameNodeRpcClient([], new HdfsClientOptions()));
    }

    [Fact]
    public async Task Constructor_WithNameNodes_SetsCount()
    {
        var nameNodes = new List<(string, int)> { ("nn1", 8020), ("nn2", 8020) };
        await using var ha = new HaNameNodeRpcClient(nameNodes, new HdfsClientOptions());

        Assert.Equal(2, ha.Count);
    }

    [Fact]
    public async Task DisposeAsync_Idempotent()
    {
        var nameNodes = new List<(string, int)> { ("nn1", 8020) };
        var ha = new HaNameNodeRpcClient(nameNodes, new HdfsClientOptions());

        await ha.DisposeAsync();
        await ha.DisposeAsync(); // no throw
    }
}

public class HdfsMetricsTests
{
    [Fact]
    public void Meter_HasCorrectName()
    {
        Assert.Equal("Gtlm.Hdfs.Client", HdfsMetrics.Meter.Name);
    }

    [Fact]
    public void AllInstruments_AreNotNull()
    {
        Assert.NotNull(HdfsMetrics.BytesRead);
        Assert.NotNull(HdfsMetrics.BlockReads);
        Assert.NotNull(HdfsMetrics.BlockReadFailovers);
        Assert.NotNull(HdfsMetrics.ChecksumErrors);
        Assert.NotNull(HdfsMetrics.CachedPeers);
        Assert.NotNull(HdfsMetrics.BlockReadDuration);
        Assert.NotNull(HdfsMetrics.NamenodeRpcDuration);
        Assert.NotNull(HdfsMetrics.PeerConnections);
        Assert.NotNull(HdfsMetrics.PeerCacheHits);
    }

    [Fact]
    public void Counters_CanBeIncremented()
    {
        // Verify counters don't throw when no listener is attached
        HdfsMetrics.BytesRead.Add(1024);
        HdfsMetrics.BlockReads.Add(1);
        HdfsMetrics.ChecksumErrors.Add(0);
    }

    [Fact]
    public void Histogram_CanRecordValues()
    {
        HdfsMetrics.BlockReadDuration.Record(42.5);
        HdfsMetrics.NamenodeRpcDuration.Record(1.2);
    }

    [Fact]
    public void UpDownCounter_CanBeUpdated()
    {
        HdfsMetrics.CachedPeers.Add(1);
        HdfsMetrics.CachedPeers.Add(-1);
    }
}

public class HadoopConfigParserTests
{
    [Fact]
    public void ParseFile_NonexistentFile_ReturnsEmpty()
    {
        var result = HadoopConfigParser.ParseFile("/nonexistent/path/core-site.xml");
        Assert.Empty(result);
    }

    [Fact]
    public void ParseFile_ValidXml_ExtractsProperties()
    {
        var xml = """
            <?xml version="1.0"?>
            <configuration>
              <property>
                <name>fs.defaultFS</name>
                <value>hdfs://namenode:8020</value>
              </property>
              <property>
                <name>dfs.replication</name>
                <value>3</value>
              </property>
            </configuration>
            """;

        var path = Path.GetTempFileName();
        try
        {
            File.WriteAllText(path, xml);
            var result = HadoopConfigParser.ParseFile(path);

            Assert.Equal(2, result.Count);
            Assert.Equal("hdfs://namenode:8020", result["fs.defaultFS"]);
            Assert.Equal("3", result["dfs.replication"]);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void ParseFile_EmptyProperties_Skipped()
    {
        var xml = """
            <?xml version="1.0"?>
            <configuration>
              <property>
                <name></name>
                <value>ignored</value>
              </property>
              <property>
                <name>valid.key</name>
                <value>valid.value</value>
              </property>
            </configuration>
            """;

        var path = Path.GetTempFileName();
        try
        {
            File.WriteAllText(path, xml);
            var result = HadoopConfigParser.ParseFile(path);

            Assert.Single(result);
            Assert.Equal("valid.value", result["valid.key"]);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void FromHadoopConfig_ExtractsNameNodeFromDefaultFS()
    {
        var xml = """
            <?xml version="1.0"?>
            <configuration>
              <property>
                <name>fs.defaultFS</name>
                <value>hdfs://my-namenode:9000</value>
              </property>
            </configuration>
            """;

        var path = Path.GetTempFileName();
        try
        {
            File.WriteAllText(path, xml);
            var options = HadoopConfigParser.FromHadoopConfig(coreSitePath: path);

            Assert.Equal("my-namenode", options.NameNodeHost);
            Assert.Equal(9000, options.NameNodePort);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void FromHadoopConfig_ExtractsHdfsSettings()
    {
        var xml = """
            <?xml version="1.0"?>
            <configuration>
              <property>
                <name>dfs.client.read.checksum</name>
                <value>false</value>
              </property>
              <property>
                <name>dfs.client.block.reader.remote.buffer.size</name>
                <value>1048576</value>
              </property>
              <property>
                <name>dfs.data.transfer.protection</name>
                <value>authentication</value>
              </property>
              <property>
                <name>dfs.encrypt.data.transfer</name>
                <value>true</value>
              </property>
              <property>
                <name>dfs.domain.socket.path</name>
                <value>/var/run/hdfs/dn</value>
              </property>
            </configuration>
            """;

        var path = Path.GetTempFileName();
        try
        {
            File.WriteAllText(path, xml);
            var options = HadoopConfigParser.FromHadoopConfig(hdfsSitePath: path);

            Assert.False(options.VerifyChecksum);
            Assert.Equal(1048576, options.RemoteBufferSize);
            Assert.Equal("authentication", options.DataTransferProtection);
            Assert.True(options.EncryptDataTransfer);
            Assert.Equal("/var/run/hdfs/dn", options.DomainSocketPath);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void FromHadoopConfig_BothFiles_HdfsSiteOverrides()
    {
        var coreSite = """
            <?xml version="1.0"?>
            <configuration>
              <property>
                <name>fs.defaultFS</name>
                <value>hdfs://nn1:8020</value>
              </property>
            </configuration>
            """;

        var hdfsSite = """
            <?xml version="1.0"?>
            <configuration>
              <property>
                <name>dfs.replication</name>
                <value>3</value>
              </property>
            </configuration>
            """;

        var corePath = Path.GetTempFileName();
        var hdfsPath = Path.GetTempFileName();
        try
        {
            File.WriteAllText(corePath, coreSite);
            File.WriteAllText(hdfsPath, hdfsSite);
            var options = HadoopConfigParser.FromHadoopConfig(corePath, hdfsPath);

            Assert.Equal("nn1", options.NameNodeHost);
        }
        finally
        {
            File.Delete(corePath);
            File.Delete(hdfsPath);
        }
    }

    [Fact]
    public void ExtractHaNameNodes_ValidConfig()
    {
        var props = new Dictionary<string, string>
        {
            ["dfs.nameservices"] = "mycluster",
            ["dfs.ha.namenodes.mycluster"] = "nn1,nn2",
            ["dfs.namenode.rpc-address.mycluster.nn1"] = "namenode1:8020",
            ["dfs.namenode.rpc-address.mycluster.nn2"] = "namenode2:8020",
        };

        var result = HadoopConfigParser.ExtractHaNameNodes(props);

        Assert.Equal(2, result.Count);
        Assert.Equal("namenode1", result[0].host);
        Assert.Equal(8020, result[0].port);
        Assert.Equal("namenode2", result[1].host);
        Assert.Equal(8020, result[1].port);
    }

    [Fact]
    public void ExtractHaNameNodes_NoNameservices_ReturnsEmpty()
    {
        var props = new Dictionary<string, string>();
        var result = HadoopConfigParser.ExtractHaNameNodes(props);
        Assert.Empty(result);
    }

    [Fact]
    public void ExtractHaNameNodes_CustomPort()
    {
        var props = new Dictionary<string, string>
        {
            ["dfs.nameservices"] = "ns1",
            ["dfs.ha.namenodes.ns1"] = "nn1",
            ["dfs.namenode.rpc-address.ns1.nn1"] = "my-nn:9000",
        };

        var result = HadoopConfigParser.ExtractHaNameNodes(props);

        Assert.Single(result);
        Assert.Equal("my-nn", result[0].host);
        Assert.Equal(9000, result[0].port);
    }
}
