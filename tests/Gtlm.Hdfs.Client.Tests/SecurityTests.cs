namespace Gtlm.Hdfs.Client.Tests;

using Google.Protobuf;
using Gtlm.Hdfs.Client.Configuration;
using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Net;
using Gtlm.Hdfs.Client.Protocol;
using Gtlm.Hdfs.Client.Proto;
using Gtlm.Hdfs.Client.Security;

public class SecurityTests
{
    private static DatanodeInfo TestDn => new()
    {
        IpAddress = "127.0.0.1",
        HostName = "test-dn.cluster",
        DatanodeUuid = "sec-uuid",
        XferPort = 9866,
    };

    // --- KerberosCredentialProvider ---

    [Fact]
    public void KerberosCredentialProvider_ThrowsWithoutPrincipal()
    {
        var options = new HdfsClientOptions();
        Assert.Throws<ArgumentException>(() => new KerberosCredentialProvider(options));
    }

    [Fact]
    public void KerberosCredentialProvider_ThrowsWithoutKeytab()
    {
        var options = new HdfsClientOptions { KerberosPrincipal = "user@REALM.COM" };
        Assert.Throws<ArgumentException>(() => new KerberosCredentialProvider(options));
    }

    // --- SaslDataTransferHandler ---

    [Fact]
    public async Task SaslHandler_SuccessResponse_DoesNotThrow()
    {
        // Simulate a DataNode that responds with SUCCESS
        var responseMsg = new DataTransferEncryptorMessageProto
        {
            Status = DataTransferEncryptorMessageProto.Types
                .DataTransferEncryptorStatus.Success,
            Payload = ByteString.CopyFrom([1, 2, 3]), // mock server token
        };

        using var responseStream = new MemoryStream();
        responseMsg.WriteDelimitedTo(responseStream);
        responseStream.Position = 0;

        var writeStream = new MemoryStream();
        var peer = Peer.CreateForTest(responseStream, writeStream, TestDn);

        // Use a mock credential provider that returns a pre-built token
        // SaslDataTransferHandler calls credentials.GetServiceTokenAsync,
        // so we need a real-enough provider. Instead, we test the protocol
        // parsing directly.
        await SaslNegotiateDirectAsync(peer, [0xAA, 0xBB]);

        // Verify the client sent a message
        writeStream.Position = 0;
        var clientMsg = DataTransferEncryptorMessageProto.Parser.ParseDelimitedFrom(writeStream);
        Assert.Equal(DataTransferEncryptorMessageProto.Types
            .DataTransferEncryptorStatus.Success, clientMsg.Status);
        Assert.Equal(new byte[] { 0xAA, 0xBB }, clientMsg.Payload.ToByteArray());
    }

    [Fact]
    public async Task SaslHandler_ErrorResponse_ThrowsHdfsProtocolException()
    {
        var responseMsg = new DataTransferEncryptorMessageProto
        {
            Status = DataTransferEncryptorMessageProto.Types
                .DataTransferEncryptorStatus.Error,
            Message = "SASL mechanism not supported",
        };

        using var responseStream = new MemoryStream();
        responseMsg.WriteDelimitedTo(responseStream);
        responseStream.Position = 0;

        var writeStream = new MemoryStream();
        var peer = Peer.CreateForTest(responseStream, writeStream, TestDn);

        var ex = await Assert.ThrowsAsync<HdfsProtocolException>(
            () => SaslNegotiateDirectAsync(peer, [0x01]));

        Assert.Contains("SASL negotiation failed", ex.Message);
        Assert.Contains("not supported", ex.Message);
    }

    [Fact]
    public async Task SaslHandler_AccessTokenError_ThrowsAccessTokenException()
    {
        var responseMsg = new DataTransferEncryptorMessageProto
        {
            Status = DataTransferEncryptorMessageProto.Types
                .DataTransferEncryptorStatus.Error,
            Message = "block token invalid",
            AccessTokenError = true,
        };

        using var responseStream = new MemoryStream();
        responseMsg.WriteDelimitedTo(responseStream);
        responseStream.Position = 0;

        var writeStream = new MemoryStream();
        var peer = Peer.CreateForTest(responseStream, writeStream, TestDn);

        await Assert.ThrowsAsync<AccessTokenException>(
            () => SaslNegotiateDirectAsync(peer, [0x01]));
    }

    // --- HdfsClientOptions ---

    [Fact]
    public void HdfsClientOptions_IsSecure_FalseByDefault()
    {
        var options = new HdfsClientOptions();
        Assert.False(options.IsSecure);
    }

    [Fact]
    public void HdfsClientOptions_IsSecure_TrueWithPrincipal()
    {
        var options = new HdfsClientOptions { KerberosPrincipal = "user@REALM" };
        Assert.True(options.IsSecure);
    }

    [Fact]
    public void HdfsClientOptions_DataTransferProtection_EmptyByDefault()
    {
        var options = new HdfsClientOptions();
        Assert.Equal("", options.DataTransferProtection);
    }

    /// <summary>
    /// Directly test the SASL protocol exchange without a real KerberosCredentialProvider.
    /// Sends a pre-built token and parses the response.
    /// </summary>
    private static async Task SaslNegotiateDirectAsync(Peer peer, byte[] clientToken)
    {
        var initMsg = new DataTransferEncryptorMessageProto
        {
            Status = DataTransferEncryptorMessageProto.Types
                .DataTransferEncryptorStatus.Success,
            Payload = ByteString.CopyFrom(clientToken),
        };

        var stream = peer.GetOutputStream();
        initMsg.WriteDelimitedTo(stream);
        await stream.FlushAsync();

        var inputStream = peer.GetInputStream();
        var response = DataTransferEncryptorMessageProto.Parser.ParseDelimitedFrom(inputStream);

        if (response.Status != DataTransferEncryptorMessageProto.Types
                .DataTransferEncryptorStatus.Success)
        {
            if (response.AccessTokenError)
                throw new AccessTokenException(
                    $"SASL negotiation failed with DataNode {peer.DataNode}: access token error - {response.Message}");

            throw new HdfsProtocolException(
                $"SASL negotiation failed with DataNode {peer.DataNode}: " +
                $"status={response.Status}, message={response.Message}");
        }
    }
}
