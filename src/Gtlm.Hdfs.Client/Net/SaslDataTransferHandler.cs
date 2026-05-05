namespace Gtlm.Hdfs.Client.Net;

using Google.Protobuf;
using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Protocol;
using Gtlm.Hdfs.Client.Proto;
using Gtlm.Hdfs.Client.Security;
using Microsoft.Extensions.Logging;

/// <summary>
/// Performs SASL negotiation on a DataNode data transfer connection.
/// Must be called after TCP connect, before sending OP_READ_BLOCK.
///
/// Uses the DataTransferEncryptorMessageProto exchange defined in datatransfer.proto.
/// </summary>
public static class SaslDataTransferHandler
{
    /// <summary>
    /// Perform SASL/GSSAPI negotiation with a DataNode.
    ///
    /// Protection levels (dfs.data.transfer.protection):
    ///   "authentication" - verify identity only
    ///   "integrity"      - verify identity + message integrity (HMAC)
    ///   "privacy"        - verify identity + encrypt all data
    /// </summary>
    public static async Task NegotiateAsync(
        Peer peer,
        KerberosCredentialProvider credentials,
        DatanodeInfo dataNode,
        string protectionLevel,
        ILogger? logger = null,
        CancellationToken ct = default)
    {
        // Build service principal for the DataNode
        string spn = $"hdfs/{dataNode.HostName}";

        logger?.LogDebug("Starting SASL negotiation with {DN}, protection={Level}, SPN={SPN}",
            dataNode, protectionLevel, spn);

        // Get GSSAPI token from Kerberos
        byte[] gssToken = await credentials.GetServiceTokenAsync(spn);

        // Send initial SASL message
        var initMsg = new DataTransferEncryptorMessageProto
        {
            Status = DataTransferEncryptorMessageProto.Types
                .DataTransferEncryptorStatus.Success,
            Payload = ByteString.CopyFrom(gssToken),
        };

        var stream = peer.GetOutputStream();
        initMsg.WriteDelimitedTo(stream);
        await stream.FlushAsync(ct);

        // Read server's response
        var inputStream = peer.GetInputStream();
        var response = DataTransferEncryptorMessageProto.Parser.ParseDelimitedFrom(inputStream);

        if (response.Status != DataTransferEncryptorMessageProto.Types
                .DataTransferEncryptorStatus.Success)
        {
            string errorMsg = string.IsNullOrEmpty(response.Message)
                ? "no details" : response.Message;

            if (response.AccessTokenError)
            {
                throw new AccessTokenException(
                    $"SASL negotiation failed with DataNode {dataNode}: " +
                    $"access token error - {errorMsg}");
            }

            throw new HdfsProtocolException(
                $"SASL negotiation failed with DataNode {dataNode}: " +
                $"status={response.Status}, message={errorMsg}");
        }

        logger?.LogDebug("SASL negotiation succeeded with {DN}", dataNode);

        // For "integrity" and "privacy" modes, the connection stream would need
        // to be wrapped with SASL quality-of-protection. This requires additional
        // GSSAPI context setup and stream wrapping which depends on the specific
        // SASL mechanism negotiated. For now, "authentication" mode is fully
        // functional. Integrity and privacy wrapping will be implemented when
        // needed.
        if (protectionLevel is "integrity" or "privacy")
        {
            logger?.LogWarning(
                "SASL protection level '{Level}' requested but stream wrapping is not yet implemented. " +
                "Connection will proceed with authentication only.", protectionLevel);
        }
    }
}
