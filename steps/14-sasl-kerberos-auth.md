# Step 14: SASL/Kerberos Authentication

**Phase:** 2 (Full Client)
**Prerequisites:** Steps 03, 12 (Peer, NameNode RPC)
**Produces:** `Net/SaslDataTransferHandler.cs` + NameNode RPC auth

---

## Objective

Add security support for HDFS clusters configured with Kerberos authentication and/or
data transfer encryption. This involves:

1. **NameNode RPC auth:** SASL/GSSAPI handshake on the IPC connection
2. **DataNode auth:** SASL negotiation on the data transfer connection when
   `dfs.data.transfer.protection` is set to `authentication`, `integrity`, or `privacy`
3. **Block access tokens:** Already supported in Phase 1 (passed through), but now
   validated end-to-end with a secure cluster

---

## Tasks

### 14.1 Dependencies

Add `Kerberos.NET` for cross-platform Kerberos support:

```xml
<PackageReference Include="Kerberos.NET" Version="5.*" />
```

### 14.2 Kerberos Credential Provider

**File:** `src/Gtlm.Hdfs.Client/Security/KerberosCredentialProvider.cs`

```csharp
namespace Gtlm.Hdfs.Client.Security;

using Kerberos.NET.Client;
using Kerberos.NET.Credentials;

/// <summary>
/// Manages Kerberos credentials and service ticket acquisition.
/// </summary>
public sealed class KerberosCredentialProvider : IAsyncDisposable
{
    private readonly KerberosClient _client;
    private readonly KerberosCredential _credential;

    public KerberosCredentialProvider(HdfsClientOptions options)
    {
        _client = new KerberosClient();

        if (!string.IsNullOrEmpty(options.KeytabPath))
        {
            _credential = new KeytabCredential(
                options.KerberosPrincipal!, options.KeytabPath);
        }
        else
        {
            // Use credential cache (kinit / OS ticket cache)
            _credential = new KerberosPasswordCredential(
                options.KerberosPrincipal!, "");
        }
    }

    /// <summary>Authenticate with the KDC.</summary>
    public async Task AuthenticateAsync(CancellationToken ct = default)
    {
        await _client.Authenticate(_credential);
    }

    /// <summary>
    /// Get a GSSAPI token for the specified service principal.
    /// Used for SASL GSSAPI handshakes with NameNode and DataNodes.
    /// </summary>
    public async Task<byte[]> GetServiceTokenAsync(string servicePrincipal)
    {
        var ticket = await _client.GetServiceTicket(servicePrincipal);
        return ticket.EncodeGssApi().ToArray();
    }

    public ValueTask DisposeAsync()
    {
        _client.Dispose();
        return ValueTask.CompletedTask;
    }
}
```

### 14.3 NameNode RPC SASL Handshake

Update `NameNodeRpcClient.ConnectAsync` to perform SASL negotiation when Kerberos
is configured:

```csharp
private async Task PerformSaslHandshakeAsync(CancellationToken ct)
{
    // 1. Send auth method = KERBEROS (value 81) in the connection header
    // 2. Read SaslMessageProto (NEGOTIATE) from server
    // 3. Send INITIATE with GSSAPI mechanism and initial token
    // 4. Loop CHALLENGE/RESPONSE until SUCCESS
    // 5. After SASL success, the stream is optionally wrapped
    //    (integrity/privacy protection)
}
```

The SASL negotiation uses its own protobuf messages
(`RpcSaslProto` from `RpcHeader.proto`):

```
State machine:
  Client                          Server
    │── NEGOTIATE ──────────────►│
    │◄── NEGOTIATE (mechs) ──────│
    │── INITIATE (GSSAPI token) ►│
    │◄── CHALLENGE (token) ──────│  (may repeat)
    │── RESPONSE (token) ────────►│
    │◄── SUCCESS ────────────────│
```

### 14.4 DataNode SASL Handshake

**File:** `src/Gtlm.Hdfs.Client/Net/SaslDataTransferHandler.cs`

The DataNode data transfer protocol has its own SASL negotiation, separate from the
NameNode IPC SASL. It uses `DataTransferEncryptorMessageProto` from `datatransfer.proto`.

```csharp
namespace Gtlm.Hdfs.Client.Net;

public sealed class SaslDataTransferHandler
{
    /// <summary>
    /// Perform SASL negotiation on a DataNode connection.
    /// Must be called after TCP connect, before sending OP_READ_BLOCK.
    ///
    /// Protection levels:
    ///   - "authentication": verify identity only
    ///   - "integrity": verify identity + message integrity (HMAC)
    ///   - "privacy": verify identity + encrypt all data
    /// </summary>
    public static async Task<Stream> NegotiateAsync(
        Peer peer,
        KerberosCredentialProvider credentials,
        DatanodeInfo dataNode,
        string protectionLevel,
        CancellationToken ct = default)
    {
        // 1. Get GSSAPI token for DataNode service principal
        //    (typically "hdfs/datanode-hostname@REALM")
        string spn = $"hdfs/{dataNode.HostName}";
        byte[] token = await credentials.GetServiceTokenAsync(spn);

        // 2. Send DataTransferEncryptorMessageProto(SUCCESS, token)
        var initMsg = new DataTransferEncryptorMessageProto
        {
            Status = DataTransferEncryptorMessageProto.Types
                .DataTransferEncryptorStatus.Success,
            Payload = Google.Protobuf.ByteString.CopyFrom(token),
        };
        // Write varint-prefixed to the stream
        initMsg.WriteDelimitedTo(peer.GetOutputStream());
        await peer.GetOutputStream().FlushAsync(ct);

        // 3. Read server's response
        var response = DataTransferEncryptorMessageProto.Parser
            .ParseDelimitedFrom(peer.GetInputStream());

        if (response.Status != DataTransferEncryptorMessageProto.Types
                .DataTransferEncryptorStatus.Success)
        {
            throw new HdfsProtocolException(
                $"SASL negotiation failed with DataNode {dataNode}: " +
                $"status={response.Status}, message={response.Message}");
        }

        // 4. For "privacy" mode, wrap the stream with encryption
        //    For "integrity" mode, wrap with HMAC verification
        //    For "authentication" mode, return the original stream
        return protectionLevel switch
        {
            "privacy" => WrapWithEncryption(peer, response),
            "integrity" => WrapWithIntegrity(peer, response),
            _ => peer.GetOutputStream(),
        };
    }
}
```

### 14.5 Configuration

Update `HdfsClientOptions`:

```csharp
/// <summary>Kerberos principal (e.g., "user@REALM.COM").</summary>
public string? KerberosPrincipal { get; set; }

/// <summary>Path to keytab file. If null, uses credential cache.</summary>
public string? KeytabPath { get; set; }

/// <summary>Data transfer protection level: "", "authentication", "integrity", "privacy".</summary>
public string DataTransferProtection { get; set; } = "";

/// <summary>Enable data transfer encryption (AES).</summary>
public bool EncryptDataTransfer { get; set; } = false;
```

### 14.6 Integration Points

Update `Peer.ConnectAsync` to call `SaslDataTransferHandler.NegotiateAsync` when
security is configured:

```csharp
// In BlockReaderFactory.CreateRemoteReaderAsync:
if (!string.IsNullOrEmpty(_options.DataTransferProtection))
{
    await SaslDataTransferHandler.NegotiateAsync(
        peer, _credentialProvider, dataNode,
        _options.DataTransferProtection, ct);
}
```

Update `NameNodeRpcClient.ConnectAsync` to perform SASL handshake when
`KerberosPrincipal` is set.

---

## Testing

### Unit Tests
- Mock SASL handshake exchange with pre-computed tokens
- Verify correct `DataTransferEncryptorMessageProto` serialization

### Integration Tests
- Requires a Kerberized Hadoop cluster (Docker Compose with MIT KDC)
- Test against `dfs.data.transfer.protection=authentication` first (simplest)
- Test `privacy` mode (encrypted data transfer)

---

## Acceptance Criteria

- [ ] `KerberosCredentialProvider` authenticates with a KDC and acquires service tickets
- [ ] NameNode RPC SASL handshake succeeds with GSSAPI
- [ ] DataNode SASL negotiation succeeds for `authentication` protection level
- [ ] Block reads work end-to-end on a Kerberized cluster
- [ ] Non-secure clusters continue to work (SASL is skipped)
- [ ] Error messages clearly indicate auth failures (expired ticket, wrong principal)
