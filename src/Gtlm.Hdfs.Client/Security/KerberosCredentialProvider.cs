namespace Gtlm.Hdfs.Client.Security;

using Kerberos.NET.Client;
using Kerberos.NET.Credentials;
using Gtlm.Hdfs.Client.Configuration;
using Microsoft.Extensions.Logging;

/// <summary>
/// Manages Kerberos credentials and service ticket acquisition.
/// Used for SASL/GSSAPI authentication with NameNodes and DataNodes.
/// </summary>
public sealed class KerberosCredentialProvider : IAsyncDisposable
{
    private readonly KerberosClient _client;
    private readonly KerberosCredential _credential;
    private readonly ILogger? _logger;
    private bool _authenticated;

    public KerberosCredentialProvider(HdfsClientOptions options, ILogger? logger = null)
    {
        _client = new KerberosClient();
        _logger = logger;

        if (string.IsNullOrEmpty(options.KerberosPrincipal))
            throw new ArgumentException("KerberosPrincipal must be set for Kerberos authentication.");

        if (string.IsNullOrEmpty(options.KeytabPath))
            throw new ArgumentException(
                "KeytabPath must be set for Kerberos authentication. " +
                "Generate a keytab with: ktutil or kadmin.");

        var keytab = new Kerberos.NET.Crypto.KeyTable(File.ReadAllBytes(options.KeytabPath));
        _credential = new KeytabCredential(options.KerberosPrincipal, keytab);
        _logger?.LogDebug("Using keytab credential for {Principal} from {Keytab}",
            options.KerberosPrincipal, options.KeytabPath);
    }

    /// <summary>Authenticate with the KDC.</summary>
    public async Task AuthenticateAsync(CancellationToken ct = default)
    {
        await _client.Authenticate(_credential);
        _authenticated = true;
        _logger?.LogInformation("Kerberos authentication succeeded for {Principal}",
            _credential.UserName);
    }

    /// <summary>
    /// Get a GSSAPI/SPNEGO token for the specified service principal.
    /// Used for SASL GSSAPI handshakes with NameNode and DataNodes.
    /// </summary>
    public async Task<byte[]> GetServiceTokenAsync(string servicePrincipal)
    {
        if (!_authenticated)
            throw new InvalidOperationException("Not authenticated. Call AuthenticateAsync first.");

        var ticket = await _client.GetServiceTicket(servicePrincipal);
        var token = ticket.EncodeGssApi().ToArray();

        _logger?.LogDebug("Obtained service ticket for {SPN} ({Bytes} bytes)",
            servicePrincipal, token.Length);

        return token;
    }

    public ValueTask DisposeAsync()
    {
        _client.Dispose();
        return ValueTask.CompletedTask;
    }
}
