namespace Gtlm.Hdfs.Client.Protocol;

/// <summary>
/// Thrown when the HDFS data transfer protocol returns an error or unexpected state.
/// </summary>
public class HdfsProtocolException : IOException
{
    public HdfsProtocolException(string message) : base(message) { }
    public HdfsProtocolException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>
/// Thrown when a block access token is invalid or expired.
/// Callers should re-fetch block locations (and tokens) from the NameNode.
/// </summary>
public class AccessTokenException : HdfsProtocolException
{
    public AccessTokenException(string message) : base(message) { }
}

/// <summary>
/// Thrown when a data checksum does not match.
/// Callers should retry with a different replica.
/// </summary>
public class ChecksumException : HdfsProtocolException
{
    public long Offset { get; }

    public ChecksumException(string message, long offset) : base(message)
    {
        Offset = offset;
    }
}
