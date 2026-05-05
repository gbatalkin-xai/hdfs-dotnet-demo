namespace Gtlm.Hdfs.Client.Protocol;

using Google.Protobuf;
using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Net;
using Gtlm.Hdfs.Client.Proto;

/// <summary>
/// Parses data transfer protocol responses from an HDFS DataNode.
/// </summary>
internal static class DataTransferReceiver
{
    /// <summary>
    /// Read and parse the BlockOpResponseProto from the DataNode.
    /// This is the first message received after sending OP_READ_BLOCK.
    /// Wire format: varint-length-prefixed protobuf message.
    /// </summary>
    public static BlockOpResponseProto ReceiveBlockOpResponse(Peer peer)
    {
        var stream = peer.GetInputStream();
        return BlockOpResponseProto.Parser.ParseDelimitedFrom(stream);
    }

    /// <summary>
    /// Validate the DataNode's response and extract checksum info.
    /// Throws on non-SUCCESS status.
    /// </summary>
    public static ChecksumInfo ValidateAndExtractChecksumInfo(
        BlockOpResponseProto response,
        DatanodeInfo dataNode,
        ExtendedBlock block,
        string file)
    {
        if (response.Status == Status.ErrorAccessToken)
        {
            throw new AccessTokenException(
                $"Block token rejected by DataNode {dataNode} for block {block.BlockId}: " +
                (string.IsNullOrEmpty(response.Message) ? "no details" : response.Message));
        }

        if (response.Status != Status.Success)
        {
            throw new HdfsProtocolException(
                $"DataNode {dataNode} failed OP_READ_BLOCK for block {block.BlockId} " +
                $"of file {file}: status={response.Status}" +
                (string.IsNullOrEmpty(response.Message) ? "" : $", message={response.Message}"));
        }

        if (response.ReadOpChecksumInfo == null)
        {
            throw new HdfsProtocolException(
                $"DataNode {dataNode} returned SUCCESS but no ReadOpChecksumInfo " +
                $"for block {block.BlockId}");
        }

        var checksumProto = response.ReadOpChecksumInfo.Checksum;
        if (checksumProto == null)
        {
            throw new HdfsProtocolException(
                $"DataNode {dataNode} returned empty ChecksumProto for block {block.BlockId}");
        }

        return new ChecksumInfo(
            ChecksumType: checksumProto.Type,
            BytesPerChecksum: (int)checksumProto.BytesPerChecksum,
            ChunkOffset: (long)response.ReadOpChecksumInfo.ChunkOffset);
    }

    /// <summary>
    /// Validate the firstChunkOffset from the DataNode response.
    /// Must satisfy: 0 &lt;= firstChunkOffset &lt;= startOffset
    ///               AND firstChunkOffset &gt; startOffset - bytesPerChecksum
    /// </summary>
    public static void ValidateChunkOffset(
        long firstChunkOffset,
        long startOffset,
        int bytesPerChecksum,
        string file)
    {
        if (firstChunkOffset < 0 ||
            firstChunkOffset > startOffset ||
            firstChunkOffset <= (startOffset - bytesPerChecksum))
        {
            throw new IOException(
                $"BlockReader: error in first chunk offset ({firstChunkOffset}), " +
                $"startOffset is {startOffset} for file {file}");
        }
    }
}
