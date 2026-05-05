namespace Gtlm.Hdfs.Client.Protocol;

using System.Buffers.Binary;
using Google.Protobuf;
using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Net;
using Gtlm.Hdfs.Client.Proto;

/// <summary>
/// Sends data transfer protocol requests to an HDFS DataNode.
/// </summary>
internal static class DataTransferSender
{
    /// <summary>
    /// Send an OP_READ_BLOCK request to the DataNode.
    ///
    /// Wire format:
    ///   [2 bytes BE: DATA_TRANSFER_VERSION (28)]
    ///   [1 byte: OP_READ_BLOCK (81)]
    ///   [varint: proto message length]
    ///   [N bytes: serialized OpReadBlockProto]
    /// </summary>
    public static async ValueTask SendReadBlockAsync(
        Peer peer,
        ExtendedBlock block,
        BlockToken token,
        string clientName,
        long offset,
        long length,
        bool sendChecksums,
        CancellationToken ct = default)
    {
        var stream = peer.GetOutputStream();

        // 1. Write version (2 bytes, big-endian)
        var header = new byte[3];
        BinaryPrimitives.WriteInt16BigEndian(header.AsSpan(0, 2), DataTransferConstants.DataTransferVersion);

        // 2. Write opcode (1 byte)
        header[2] = OpCode.ReadBlock;
        await stream.WriteAsync(header, ct);

        // 3. Build and write varint-prefixed protobuf message
        var proto = new OpReadBlockProto
        {
            Header = new ClientOperationHeaderProto
            {
                BaseHeader = new BaseHeaderProto
                {
                    Block = block.ToProto(),
                    Token = token.ToProto(),
                },
                ClientName = clientName,
            },
            Offset = (ulong)offset,
            Len = (ulong)length,
            SendChecksums = sendChecksums,
        };

        proto.WriteDelimitedTo(stream);

        // 4. Flush to ensure request is sent immediately
        await stream.FlushAsync(ct);
    }

    /// <summary>
    /// Send the client's final read status to the DataNode.
    /// Called after all packets have been read (or on error).
    /// </summary>
    public static async ValueTask SendClientReadStatusAsync(
        Peer peer,
        Status status,
        CancellationToken ct = default)
    {
        var stream = peer.GetOutputStream();

        var proto = new ClientReadStatusProto { Status = status };
        proto.WriteDelimitedTo(stream);

        await stream.FlushAsync(ct);
    }
}
