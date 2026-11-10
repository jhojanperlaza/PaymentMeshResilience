using System.Buffers;
using System.Buffers.Binary;
using DotNext.IO;
using DotNext.Net.Cluster.Consensus.Raft.StateMachine;

namespace SolidarityGrid.Infrastructure.Raft;

public sealed class DiagnosticStateMachine(NodeSettings settings)
    : SimpleStateMachine(new DirectoryInfo(Path.GetFullPath(Path.Combine(settings.DataPath, "snapshots"))))
{
    private long value;
    public long Value => Interlocked.Read(ref value);

    protected override async ValueTask RestoreAsync(FileInfo snapshotFile, CancellationToken token)
    {
        var bytes = await File.ReadAllBytesAsync(snapshotFile.FullName, token);
        if (bytes.Length != sizeof(long))
            throw new InvalidDataException("Invalid diagnostic snapshot.");
        Interlocked.Exchange(ref value, BinaryPrimitives.ReadInt64LittleEndian(bytes));
    }

    protected override ValueTask PersistAsync(IAsyncBinaryWriter writer, CancellationToken token)
        => writer.Invoke(Encode(Value), token);

    protected override ValueTask<bool> ApplyAsync(LogEntry entry, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        if (!entry.TryGetPayload(out var payload) || payload.Length != sizeof(long))
            throw new InvalidDataException("Invalid diagnostic log entry.");
        Span<byte> bytes = stackalloc byte[sizeof(long)];
        payload.CopyTo(bytes);
        Interlocked.Exchange(ref value, BinaryPrimitives.ReadInt64LittleEndian(bytes));
        return ValueTask.FromResult(entry.Index % 8 == 0);
    }

    public static byte[] Encode(long value)
    {
        var bytes = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, value);
        return bytes;
    }
}
