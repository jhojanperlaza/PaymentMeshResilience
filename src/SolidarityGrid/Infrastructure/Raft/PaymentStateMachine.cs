using System.Text.Json;
using DotNext.IO;
using DotNext.Net.Cluster.Consensus.Raft.StateMachine;
using SolidarityGrid.Core.Transactions;

namespace SolidarityGrid.Infrastructure.Raft;

public sealed class PaymentStateMachine(NodeSettings settings)
    : SimpleStateMachine(new DirectoryInfo(Path.GetFullPath(Path.Combine(settings.DataPath, "snapshots"))))
{
    public const int CurrentVersion = 3;
    public const string AcceptCommandType = "AcceptPayment";
    public const string AssignCommandType = "AssignPayment";
    public const string ReassignCommandType = "ReassignPayment";
    public const string CompleteCommandType = "CompletePayment";
    private readonly object sync = new();
    private Dictionary<string, Payment> byKey = new(StringComparer.Ordinal);
    private Dictionary<Guid, Payment> byId = new();
    private readonly Dictionary<Guid, int> effectiveCompletions = new();

    public Payment? FindKey(string key) { lock (sync) return byKey.GetValueOrDefault(key); }
    public Payment? FindId(Guid id) { lock (sync) return byId.GetValueOrDefault(id); }
    public Payment[] Payments { get { lock (sync) return byId.Values.ToArray(); } }
    public int Count { get { lock (sync) return byId.Count; } }
    public int EffectiveCompletionCount(Guid id) { lock (sync) return effectiveCompletions.GetValueOrDefault(id); }

    protected override async ValueTask RestoreAsync(FileInfo file, CancellationToken token)
    {
        var data = JsonSerializer.Deserialize<Snapshot>(await File.ReadAllBytesAsync(file.FullName, token));
        if (data is null || data.Version != CurrentVersion || data.Payments is null)
            throw new InvalidDataException("Unsupported payment snapshot format.");
        var keys = new Dictionary<string, Payment>(StringComparer.Ordinal);
        var ids = new Dictionary<Guid, Payment>();
        foreach (var payment in data.Payments)
        {
            if (payment is null || payment.TransactionId == Guid.Empty || string.IsNullOrWhiteSpace(payment.IdempotencyKey) ||
                string.IsNullOrWhiteSpace(payment.RequestFingerprint) || payment.DurationSeconds is < 5 or > 10 ||
                !IsValidSnapshotState(payment) ||
                !keys.TryAdd(payment.IdempotencyKey, payment) || !ids.TryAdd(payment.TransactionId, payment))
                throw new InvalidDataException("Duplicate or invalid payment in snapshot.");
        }
        lock (sync) { byKey = keys; byId = ids; effectiveCompletions.Clear(); }
    }

    protected override ValueTask PersistAsync(IAsyncBinaryWriter writer, CancellationToken token)
    {
        byte[] data;
        lock (sync) data = JsonSerializer.SerializeToUtf8Bytes(new Snapshot(CurrentVersion,
            byId.Values.OrderBy(x => x.TransactionId).ToArray()));
        return writer.Invoke(data, token);
    }

    protected override ValueTask<bool> ApplyAsync(LogEntry entry, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        if (!entry.TryGetPayload(out var payload)) throw new InvalidDataException("Missing payment command.");
        using var document = JsonDocument.Parse(payload);
        var envelope = document.RootElement.Deserialize<PaymentCommandEnvelope>()
            ?? throw new InvalidDataException("Invalid payment command envelope.");
        if (envelope.Version != CurrentVersion || string.IsNullOrWhiteSpace(envelope.Type))
            throw new InvalidDataException("Unsupported payment command envelope version.");

        lock (sync)
        {
            switch (envelope.Type)
            {
                case AcceptCommandType:
                    ApplyAcceptance(Deserialize<AcceptPayment>(envelope));
                    break;
                case AssignCommandType:
                    ApplyAssignment(Deserialize<AssignPayment>(envelope));
                    break;
                case ReassignCommandType:
                    ApplyReassignment(Deserialize<ReassignPayment>(envelope));
                    break;
                case CompleteCommandType:
                    ApplyCompletion(Deserialize<CompletePayment>(envelope));
                    break;
                default:
                    throw new InvalidDataException($"Unknown payment command type '{envelope.Type}'.");
            }
        }
        return ValueTask.FromResult(entry.Index % 8 == 0);
    }

    private static T Deserialize<T>(PaymentCommandEnvelope envelope)
    {
        var command = envelope.Payload.Deserialize<T>();
        var commandVersion = command switch
        {
            AcceptPayment value => value.Version,
            AssignPayment value => value.Version,
            ReassignPayment value => value.Version,
            CompletePayment value => value.Version,
            _ => 0
        };
        return command is not null && commandVersion == CurrentVersion
            ? command
            : throw new InvalidDataException("Invalid or unsupported payment command.");
    }

    private void ApplyAcceptance(AcceptPayment command)
    {
        if (command.TransactionId == Guid.Empty || string.IsNullOrWhiteSpace(command.IdempotencyKey) ||
            string.IsNullOrWhiteSpace(command.RequestFingerprint) || command.DurationSeconds is < 5 or > 10)
            throw new InvalidDataException("Invalid acceptance command.");
        if (!byKey.ContainsKey(command.IdempotencyKey) && !byId.ContainsKey(command.TransactionId))
        {
            var payment = new Payment(command.TransactionId, command.IdempotencyKey, command.RequestFingerprint,
                command.DurationSeconds, "Accepted", null, 0, command.CreatedAt);
            byKey.Add(payment.IdempotencyKey, payment);
            byId.Add(payment.TransactionId, payment);
        }
    }

    private void ApplyAssignment(AssignPayment command)
    {
        if (string.IsNullOrWhiteSpace(command.AssignedNodeId) || !settings.Members.ContainsKey(command.AssignedNodeId) || command.Attempt != 1 ||
            !byId.TryGetValue(command.TransactionId, out var payment)) return;
        if (payment.Status == "Accepted")
            Replace(payment with { Status = "Processing", AssignedNodeId = command.AssignedNodeId, Attempt = 1 });
    }

    private void ApplyReassignment(ReassignPayment command)
    {
        if (string.IsNullOrWhiteSpace(command.ExpectedAssignedNodeId) || string.IsNullOrWhiteSpace(command.NewAssignedNodeId) ||
            !settings.Members.ContainsKey(command.NewAssignedNodeId) ||
            !byId.TryGetValue(command.TransactionId, out var payment) || payment.Status != "Processing" ||
            payment.AssignedNodeId != command.ExpectedAssignedNodeId || payment.Attempt != command.ExpectedAttempt ||
            command.NewAttempt != command.ExpectedAttempt + 1) return;
        Replace(payment with { AssignedNodeId = command.NewAssignedNodeId, Attempt = command.NewAttempt });
    }

    private void ApplyCompletion(CompletePayment command)
    {
        if (string.IsNullOrWhiteSpace(command.AssignedNodeId) || !settings.Members.ContainsKey(command.AssignedNodeId) ||
            command.CompletedAt == default || command.Result != "Succeeded" ||
            !byId.TryGetValue(command.TransactionId, out var payment)) return;
        if (payment.Status == "Processing" && payment.AssignedNodeId == command.AssignedNodeId && payment.Attempt == command.Attempt)
        {
            Replace(payment with { Status = "Completed", CompletedAt = command.CompletedAt, Result = command.Result });
            effectiveCompletions[command.TransactionId] = effectiveCompletions.GetValueOrDefault(command.TransactionId) + 1;
        }
    }

    private void Replace(Payment payment)
    {
        byId[payment.TransactionId] = payment;
        byKey[payment.IdempotencyKey] = payment;
    }

    private bool IsValidSnapshotState(Payment payment) => payment.Status switch
    {
        "Accepted" => payment.AssignedNodeId is null && payment.Attempt == 0 && payment.CompletedAt is null && payment.Result is null,
        "Processing" => payment.AssignedNodeId is not null && settings.Members.ContainsKey(payment.AssignedNodeId) &&
            payment.Attempt >= 1 && payment.CompletedAt is null && payment.Result is null,
        "Completed" => payment.AssignedNodeId is not null && settings.Members.ContainsKey(payment.AssignedNodeId) &&
            payment.Attempt >= 1 && payment.CompletedAt is not null && payment.Result == "Succeeded",
        _ => false
    };

    public static byte[] Encode<T>(T command)
    {
        var type = command switch
        {
            AcceptPayment => AcceptCommandType,
            AssignPayment => AssignCommandType,
            ReassignPayment => ReassignCommandType,
            CompletePayment => CompleteCommandType,
            _ => throw new ArgumentException("Unsupported payment command.", nameof(command))
        };
        var payload = JsonSerializer.SerializeToElement(command);
        return JsonSerializer.SerializeToUtf8Bytes(new PaymentCommandEnvelope(CurrentVersion, type, payload));
    }

    public static AcceptPayment Create(string key, string fingerprint, int duration)
        => new(CurrentVersion, Guid.NewGuid(), key, fingerprint, duration, DateTimeOffset.UtcNow);

    public sealed record Snapshot(int Version, Payment[] Payments);
}
