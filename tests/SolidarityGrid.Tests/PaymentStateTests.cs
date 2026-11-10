using DotNext.Net.Cluster.Consensus.Raft;
using DotNext.Net.Cluster.Consensus.Raft.StateMachine;
using Microsoft.Extensions.Logging.Abstractions;
using SolidarityGrid.Core.Transactions;
using SolidarityGrid.Infrastructure.Messaging;
using SolidarityGrid.Infrastructure.Raft;
using SolidarityGrid.Observability;

namespace SolidarityGrid.Tests;

public sealed class PaymentStateTests : IDisposable
{
    private readonly string root = Path.Combine(Path.GetTempPath(), "solidarity-grid-payments", Guid.NewGuid().ToString("N"));

    [Fact]
    public void MarkerRejectsLegacyStorageWithoutChangingIt()
    {
        Directory.CreateDirectory(root);
        var legacy = Path.Combine(root, "legacy.bin");
        File.WriteAllBytes(legacy, [1, 2, 3]);
        Assert.Throws<InvalidDataException>(() => StorageFormat.Check(root));
        Assert.Equal(new byte[] { 1, 2, 3 }, File.ReadAllBytes(legacy));
        Assert.False(File.Exists(Path.Combine(root, "format.txt")));
    }

    [Fact]
    public void MarkerRejectsUnknownVersionWithoutChangingIt()
    {
        StorageFormat.Check(root);
        var path = Path.Combine(root, "format.txt");
        File.WriteAllText(path, "solidarity-grid-payments-v2\n");
        Assert.Throws<InvalidDataException>(() => StorageFormat.Check(root));
        Assert.Equal("solidarity-grid-payments-v2\n", File.ReadAllText(path));
    }

    [Fact]
    public void TransientClassifierDoesNotHideProgrammingErrors()
    {
        Assert.True(ClusterDiagnostics.IsUnavailable(new TimeoutException()));
        Assert.True(ClusterDiagnostics.IsUnavailable(new NotLeaderException()));
        Assert.True(ClusterDiagnostics.IsUnavailable(new QuorumUnreachableException()));
        Assert.False(ClusterDiagnostics.IsUnavailable(new InvalidOperationException()));
        Assert.False(ClusterDiagnostics.IsUnavailable(new InvalidDataException()));
    }

    [Fact]
    public void CommandsUseExplicitVersionedDiscriminators()
    {
        var transactionId = Guid.NewGuid();
        var encoded = PaymentStateMachine.Encode(new ReassignPayment(PaymentStateMachine.CurrentVersion,
            transactionId, 1, "node-a", "node-b", 2, DateTimeOffset.UtcNow));
        var envelope = System.Text.Json.JsonSerializer.Deserialize<PaymentCommandEnvelope>(encoded);
        Assert.NotNull(envelope);
        Assert.Equal(3, envelope.Version);
        Assert.Equal(PaymentStateMachine.ReassignCommandType, envelope.Type);
        Assert.Equal(transactionId, envelope.Payload.GetProperty("TransactionId").GetGuid());
    }

    [Fact]
    public async Task WorkerBoundsLocalCapacityAndDeduplicatesTheSameAttempt()
    {
        StorageFormat.Check(root);
        var settings = new NodeSettings { DataPath = root, NodeId = "node-b", MaxConcurrentPayments = 2 };
        var options = new WriteAheadLog.Options { Location = Path.Combine(root, "wal") };
        var payments = Enumerable.Range(0, 3).Select(i => PaymentStateMachine.Create("worker-" + i, "fp-" + i, 5)).ToArray();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await using var state = new PaymentStateMachine(settings);
        await state.RestoreAsync(timeout.Token);
        await using var wal = new WriteAheadLog(options, state);
        await wal.InitializeAsync(timeout.Token);
        foreach (var payment in payments)
        {
            var acceptIndex = await wal.AppendAsync(new BinaryLogEntry
                { Term = 1, Content = PaymentStateMachine.Encode(payment) }, timeout.Token);
            await wal.CommitAsync(acceptIndex, timeout.Token);
            await wal.WaitForApplyAsync(acceptIndex, timeout.Token);
            var assign = new AssignPayment(PaymentStateMachine.CurrentVersion, payment.TransactionId, "node-b", 1, DateTimeOffset.UtcNow);
            var assignIndex = await wal.AppendAsync(new BinaryLogEntry
                { Term = 1, Content = PaymentStateMachine.Encode(assign) }, timeout.Token);
            await wal.CommitAsync(assignIndex, timeout.Token);
            await wal.WaitForApplyAsync(assignIndex, timeout.Token);
        }

        using var worker = new PaymentWorker(settings, state, null!, NullLogger<PaymentWorker>.Instance);
        var first = new StartProcessing(payments[0].TransactionId, 1);
        Assert.Equal(ProcessingResultCode.Queued, worker.TryEnqueue(first).Code);
        Assert.Equal(ProcessingResultCode.Queued, worker.TryEnqueue(first).Code);
        Assert.Equal(1, worker.ActiveCount);
        Assert.Equal(ProcessingResultCode.Queued, worker.TryEnqueue(new(payments[1].TransactionId, 1)).Code);
        Assert.Equal(2, worker.ActiveCount);
        Assert.Equal(ProcessingResultCode.Busy, worker.TryEnqueue(new(payments[2].TransactionId, 1)).Code);
        Assert.Equal(2, worker.ActiveCount);
        Assert.Equal(ProcessingResultCode.Obsolete, worker.TryEnqueue(new(payments[0].TransactionId, 2)).Code);
    }

    [Fact]
    public async Task ConcurrentCompletionsApplyOneEffectiveTransition()
    {
        StorageFormat.Check(root);
        var settings = new NodeSettings { DataPath = root };
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await using var state = new PaymentStateMachine(settings);
        await state.RestoreAsync(timeout.Token);
        await using var wal = new WriteAheadLog(new WriteAheadLog.Options { Location = Path.Combine(root, "wal") }, state);
        await wal.InitializeAsync(timeout.Token);
        var payment = PaymentStateMachine.Create("concurrent", "same", 5);
        foreach (var command in new object[] { payment,
            new AssignPayment(PaymentStateMachine.CurrentVersion, payment.TransactionId, "node-a", 1, DateTimeOffset.UtcNow) })
        {
            var index = await wal.AppendAsync(new BinaryLogEntry { Term = 1, Content = PaymentStateMachine.Encode(command) }, timeout.Token);
            await wal.CommitAsync(index, timeout.Token);
            await wal.WaitForApplyAsync(index, timeout.Token);
        }
        var at = DateTimeOffset.UtcNow;
        var proposals = Enumerable.Range(0, 8).Select(i => Task.Run(async () =>
            await wal.AppendAsync(new BinaryLogEntry { Term = 1, Content = PaymentStateMachine.Encode(
                new CompletePayment(PaymentStateMachine.CurrentVersion, payment.TransactionId, "node-a", 1,
                    at.AddMilliseconds(i), "Succeeded")) }, timeout.Token), timeout.Token));
        var indexes = await Task.WhenAll(proposals);
        await wal.CommitAsync(indexes.Max(), timeout.Token);
        await wal.WaitForApplyAsync(indexes.Max(), timeout.Token);
        Assert.Equal(1, state.EffectiveCompletionCount(payment.TransactionId));
        Assert.Equal("Completed", state.FindId(payment.TransactionId)?.Status);
        var recordedAt = state.FindId(payment.TransactionId)?.CompletedAt;
        var stale = await wal.AppendAsync(new BinaryLogEntry { Term = 1, Content = PaymentStateMachine.Encode(
            new CompletePayment(PaymentStateMachine.CurrentVersion, payment.TransactionId, "node-b", 2, at, "Succeeded")) }, timeout.Token);
        await wal.CommitAsync(stale, timeout.Token);
        await wal.WaitForApplyAsync(stale, timeout.Token);
        var lateReassignment = await wal.AppendAsync(new BinaryLogEntry { Term = 1, Content = PaymentStateMachine.Encode(
            new ReassignPayment(PaymentStateMachine.CurrentVersion, payment.TransactionId, 1, "node-a", "node-b", 2, at)) }, timeout.Token);
        await wal.CommitAsync(lateReassignment, timeout.Token);
        await wal.WaitForApplyAsync(lateReassignment, timeout.Token);
        Assert.Equal(1, state.EffectiveCompletionCount(payment.TransactionId));
        Assert.Equal(recordedAt, state.FindId(payment.TransactionId)?.CompletedAt);
        Assert.Equal(1, state.FindId(payment.TransactionId)?.Attempt);
    }

    [Fact]
    public async Task CommittedCommandsResolveDuplicatesAndRecoverFromSnapshot()
    {
        StorageFormat.Check(root);
        var settings = new NodeSettings { DataPath = root };
        var options = new WriteAheadLog.Options { Location = Path.Combine(root, "wal") };
        var first = PaymentStateMachine.Create("key", "same", 10);
        var duplicate = PaymentStateMachine.Create("key", "same", 10);
        var conflict = PaymentStateMachine.Create("key", "different", 5);
        var assignedAt = DateTimeOffset.UtcNow;
        var completedAt = assignedAt.AddSeconds(7);
        var commands = new List<object>
        {
            first, duplicate, conflict,
            new AssignPayment(PaymentStateMachine.CurrentVersion, first.TransactionId, "node-b", 1, assignedAt),
            new ReassignPayment(PaymentStateMachine.CurrentVersion, first.TransactionId, 1, "node-c", "node-a", 2, assignedAt),
            new ReassignPayment(PaymentStateMachine.CurrentVersion, first.TransactionId, 1, "node-b", "node-c", 2, assignedAt),
            new ReassignPayment(PaymentStateMachine.CurrentVersion, first.TransactionId, 1, "node-b", "node-a", 2, assignedAt),
            new CompletePayment(PaymentStateMachine.CurrentVersion, first.TransactionId, "node-b", 1, completedAt, "Succeeded"),
            new CompletePayment(PaymentStateMachine.CurrentVersion, first.TransactionId, "node-c", 2, completedAt, "Succeeded"),
            new ReassignPayment(PaymentStateMachine.CurrentVersion, first.TransactionId, 2, "node-c", "node-a", 3, assignedAt),
            new CompletePayment(PaymentStateMachine.CurrentVersion, first.TransactionId, "node-c", 2, completedAt.AddSeconds(1), "Succeeded")
        };
        commands.AddRange(Enumerable.Range(0, 9).Select(i => (object)PaymentStateMachine.Create("key-" + i, "fp-" + i, 10)));
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await using (var state = new PaymentStateMachine(settings))
        {
            await state.RestoreAsync(timeout.Token);
            await using var wal = new WriteAheadLog(options, state);
            await wal.InitializeAsync(timeout.Token);
            for (var i = 0; i < commands.Count; i++)
            {
                var index = await wal.AppendAsync(new BinaryLogEntry
                { Term = 1, Content = PaymentStateMachine.Encode(commands[i]) }, timeout.Token);
                await wal.CommitAsync(index, timeout.Token);
                await wal.WaitForApplyAsync(index, timeout.Token);
                if (i == 3)
                {
                    Assert.Equal("Processing", state.FindId(first.TransactionId)?.Status);
                    Assert.Equal("node-b", state.FindId(first.TransactionId)?.AssignedNodeId);
                    Assert.Equal(1, state.FindId(first.TransactionId)?.Attempt);
                }
                if (i == 5)
                {
                    Assert.Equal("Processing", state.FindId(first.TransactionId)?.Status);
                    Assert.Equal("node-c", state.FindId(first.TransactionId)?.AssignedNodeId);
                    Assert.Equal(2, state.FindId(first.TransactionId)?.Attempt);
                }
            }
            Assert.Equal(10, state.Count);
            Assert.Equal(first.TransactionId, state.FindKey("key")?.TransactionId);
            Assert.Equal(10, state.FindId(first.TransactionId)?.DurationSeconds);
            Assert.Equal("Completed", state.FindId(first.TransactionId)?.Status);
            Assert.Equal("node-c", state.FindId(first.TransactionId)?.AssignedNodeId);
            Assert.Equal(2, state.FindId(first.TransactionId)?.Attempt);
            Assert.Equal("Succeeded", state.FindId(first.TransactionId)?.Result);
            Assert.Equal(completedAt, state.FindId(first.TransactionId)?.CompletedAt);
            Assert.Equal(1, state.EffectiveCompletionCount(first.TransactionId));
        }
        await using var restored = new PaymentStateMachine(settings);
        await restored.RestoreAsync(timeout.Token);
        await using var recovered = new WriteAheadLog(options, restored);
        await recovered.InitializeAsync(timeout.Token);
        await recovered.WaitForApplyAsync(commands.Count, timeout.Token);
        Assert.Equal(10, restored.Count);
        Assert.Equal(first.TransactionId, restored.FindKey("key")?.TransactionId);
        Assert.Equal(10, restored.FindId(first.TransactionId)?.DurationSeconds);
        Assert.Equal("Completed", restored.FindId(first.TransactionId)?.Status);
        Assert.Equal("node-c", restored.FindId(first.TransactionId)?.AssignedNodeId);
        Assert.Equal(2, restored.FindId(first.TransactionId)?.Attempt);
        Assert.Equal(completedAt, restored.FindId(first.TransactionId)?.CompletedAt);
    }

    public void Dispose()
    {
        if (Directory.Exists(root)) Directory.Delete(root, true);
    }
}
