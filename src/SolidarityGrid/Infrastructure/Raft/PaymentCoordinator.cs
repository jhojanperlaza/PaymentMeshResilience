using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using DotNext.Net.Cluster;
using DotNext.Net.Cluster.Consensus.Raft;
using DotNext.Net.Cluster.Messaging;
using DotNext.Net;
using Microsoft.Extensions.Hosting;
using SolidarityGrid.Core.Transactions;
using SolidarityGrid.Infrastructure.Messaging;
using SolidarityGrid.Observability;

namespace SolidarityGrid.Infrastructure.Raft;

public sealed class PaymentCoordinator(
    IRaftCluster cluster,
    IMessageBus bus,
    NodeSettings settings,
    PaymentStateMachine state,
    PaymentWorker worker,
    ILogger<PaymentCoordinator> logger) : BackgroundService
{
    private readonly ConcurrentDictionary<(Guid Id, int Attempt), WorkObservation> observed = new();
    private int nextWorker;
    private bool wasLeader;
    private long leadershipStartedAt;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(500));
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            var isLeader = !cluster.LeadershipToken.IsCancellationRequested;
            if (!isLeader)
            {
                wasLeader = false;
                continue;
            }
            if (!wasLeader)
            {
                observed.Clear();
                leadershipStartedAt = Stopwatch.GetTimestamp();
                logger.LogInformation("Payment Coordinator elected NodeId={NodeId} RaftTerm={RaftTerm} EventType=CoordinatorLeaderStarted",
                    settings.NodeId, cluster.Term);
            }
            wasLeader = true;
            using var leadership = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, cluster.LeadershipToken);
            try { await CoordinateOnceAsync(leadership.Token); }
            catch (OperationCanceledException) when (leadership.IsCancellationRequested) { }
            catch (Exception e) when (ClusterDiagnostics.IsUnavailable(e))
            { logger.LogWarning(e, "Coordinator cannot currently reach quorum NodeId={NodeId} RaftTerm={RaftTerm}", settings.NodeId, cluster.Term); }
        }
    }

    private async Task CoordinateOnceAsync(CancellationToken token)
    {
        foreach (var payment in state.Payments)
        {
            token.ThrowIfCancellationRequested();
            if (payment.Status == "Accepted")
            {
                var target = SelectAvailableWorker();
                if (target is null) continue;
                var command = new AssignPayment(PaymentStateMachine.CurrentVersion, payment.TransactionId,
                    target, 1, DateTimeOffset.UtcNow);
                await cluster.ReplicateAsync((ReadOnlyMemory<byte>)PaymentStateMachine.Encode(command), token: token);
                var applied = state.FindId(payment.TransactionId);
                if (applied is { Status: "Processing", Attempt: 1 })
                    logger.LogInformation("Payment assigned TransactionId={TransactionId} NodeId={NodeId} AssignedNodeId={AssignedNodeId} Attempt={Attempt} RaftTerm={RaftTerm} EventType=PaymentAssigned",
                        payment.TransactionId, settings.NodeId, applied.AssignedNodeId, applied.Attempt, cluster.Term);
            }
        }

        foreach (var payment in state.Payments.Where(p => p.Status == "Processing" && p.AssignedNodeId is not null))
        {
            token.ThrowIfCancellationRequested();
            var key = (payment.TransactionId, payment.Attempt);
            var observation = observed.GetOrAdd(key, _ => new WorkObservation(
                leadershipStartedAt, Stopwatch.GetTimestamp()));
            if (await IsTimedOutAsync(payment, observation, token))
            {
                await ReassignAsync(payment, token);
                continue;
            }
            var progress = observation.Read();
            if (progress.StartedAt is null &&
                Stopwatch.GetElapsedTime(progress.LastDispatchAt) >= TimeSpan.FromSeconds(2))
                await DispatchAsync(payment, observation, token);
        }

        foreach (var key in observed.Keys)
        {
            var payment = state.FindId(key.Id);
            if (payment is null || payment.Status != "Processing" || payment.Attempt != key.Attempt)
                observed.TryRemove(key, out _);
        }
    }

    private async Task<bool> IsTimedOutAsync(Payment payment, WorkObservation observation, CancellationToken token)
    {
        if (Stopwatch.GetElapsedTime(observation.GraceFrom) < TimeSpan.FromSeconds(settings.LeadershipGracePeriodSeconds)) return false;
        var progress = observation.Read();

        if (!IsWorkerAvailable(payment.AssignedNodeId!))
        {
            if (Stopwatch.GetElapsedTime(observation.GraceFrom) >= TimeSpan.FromSeconds(settings.LeadershipGracePeriodSeconds + settings.UnavailableGracePeriodSeconds))
            {
                logger.LogWarning("Worker unavailable TransactionId={TransactionId} NodeId={NodeId} AssignedNodeId={AssignedNodeId} Attempt={Attempt} RaftTerm={RaftTerm} EventType=WorkerUnavailable",
                    payment.TransactionId, settings.NodeId, payment.AssignedNodeId, payment.Attempt, cluster.Term);
                return true;
            }
            return false;
        }

        if (progress.StartedAt is { } startedAt)
        {
            if (Stopwatch.GetElapsedTime(startedAt) >= TimeSpan.FromSeconds(settings.ProcessingTimeoutSeconds))
            {
                logger.LogWarning("Processing timeout TransactionId={TransactionId} NodeId={NodeId} AssignedNodeId={AssignedNodeId} Attempt={Attempt} RaftTerm={RaftTerm} EventType=ProcessingTimeout",
                    payment.TransactionId, settings.NodeId, payment.AssignedNodeId, payment.Attempt, cluster.Term);
                return true;
            }
        }
        else if (!progress.WorkerWasBusy && Stopwatch.GetElapsedTime(observation.FirstDispatchAt) >= TimeSpan.FromSeconds(settings.WorkerStartTimeoutSeconds))
        {
            logger.LogWarning("Worker start timeout TransactionId={TransactionId} NodeId={NodeId} AssignedNodeId={AssignedNodeId} Attempt={Attempt} RaftTerm={RaftTerm} EventType=WorkerStartTimeout",
                payment.TransactionId, settings.NodeId, payment.AssignedNodeId, payment.Attempt, cluster.Term);
            return true;
        }
        return false;
    }

    private bool IsWorkerAvailable(string nodeId)
    {
        if (nodeId == settings.NodeId) return true;
        return bus.Members.Any(member => member.Status == ClusterMemberStatus.Available && settings.Resolve(member.EndPoint) == nodeId);
    }

    private string? SelectAvailableWorker(string? exclude = null)
    {
        var available = bus.Members.Where(m => m.Status == ClusterMemberStatus.Available)
            .Select(m => settings.Resolve(m.EndPoint))
            .Where(id => id is not null && id != exclude).Select(id => id!)
            .Distinct().OrderBy(id => id, StringComparer.Ordinal).ToArray();
        if (available.Length == 0) return null;
        var index = (uint)Interlocked.Increment(ref nextWorker) % (uint)available.Length;
        return available[index];
    }

    private async Task DispatchAsync(Payment payment, WorkObservation observation, CancellationToken token)
    {
        var request = new StartProcessing(payment.TransactionId, payment.Attempt);
        ProcessingReply reply;
        try
        {
            if (payment.AssignedNodeId == settings.NodeId)
                reply = worker.TryEnqueue(request);
            else if (!settings.Members.TryGetValue(payment.AssignedNodeId!, out var uri) ||
                ((IPeerMesh<ISubscriber>)bus).TryGetPeer(new Microsoft.AspNetCore.Connections.UriEndPoint(uri)) is not ISubscriber subscriber)
                return;
            else
                reply = await subscriber.SendMessageAsync(
                    new TextMessage(JsonSerializer.Serialize(request), StartProcessingChannel.RequestName),
                    static async (message, ct) => message.Name == StartProcessingChannel.ResponseName
                        ? JsonSerializer.Deserialize<ProcessingReply>(await message.ReadAsTextAsync(ct))
                            ?? throw new InvalidDataException("Invalid worker acknowledgement.")
                        : throw new InvalidDataException("Unexpected worker response."), token);
        }
        catch (Exception e) when ((ClusterDiagnostics.IsUnavailable(e) || e is InvalidOperationException) && !token.IsCancellationRequested)
        {
            logger.LogWarning(e, "Worker dispatch uncertain TransactionId={TransactionId} NodeId={NodeId} AssignedNodeId={AssignedNodeId} Attempt={Attempt} EventType=DispatchUnavailable",
                payment.TransactionId, settings.NodeId, payment.AssignedNodeId, payment.Attempt);
            return;
        }

        switch (reply.Code)
        {
            case ProcessingResultCode.Queued:
                observation.RecordDispatch(Stopwatch.GetTimestamp());
                break;
            case ProcessingResultCode.AlreadyRunning:
                observation.RecordStarted(Stopwatch.GetTimestamp());
                break;
            case ProcessingResultCode.Busy:
                observation.RecordDispatch(Stopwatch.GetTimestamp());
                observation.RecordBusy();
                logger.LogInformation("Worker busy TransactionId={TransactionId} NodeId={NodeId} AssignedNodeId={AssignedNodeId} Attempt={Attempt} Reason={Reason} EventType=WorkerBusy",
                    payment.TransactionId, settings.NodeId, payment.AssignedNodeId, payment.Attempt, reply.Reason);
                break;
            case ProcessingResultCode.AlreadyCompleted:
            case ProcessingResultCode.Obsolete:
            case ProcessingResultCode.Rejected:
                observed.TryRemove((payment.TransactionId, payment.Attempt), out _);
                break;
        }
    }

    private async Task ReassignAsync(Payment payment, CancellationToken token)
    {
        if (cluster.LeadershipToken.IsCancellationRequested || payment.AssignedNodeId is null) return;
        await cluster.ApplyReadBarrierAsync(ReadBarrierType.Strong, token);
        var current = state.FindId(payment.TransactionId);
        if (current is not { Status: "Processing" } || current.Attempt != payment.Attempt || current.AssignedNodeId != payment.AssignedNodeId)
            return;
        var target = SelectAvailableWorker(current.AssignedNodeId);
        if (target is null) return;

        var command = new ReassignPayment(PaymentStateMachine.CurrentVersion, payment.TransactionId,
            payment.Attempt, payment.AssignedNodeId, target, checked(payment.Attempt + 1), DateTimeOffset.UtcNow);
        logger.LogWarning("Reassignment proposed TransactionId={TransactionId} NodeId={NodeId} PreviousNodeId={PreviousNodeId} NewNodeId={NewNodeId} ExpectedAttempt={ExpectedAttempt} NewAttempt={NewAttempt} RaftTerm={RaftTerm} EventType=ReassignmentProposed",
            payment.TransactionId, settings.NodeId, payment.AssignedNodeId, target, payment.Attempt, command.NewAttempt, cluster.Term);
        try
        {
            await cluster.ReplicateAsync((ReadOnlyMemory<byte>)PaymentStateMachine.Encode(command), token: token);
        }
        catch (Exception e) when (ClusterDiagnostics.IsUnavailable(e) && !token.IsCancellationRequested)
        {
            logger.LogWarning(e, "Reassignment result uncertain TransactionId={TransactionId} Attempt={Attempt} EventType=ReassignmentUncertain",
                payment.TransactionId, payment.Attempt);
            try { await cluster.ApplyReadBarrierAsync(ReadBarrierType.Strong, token); }
            catch (Exception readError) when (ClusterDiagnostics.IsUnavailable(readError) && !token.IsCancellationRequested) { return; }
        }

        var applied = state.FindId(payment.TransactionId);
        if (applied is { Status: "Processing" } && applied.Attempt == command.NewAttempt && applied.AssignedNodeId == target)
        {
            observed.TryRemove((payment.TransactionId, payment.Attempt), out _);
            logger.LogWarning("Reassignment confirmed TransactionId={TransactionId} NodeId={NodeId} AssignedNodeId={AssignedNodeId} Attempt={Attempt} RaftTerm={RaftTerm} EventType=ReassignmentConfirmed",
                payment.TransactionId, settings.NodeId, target, applied.Attempt, cluster.Term);
        }
    }

    public Task<ProcessingReply> ConfirmStartedAsync(ProcessingStarted started, CancellationToken token)
    {
        if (cluster.LeadershipToken.IsCancellationRequested)
            return Task.FromResult(new ProcessingReply(ProcessingResultCode.RetryableUnavailable, "This node is not the current leader."));
        var payment = state.FindId(started.TransactionId);
        if (payment is null) return Task.FromResult(new ProcessingReply(ProcessingResultCode.Rejected, "Transaction does not exist."));
        if (payment.Status == "Completed")
            return Task.FromResult(new ProcessingReply(payment.Attempt == started.Attempt && payment.AssignedNodeId == started.AssignedNodeId
                ? ProcessingResultCode.AlreadyCompleted : ProcessingResultCode.Obsolete));
        if (payment.Status != "Processing" || payment.Attempt != started.Attempt || payment.AssignedNodeId != started.AssignedNodeId)
            return Task.FromResult(new ProcessingReply(ProcessingResultCode.Obsolete, "A newer assignment is confirmed."));
        if (!settings.Members.ContainsKey(started.AssignedNodeId))
            return Task.FromResult(new ProcessingReply(ProcessingResultCode.Rejected, "Unknown Worker."));
        var observation = observed.GetOrAdd((started.TransactionId, started.Attempt), _ => new WorkObservation(
            leadershipStartedAt, Stopwatch.GetTimestamp()));
        observation.RecordStarted(Stopwatch.GetTimestamp());
        logger.LogInformation("Worker start confirmed TransactionId={TransactionId} NodeId={NodeId} AssignedNodeId={AssignedNodeId} Attempt={Attempt} RaftTerm={RaftTerm} EventType=WorkerStarted",
            started.TransactionId, settings.NodeId, started.AssignedNodeId, started.Attempt, cluster.Term);
        return Task.FromResult(new ProcessingReply(ProcessingResultCode.Confirmed));
    }

    public async Task<ProcessingReply> CompleteAsync(ProcessingCompleted completed, CancellationToken token)
    {
        if (completed.Result != "Succeeded" || !settings.Members.ContainsKey(completed.AssignedNodeId))
            return new(ProcessingResultCode.Rejected, "Invalid completion result or Worker.");
        if (state.FindId(completed.TransactionId) is not { } existing)
            return new(ProcessingResultCode.Rejected, "Transaction does not exist.");
        var known = ResolveCurrentResult(existing, completed.AssignedNodeId, completed.Attempt);
        if (known is not null) return known;
        if (cluster.LeadershipToken.IsCancellationRequested) return new(ProcessingResultCode.RetryableUnavailable, "This node is not the current leader.");

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token, cluster.LeadershipToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));
        var command = new CompletePayment(PaymentStateMachine.CurrentVersion, completed.TransactionId,
            completed.AssignedNodeId, completed.Attempt, completed.CompletedAt, completed.Result);
        try
        {
            await cluster.ReplicateAsync((ReadOnlyMemory<byte>)PaymentStateMachine.Encode(command), token: timeout.Token);
        }
        catch (Exception e) when (ClusterDiagnostics.IsUnavailable(e) && !token.IsCancellationRequested)
        {
            logger.LogWarning(e, "Completion result uncertain TransactionId={TransactionId} Attempt={Attempt} RaftTerm={RaftTerm} EventType=CompletionUncertain",
                completed.TransactionId, completed.Attempt, cluster.Term);
            try { await cluster.ApplyReadBarrierAsync(ReadBarrierType.Strong, token); }
            catch (Exception readError) when (ClusterDiagnostics.IsUnavailable(readError) && !token.IsCancellationRequested)
                { return new(ProcessingResultCode.RetryableUnavailable, "Cannot establish a consistent result read."); }
        }

        var applied = state.FindId(completed.TransactionId);
        var result = applied is null ? new ProcessingReply(ProcessingResultCode.Rejected, "Transaction disappeared.")
            : applied.Status == "Completed" && applied.AssignedNodeId == completed.AssignedNodeId && applied.Attempt == completed.Attempt
                ? new ProcessingReply(ProcessingResultCode.Confirmed)
                : ResolveCurrentResult(applied, completed.AssignedNodeId, completed.Attempt)
                    ?? new ProcessingReply(ProcessingResultCode.RetryableUnavailable, "Completion is not yet visible after replication.");
        if (result.Code == ProcessingResultCode.Confirmed)
            logger.LogInformation("Payment completed TransactionId={TransactionId} NodeId={NodeId} AssignedNodeId={AssignedNodeId} Attempt={Attempt} RaftTerm={RaftTerm} EventType=PaymentCompleted",
                completed.TransactionId, settings.NodeId, completed.AssignedNodeId, completed.Attempt, cluster.Term);
        return result;
    }

    private static ProcessingReply? ResolveCurrentResult(Payment payment, string nodeId, int attempt)
    {
        if (payment.Status == "Completed")
            return payment.AssignedNodeId == nodeId && payment.Attempt == attempt
                ? new(ProcessingResultCode.AlreadyCompleted)
                : new(ProcessingResultCode.Obsolete, "Transaction completed by another assignment.");
        if (payment.Status != "Processing" || payment.AssignedNodeId != nodeId || payment.Attempt != attempt)
            return new(ProcessingResultCode.Obsolete, "A newer assignment is confirmed.");
        return null;
    }

    private sealed class WorkObservation(long graceFrom, long dispatchedAt)
    {
        private readonly object sync = new();
        private long lastDispatchAt = dispatchedAt;
        private long? startedAt;
        private bool workerWasBusy;
        public long GraceFrom { get; } = graceFrom;
        public long FirstDispatchAt { get; } = dispatchedAt;
        public (long LastDispatchAt, long? StartedAt, bool WorkerWasBusy) Read()
        { lock (sync) return (lastDispatchAt, startedAt, workerWasBusy); }
        public void RecordDispatch(long when) { lock (sync) { lastDispatchAt = when; workerWasBusy = false; } }
        public void RecordStarted(long when) { lock (sync) { startedAt ??= when; workerWasBusy = false; } }
        public void RecordBusy() { lock (sync) workerWasBusy = true; }
    }
}
