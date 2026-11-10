using System.Text.Json;
using System.Threading.Channels;
using DotNext.Net.Cluster.Messaging;
using Microsoft.Extensions.Hosting;
using SolidarityGrid.Infrastructure.Messaging;
using SolidarityGrid.Observability;

namespace SolidarityGrid.Infrastructure.Raft;

public sealed class PaymentWorker(
    NodeSettings settings,
    PaymentStateMachine state,
    IMessageBus bus,
    ILogger<PaymentWorker> logger) : BackgroundService
{
    private readonly object sync = new();
    private readonly Dictionary<(Guid Id, int Attempt), string> active = new();
    private readonly HashSet<(Guid Id, int Attempt)> suppressed = [];
    private readonly Channel<StartProcessing> queue = Channel.CreateBounded<StartProcessing>(new BoundedChannelOptions(settings.MaxConcurrentPayments)
    {
        FullMode = BoundedChannelFullMode.Wait,
        SingleReader = false,
        SingleWriter = false
    });

    public int ActiveCount { get { lock (sync) return active.Count; } }

    public ProcessingReply TryEnqueue(StartProcessing request)
    {
        var payment = state.FindId(request.TransactionId);
        if (payment?.Status == "Completed" && payment.AssignedNodeId == settings.NodeId && payment.Attempt == request.Attempt)
            return new(ProcessingResultCode.AlreadyCompleted);
        if (payment is null || payment.Status != "Processing" || payment.AssignedNodeId != settings.NodeId || payment.Attempt != request.Attempt)
            return new(ProcessingResultCode.Obsolete, "Assignment no longer belongs to this Worker.");

        lock (sync)
        {
            PruneObsoleteAttempts();
            var key = (request.TransactionId, request.Attempt);
            if (active.TryGetValue(key, out var status))
                return new(status == "Started" ? ProcessingResultCode.AlreadyRunning : ProcessingResultCode.Queued);
            if (suppressed.Contains(key)) return new(ProcessingResultCode.Queued, "This generation already ran locally.");
            if (active.Count >= settings.MaxConcurrentPayments) return new(ProcessingResultCode.Busy, "Worker capacity is full.");
            active.Add(key, "Queued");
            if (queue.Writer.TryWrite(request)) return new(ProcessingResultCode.Queued);
            active.Remove(key);
            return new(ProcessingResultCode.Busy, "Worker queue is full.");
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.WhenAll(RunQueueAsync(stoppingToken), PruneSuppressedAsync(stoppingToken));
    }

    private Task RunQueueAsync(CancellationToken stoppingToken)
        => Parallel.ForEachAsync(queue.Reader.ReadAllAsync(stoppingToken), new ParallelOptions
        {
            MaxDegreeOfParallelism = settings.MaxConcurrentPayments,
            CancellationToken = stoppingToken
        }, async (request, token) =>
        {
            var keepSuppressed = false;
            try { keepSuppressed = !await ProcessAsync(request, token); }
            catch (OperationCanceledException) when (token.IsCancellationRequested) { }
            catch (Exception e)
            {
                keepSuppressed = true;
                logger.LogError(e, "Worker processing failed TransactionId={TransactionId} NodeId={NodeId} Attempt={Attempt}",
                    request.TransactionId, settings.NodeId, request.Attempt);
            }
            finally { FinishAttempt(request, keepSuppressed); }
        });

    private async Task PruneSuppressedAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            lock (sync) PruneObsoleteAttempts();
        }
    }

    private async Task<bool> ProcessAsync(StartProcessing request, CancellationToken token)
    {
        if (!IsCurrentOwner(request)) return true;
        SetAttemptState(request, "Started");
        var started = new ProcessingStarted(request.TransactionId, settings.NodeId, request.Attempt, DateTimeOffset.UtcNow);
        while (!token.IsCancellationRequested)
        {
            if (!IsCurrentOwner(request)) return true;
            ProcessingReply startReply;
            try
            {
                startReply = await SendToLeaderAsync(started, ProcessingStartedChannel.RequestName,
                    ProcessingStartedChannel.ResponseName, token);
            }
            catch (Exception e) when ((ClusterDiagnostics.IsUnavailable(e) || e is InvalidOperationException) && !token.IsCancellationRequested)
            {
                logger.LogWarning(e, "Start confirmation uncertain TransactionId={TransactionId} NodeId={NodeId} Attempt={Attempt} EventType=StartConfirmationRetry",
                    request.TransactionId, settings.NodeId, request.Attempt);
                await Task.Delay(TimeSpan.FromSeconds(1), token);
                continue;
            }
            if (startReply.Code == ProcessingResultCode.Confirmed) break;
            if (startReply.Code is ProcessingResultCode.Obsolete or ProcessingResultCode.Rejected or ProcessingResultCode.AlreadyCompleted)
                return true;
            await Task.Delay(TimeSpan.FromSeconds(1), token);
        }
        if (token.IsCancellationRequested) return false;

        var payment = state.FindId(request.TransactionId);
        if (payment is null || payment.Status != "Processing" || payment.AssignedNodeId != settings.NodeId || payment.Attempt != request.Attempt)
            return true;
        logger.LogInformation("Payment Worker started TransactionId={TransactionId} NodeId={NodeId} AssignedNodeId={AssignedNodeId} Attempt={Attempt} ActiveCount={ActiveCount} EventType=WorkerStarted",
            request.TransactionId, settings.NodeId, settings.NodeId, request.Attempt, ActiveCount);
        await Task.Delay(TimeSpan.FromSeconds(payment.DurationSeconds), token);

        var completed = new ProcessingCompleted(payment.TransactionId, settings.NodeId, request.Attempt,
            DateTimeOffset.UtcNow, "Succeeded");
        while (!token.IsCancellationRequested)
        {
            ProcessingReply reply;
            try
            {
                reply = await SendToLeaderAsync(completed, ProcessingCompletedChannel.RequestName,
                    ProcessingCompletedChannel.ResponseName, token);
            }
            catch (Exception e) when ((ClusterDiagnostics.IsUnavailable(e) || e is InvalidOperationException) && !token.IsCancellationRequested)
            {
                logger.LogWarning(e, "Completion delivery uncertain TransactionId={TransactionId} NodeId={NodeId} Attempt={Attempt} EventType=CompletionRetry",
                    request.TransactionId, settings.NodeId, request.Attempt);
                await Task.Delay(TimeSpan.FromSeconds(1), token);
                continue;
            }

            switch (reply.Code)
            {
                case ProcessingResultCode.Confirmed:
                case ProcessingResultCode.AlreadyCompleted:
                    logger.LogInformation("Payment Worker observed completion TransactionId={TransactionId} NodeId={NodeId} Attempt={Attempt} EventType=CompletionConfirmed",
                        request.TransactionId, settings.NodeId, request.Attempt);
                    return true;
                case ProcessingResultCode.Obsolete:
                    logger.LogInformation("Obsolete Worker attempt stopped TransactionId={TransactionId} NodeId={NodeId} Attempt={Attempt} Reason={Reason} EventType=ObsoleteAttemptRejected",
                        request.TransactionId, settings.NodeId, request.Attempt, reply.Reason);
                    return true;
                case ProcessingResultCode.Rejected:
                    logger.LogWarning("Worker result rejected TransactionId={TransactionId} NodeId={NodeId} Attempt={Attempt} Reason={Reason} EventType=CompletionRejected",
                        request.TransactionId, settings.NodeId, request.Attempt, reply.Reason);
                    return true;
                case ProcessingResultCode.RetryableUnavailable:
                    await Task.Delay(TimeSpan.FromSeconds(1), token);
                    break;
                default:
                    return true;
            }
        }
        return false;
    }

    private async Task<ProcessingReply> SendToLeaderAsync<T>(T payload, string requestName, string responseName, CancellationToken token)
        => await bus.LeaderRouter.SendMessageAsync(
            new TextMessage(JsonSerializer.Serialize(payload), requestName),
            async (message, ct) => message.Name == responseName
                ? JsonSerializer.Deserialize<ProcessingReply>(await message.ReadAsTextAsync(ct))
                    ?? throw new InvalidDataException("Invalid processing response.")
                : throw new InvalidDataException("Unexpected processing response."), token);

    private bool IsCurrentOwner(StartProcessing request)
    {
        var payment = state.FindId(request.TransactionId);
        return payment is { Status: "Processing" } && payment.AssignedNodeId == settings.NodeId && payment.Attempt == request.Attempt;
    }

    private void SetAttemptState(StartProcessing request, string status)
    {
        lock (sync) active[(request.TransactionId, request.Attempt)] = status;
    }

    private void FinishAttempt(StartProcessing request, bool suppressDuplicate)
    {
        lock (sync)
        {
            var key = (request.TransactionId, request.Attempt);
            active.Remove(key);
            var payment = state.FindId(request.TransactionId);
            if (payment is null || payment.Status == "Completed" || payment.Attempt != request.Attempt || payment.AssignedNodeId != settings.NodeId)
                suppressed.Remove(key);
            else if (suppressDuplicate)
                suppressed.Add(key);
            else
                suppressed.Remove(key);
        }
    }

    private void PruneObsoleteAttempts()
    {
        foreach (var key in suppressed.ToArray())
        {
            var payment = state.FindId(key.Id);
            if (payment is null || payment.Status == "Completed" || payment.Attempt != key.Attempt || payment.AssignedNodeId != settings.NodeId)
                suppressed.Remove(key);
        }
    }
}
