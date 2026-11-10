using System.Text.Json;
using DotNext.Net.Cluster.Messaging;
using SolidarityGrid.Infrastructure.Raft;

namespace SolidarityGrid.Infrastructure.Messaging;

public sealed record StartProcessing(Guid TransactionId, int Attempt);
public sealed record ProcessingStarted(Guid TransactionId, string AssignedNodeId, int Attempt, DateTimeOffset StartedAt);
public sealed record ProcessingCompleted(Guid TransactionId, string AssignedNodeId, int Attempt,
    DateTimeOffset CompletedAt, string Result);

public static class ProcessingResultCode
{
    public const string Queued = "Queued";
    public const string Started = "Started";
    public const string AlreadyRunning = "AlreadyRunning";
    public const string AlreadyCompleted = "AlreadyCompleted";
    public const string Busy = "Busy";
    public const string Confirmed = "Confirmed";
    public const string Obsolete = "Obsolete";
    public const string RetryableUnavailable = "RetryableUnavailable";
    public const string Rejected = "Rejected";
}

public sealed record ProcessingReply(string Code, string? Reason = null);

public sealed class StartProcessingChannel(IServiceProvider services) : IInputChannel
{
    public const string RequestName = "solidarity-grid.processing.start.v1";
    public const string ResponseName = "solidarity-grid.processing.started.v1";
    public bool IsSupported(string messageName, bool oneWay) => !oneWay && messageName == RequestName;

    public async Task<IMessage> ReceiveMessage(ISubscriber sender, IMessage message, object? context, CancellationToken token)
    {
        var request = JsonSerializer.Deserialize<StartProcessing>(await message.ReadAsTextAsync(token))
            ?? throw new InvalidDataException("Invalid processing request.");
        var reply = services.GetRequiredService<PaymentWorker>().TryEnqueue(request);
        return new TextMessage(JsonSerializer.Serialize(reply), ResponseName);
    }

    public Task ReceiveSignal(ISubscriber sender, IMessage signal, object? context, CancellationToken token)
        => Task.FromException(new NotSupportedException("Processing requires request/reply."));
}

public sealed class ProcessingStartedChannel(IServiceProvider services) : IInputChannel
{
    public const string RequestName = "solidarity-grid.processing.start-confirmed.v1";
    public const string ResponseName = "solidarity-grid.processing.start-result.v1";
    public bool IsSupported(string messageName, bool oneWay) => !oneWay && messageName == RequestName;

    public async Task<IMessage> ReceiveMessage(ISubscriber sender, IMessage message, object? context, CancellationToken token)
    {
        var started = JsonSerializer.Deserialize<ProcessingStarted>(await message.ReadAsTextAsync(token))
            ?? throw new InvalidDataException("Invalid processing-start message.");
        var reply = await services.GetRequiredService<PaymentCoordinator>().ConfirmStartedAsync(started, token);
        return new TextMessage(JsonSerializer.Serialize(reply), ResponseName);
    }

    public Task ReceiveSignal(ISubscriber sender, IMessage signal, object? context, CancellationToken token)
        => Task.FromException(new NotSupportedException("Processing start requires request/reply."));
}

public sealed class ProcessingCompletedChannel(IServiceProvider services) : IInputChannel
{
    public const string RequestName = "solidarity-grid.processing.completed.v1";
    public const string ResponseName = "solidarity-grid.processing.result.v1";
    public bool IsSupported(string messageName, bool oneWay) => !oneWay && messageName == RequestName;

    public async Task<IMessage> ReceiveMessage(ISubscriber sender, IMessage message, object? context, CancellationToken token)
    {
        var completed = JsonSerializer.Deserialize<ProcessingCompleted>(await message.ReadAsTextAsync(token))
            ?? throw new InvalidDataException("Invalid completion message.");
        var reply = await services.GetRequiredService<PaymentCoordinator>().CompleteAsync(completed, token);
        return new TextMessage(JsonSerializer.Serialize(reply), ResponseName);
    }

    public Task ReceiveSignal(ISubscriber sender, IMessage signal, object? context, CancellationToken token)
        => Task.FromException(new NotSupportedException("Completion requires request/reply."));
}
