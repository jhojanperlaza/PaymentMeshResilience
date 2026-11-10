using System.Text.Json;
using DotNext.Net.Cluster.Messaging;
using SolidarityGrid.Infrastructure.Raft;

namespace SolidarityGrid.Infrastructure.Messaging;

public sealed record SubmitPayment(string Key, string Fingerprint, int Duration);
public sealed record SubmitResult(int Status, Guid? TransactionId);

public sealed class SubmitPaymentChannel(IServiceProvider services) : IInputChannel
{
    public const string RequestName = "solidarity-grid.submit.v1";
    public const string ResponseName = "solidarity-grid.submit-result.v1";
    public bool IsSupported(string messageName, bool oneWay) => !oneWay && messageName == RequestName;

    public async Task<IMessage> ReceiveMessage(ISubscriber sender, IMessage message, object? context, CancellationToken token)
    {
        var request = JsonSerializer.Deserialize<SubmitPayment>(await message.ReadAsTextAsync(token))
            ?? throw new InvalidDataException("Invalid submit message.");
        var (status, payment) = await services.GetRequiredService<PaymentAcceptance>()
            .AcceptAsync(request.Key, request.Fingerprint, request.Duration, token);
        return new TextMessage(JsonSerializer.Serialize(new SubmitResult(status, payment?.TransactionId)), ResponseName);
    }

    public Task ReceiveSignal(ISubscriber sender, IMessage signal, object? context, CancellationToken token)
        => Task.FromException(new NotSupportedException("Submit requires request/reply."));
}
