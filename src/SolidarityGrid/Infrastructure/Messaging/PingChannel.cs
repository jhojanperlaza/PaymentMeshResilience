using DotNext.Net.Cluster.Messaging;
using SolidarityGrid.Infrastructure.Raft;

namespace SolidarityGrid.Infrastructure.Messaging;

public sealed class PingChannel(NodeSettings settings, ILogger<PingChannel> logger) : IInputChannel
{
    public const string PingName = "solidarity-grid.ping.v1";
    public const string PongName = "solidarity-grid.pong.v1";

    public bool IsSupported(string messageName, bool oneWay) => !oneWay && messageName == PingName;

    public Task<IMessage> ReceiveMessage(ISubscriber sender, IMessage message, object? context, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        logger.LogInformation("Ping received by {NodeId} from {Sender}", settings.NodeId, sender.EndPoint);
        return Task.FromResult<IMessage>(new TextMessage(settings.NodeId, PongName));
    }

    public Task ReceiveSignal(ISubscriber sender, IMessage signal, object? context, CancellationToken token)
        => Task.FromException(new NotSupportedException("Ping requires request/reply."));
}
