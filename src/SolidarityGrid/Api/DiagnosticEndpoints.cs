using DotNext.Net.Cluster.Consensus.Raft;
using DotNext.Net.Cluster.Messaging;
using SolidarityGrid.Infrastructure.Messaging;
using SolidarityGrid.Infrastructure.Raft;
using SolidarityGrid.Observability;

namespace SolidarityGrid.Api;

public static class DiagnosticEndpoints
{
    public static void MapDiagnostics(this WebApplication app)
    {
        app.MapGet("/health/live", () => Results.Ok(new { status = "alive" }));
        app.MapGet("/cluster", (ClusterDiagnostics diagnostics, CancellationToken token) => diagnostics.ReadAsync(token));
        app.MapGet("/health/ready", async (ClusterDiagnostics diagnostics, CancellationToken token) =>
        {
            var status = await diagnostics.ReadAsync(token);
            return Results.Json(status, statusCode: status.Ready ? 200 : 503);
        });
        app.MapPost("/diagnostics/ping/{target}", async (string target, IMessageBus bus, NodeSettings settings,
            CancellationToken token) =>
        {
            var member = bus.Members.FirstOrDefault(member => settings.Resolve(member.EndPoint) == target);
            if (member is null) return Results.NotFound();
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
            timeout.CancelAfter(TimeSpan.FromSeconds(3));
            try
            {
                var receiver = await member.SendMessageAsync(new TextMessage(settings.NodeId, PingChannel.PingName),
                    static async (message, ct) => message.Name == PingChannel.PongName
                        ? await message.ReadAsTextAsync(ct)
                        : throw new InvalidDataException("Unexpected pong response."), timeout.Token);
                return Results.Ok(new { senderId = settings.NodeId, receiverId = receiver, transport = "IMessageBus" });
            }
            catch (Exception e) when (ClusterDiagnostics.IsUnavailable(e) && !token.IsCancellationRequested)
            {
                return Results.StatusCode(503);
            }
        });
    }
}
