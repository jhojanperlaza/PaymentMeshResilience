using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using DotNext.Net.Cluster.Consensus.Raft;
using DotNext.Net.Cluster.Messaging;
using SolidarityGrid.Infrastructure.Messaging;
using SolidarityGrid.Infrastructure.Raft;
using SolidarityGrid.Observability;

namespace SolidarityGrid.Api;

public static class PaymentEndpoints
{
    public static void MapPayments(this WebApplication app)
    {
        app.MapPost("/pay", async (HttpRequest request, IRaftCluster cluster, IMessageBus bus,
            PaymentAcceptance acceptance, CancellationToken token) =>
        {
            if (!request.Headers.TryGetValue("Idempotency-Key", out var keyHeader) || keyHeader.Count != 1 ||
                string.IsNullOrWhiteSpace(keyHeader[0]) || keyHeader[0]!.Length > 256)
                return Results.BadRequest(new { error = "Idempotency-Key is required (1–256 characters)." });
            var key = keyHeader[0]!;
            int duration;
            try
            {
                if (request.ContentLength == 0) duration = 10;
                else
                {
                    using var document = await JsonDocument.ParseAsync(request.Body, cancellationToken: token);
                    if (document.RootElement.ValueKind != JsonValueKind.Object ||
                        document.RootElement.EnumerateObject().Count() > 1 ||
                        document.RootElement.EnumerateObject().Any(p => p.Name != "durationSeconds"))
                        return Results.BadRequest(new { error = "Only durationSeconds is supported." });
                    if (document.RootElement.TryGetProperty("durationSeconds", out var value))
                    {
                        if (value.ValueKind != JsonValueKind.Number || !value.TryGetInt32(out duration))
                            return Results.BadRequest(new { error = "durationSeconds must be an integer." });
                    }
                    else duration = 10;
                }
            }
            catch (JsonException) { return Results.BadRequest(new { error = "Invalid JSON body." }); }
            if (duration is < 5 or > 10) return Results.BadRequest(new { error = "durationSeconds must be 5–10." });
            var fingerprint = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(duration.ToString(System.Globalization.CultureInfo.InvariantCulture))));
            int status;
            Guid? id;
            if (!cluster.LeadershipToken.IsCancellationRequested)
            {
                var result = await acceptance.AcceptAsync(key, fingerprint, duration, token);
                (status, id) = (result.Status, result.Payment?.TransactionId);
            }
            else
            {
                if (cluster.Leader is not ISubscriber leader) return Results.StatusCode(503);
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
                timeout.CancelAfter(TimeSpan.FromSeconds(5));
                try
                {
                    var payload = JsonSerializer.Serialize(new SubmitPayment(key, fingerprint, duration));
                    var result = await leader.SendMessageAsync(new TextMessage(payload, SubmitPaymentChannel.RequestName),
                        static async (message, ct) => message.Name == SubmitPaymentChannel.ResponseName
                            ? JsonSerializer.Deserialize<SubmitResult>(await message.ReadAsTextAsync(ct))
                                ?? throw new InvalidDataException("Invalid submit response.")
                            : throw new InvalidDataException("Unexpected submit response."), timeout.Token);
                    (status, id) = (result.Status, result.TransactionId);
                }
                catch (Exception e) when (ClusterDiagnostics.IsUnavailable(e) && !token.IsCancellationRequested)
                { return Results.StatusCode(503); }
            }
            return status switch
            {
                202 when id.HasValue => Results.Accepted($"/payments/{id}", new { transactionId = id.Value }),
                409 => Results.Conflict(new { error = "Idempotency-Key already used with different content." }),
                _ => Results.StatusCode(503)
            };
        });

        app.MapGet("/payments/{transactionId:guid}", async (Guid transactionId, IRaftCluster cluster,
            PaymentStateMachine state, CancellationToken token) =>
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
            timeout.CancelAfter(TimeSpan.FromSeconds(5));
            try
            {
                await cluster.ApplyReadBarrierAsync(ReadBarrierType.Strong, timeout.Token);
                return state.FindId(transactionId) is { } payment ? Results.Ok(payment) : Results.NotFound();
            }
            catch (Exception e) when (ClusterDiagnostics.IsUnavailable(e) && !token.IsCancellationRequested)
            { return Results.StatusCode(503); }
        });
    }
}
