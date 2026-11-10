using DotNext.Net.Cluster.Consensus.Raft;
using SolidarityGrid.Core.Transactions;
using SolidarityGrid.Observability;

namespace SolidarityGrid.Infrastructure.Raft;

public sealed class PaymentAcceptance(IRaftCluster cluster, PaymentStateMachine state)
{
    private readonly SemaphoreSlim gate = new(1, 1);

    public async Task<(int Status, Payment? Payment)> AcceptAsync(string key, string fingerprint, int duration,
        CancellationToken token)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token, cluster.LeadershipToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));
        try
        {
            await gate.WaitAsync(timeout.Token);
            try
            {
                if (cluster.LeadershipToken.IsCancellationRequested) return (503, null);
                await cluster.ApplyReadBarrierAsync(ReadBarrierType.Strong, timeout.Token);
                var existing = state.FindKey(key);
                if (existing is not null) return existing.RequestFingerprint == fingerprint ? (202, existing) : (409, null);
                var command = PaymentStateMachine.Create(key, fingerprint, duration);
                await cluster.ReplicateAsync((ReadOnlyMemory<byte>)PaymentStateMachine.Encode(command), token: timeout.Token);
                var applied = state.FindKey(key);
                return applied is null ? (503, null) : applied.RequestFingerprint == fingerprint ? (202, applied) : (409, null);
            }
            finally { gate.Release(); }
        }
        catch (Exception e) when (ClusterDiagnostics.IsUnavailable(e) && !token.IsCancellationRequested)
        {
            return (503, null);
        }
    }
}
