using DotNext.Net.Cluster.Consensus.Raft;
using SolidarityGrid.Infrastructure.Raft;

namespace SolidarityGrid.Observability;

public sealed class ClusterDiagnostics(IRaftCluster cluster, NodeSettings settings)
{
    public async Task<ClusterStatus> ReadAsync(CancellationToken token)
    {
        var ready = false;
        if (cluster.Readiness.IsCompletedSuccessfully)
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
            timeout.CancelAfter(TimeSpan.FromSeconds(3));
            try
            {
                await cluster.ApplyReadBarrierAsync(ReadBarrierType.Strong, timeout.Token);
                ready = true;
            }
            catch (Exception e) when (IsUnavailable(e) && !token.IsCancellationRequested) { }
        }

        return new(settings.NodeId,
            !cluster.LeadershipToken.IsCancellationRequested ? "leader" : cluster.Leader is null ? "unknown" : "follower",
            settings.Resolve(cluster.Leader?.EndPoint), cluster.Term, ready, ready ? "confirmed" : "unknown",
            cluster.Members.Select(member => settings.Resolve(member.EndPoint) ?? member.EndPoint.ToString()!).Order().ToArray());
    }

    public static bool IsUnavailable(Exception e) => e is OperationCanceledException or TimeoutException or
        NotLeaderException or QuorumUnreachableException or DotNext.Net.Cluster.MemberUnavailableException;
}

public sealed record ClusterStatus(string NodeId, string Role, string? LeaderId, long Term, bool Ready,
    string Quorum, string[] Members);
