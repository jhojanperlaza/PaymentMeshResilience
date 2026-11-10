namespace SolidarityGrid.Observability;

public sealed class ClusterObserver(ClusterDiagnostics diagnostics, ILogger<ClusterObserver> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        (long, string?, string, bool)? previous = null;
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        do
        {
            var status = await diagnostics.ReadAsync(stoppingToken);
            var current = (status.Term, status.LeaderId, status.Role, status.Ready);
            if (current != previous)
            {
                logger.LogInformation("Raft change NodeId={NodeId} Term={Term} LeaderId={LeaderId} Role={Role} Ready={Ready} Quorum={Quorum}",
                    status.NodeId, status.Term, status.LeaderId, status.Role, status.Ready, status.Quorum);
                previous = current;
            }
        } while (await timer.WaitForNextTickAsync(stoppingToken));
    }
}
