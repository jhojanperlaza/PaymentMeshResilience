using DotNext.Net.Cluster.Consensus.Raft.Membership;
using Microsoft.AspNetCore.Connections;

namespace SolidarityGrid.Infrastructure.Raft;

public static class FixedMembership
{
    public static async Task InitializeAsync(IClusterConfigurationStorage<UriEndPoint> storage,
        NodeSettings settings, CancellationToken token = default)
    {
        var configuration = await storage.LoadConfigurationAsync(token);
        var expected = settings.Members.Values.Select(uri => new UriEndPoint(uri)).ToArray();
        if (configuration.Members.Count == 0)
        {
            
            foreach (var member in expected)
                configuration = configuration.Add(member);
            await storage.SaveConfigurationAsync(configuration, configurationVersion: 0, token);
        }
        else if (!configuration.Members.SetEquals(expected))
        {
            throw new InvalidOperationException("Persisted membership differs from the configured three nodes. Refusing to overwrite it.");
        }
    }
}
