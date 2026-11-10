using Microsoft.AspNetCore.Connections;

namespace SolidarityGrid.Infrastructure.Raft;

public sealed class NodeSettings
{
    public string NodeId { get; init; } = "node-a";
    public string DataPath { get; init; } = "data";
    public int ApiPort { get; init; } = 8080;
    public int TransportPort { get; init; } = 8081;
    public int MaxConcurrentPayments { get; init; } = 2;
    public int WorkerStartTimeoutSeconds { get; init; } = 5;
    public int ProcessingTimeoutSeconds { get; init; } = 20;
    public int LeadershipGracePeriodSeconds { get; init; } = 5;
    public int UnavailableGracePeriodSeconds { get; init; } = 2;
    public Dictionary<string, Uri> Members { get; init; } = new()
    {
        ["node-a"] = new("http://node-a:8081"),
        ["node-b"] = new("http://node-b:8081"),
        ["node-c"] = new("http://node-c:8081")
    };

    public void Validate()
    {
        if (!Members.Keys.ToHashSet().SetEquals(["node-a", "node-b", "node-c"]) || !Members.ContainsKey(NodeId))
            throw new InvalidOperationException("Exactly node-a, node-b and node-c must be configured, including the local NodeId.");
        if (Members.Values.Any(uri => !uri.IsAbsoluteUri || uri.Scheme != "http" || uri.AbsolutePath != "/") ||
            Members.Values.Distinct().Count() != 3)
            throw new InvalidOperationException("Members must have three distinct absolute HTTP addresses without a path.");
        if (ApiPort is < 1 or > 65535 || TransportPort is < 1 or > 65535 || ApiPort == TransportPort ||
            Members[NodeId].Port != TransportPort || string.IsNullOrWhiteSpace(DataPath) ||
            MaxConcurrentPayments is < 1 or > 32 || WorkerStartTimeoutSeconds < 1 || ProcessingTimeoutSeconds < 2 ||
            LeadershipGracePeriodSeconds < 0 || UnavailableGracePeriodSeconds < 0)
            throw new InvalidOperationException("Invalid ports or data path; the local member must use TransportPort.");
    }

    public string? Resolve(System.Net.EndPoint? endpoint) => endpoint is UriEndPoint uri
        ? Members.FirstOrDefault(member => member.Value == uri.Uri).Key
        : null;
}
