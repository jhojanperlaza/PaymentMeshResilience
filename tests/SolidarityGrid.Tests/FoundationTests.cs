using DotNext.Net.Cluster.Consensus.Raft;
using DotNext.Net.Cluster.Consensus.Raft.Membership;
using DotNext.Net.Cluster.Consensus.Raft.StateMachine;
using Microsoft.AspNetCore.Connections;
using Microsoft.Extensions.DependencyInjection;
using SolidarityGrid.Infrastructure.Raft;

namespace SolidarityGrid.Tests;

public sealed class FoundationTests : IDisposable
{
    private readonly string directory = Path.Combine(Path.GetTempPath(), "solidarity-grid-tests", Guid.NewGuid().ToString("N"));

    [Fact]
    public void RejectsDuplicateMembersAndUnknownLocalNode()
    {
        var settings = new NodeSettings();
        settings.Members["node-b"] = settings.Members["node-a"];
        Assert.Throws<InvalidOperationException>(settings.Validate);
        Assert.Throws<InvalidOperationException>(() => new NodeSettings { NodeId = "node-d" }.Validate());
    }

    [Fact]
    public async Task MembershipPersistsAndRejectsConfigurationDrift()
    {
        Directory.CreateDirectory(directory);
        var settings = new NodeSettings();
        using (var services = StorageServices())
            await FixedMembership.InitializeAsync(services.GetRequiredService<IClusterConfigurationStorage<UriEndPoint>>(), settings);

        using var reopened = StorageServices();
        var storage = reopened.GetRequiredService<IClusterConfigurationStorage<UriEndPoint>>();
        await FixedMembership.InitializeAsync(storage, settings);
        Assert.Equal(3, (await storage.LoadConfigurationAsync()).Members.Count);
        settings.Members["node-c"] = new Uri("http://replacement:8081");
        await Assert.ThrowsAsync<InvalidOperationException>(() => FixedMembership.InitializeAsync(storage, settings));
    }

    [Theory]
    [InlineData(3)] // WAL replay without a snapshot.
    [InlineData(12)] // Snapshot plus the remaining WAL suffix.
    public async Task OnlyCommittedValuesAreAppliedAndRecovered(int committedCount)
    {
        var settings = new NodeSettings { DataPath = directory };
        var options = new WriteAheadLog.Options { Location = Path.Combine(directory, "wal") };
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var token = timeout.Token;
        await using (var state = new DiagnosticStateMachine(settings))
        {
            await state.RestoreAsync(token);
            await using var wal = new WriteAheadLog(options, state);
            await wal.InitializeAsync(token);
            for (var value = 1; value <= committedCount; value++)
            {
                var index = await wal.AppendAsync(new BinaryLogEntry
                {
                    Term = 1,
                    Content = DiagnosticStateMachine.Encode(value)
                }, token);
                await wal.CommitAsync(index, token);
                await wal.WaitForApplyAsync(index, token);
            }
            await wal.AppendAsync(new BinaryLogEntry { Term = 1, Content = DiagnosticStateMachine.Encode(999) }, token);
            Assert.Equal(committedCount, state.Value);
        }

        await using var restored = new DiagnosticStateMachine(settings);
        await restored.RestoreAsync(token); // Must precede the WAL constructor.
        Assert.Equal(committedCount >= 8 ? 8 : 0, restored.Value);
        await using var recoveredWal = new WriteAheadLog(options, restored);
        await recoveredWal.InitializeAsync(token);
        await recoveredWal.WaitForApplyAsync(committedCount, token);
        Assert.Equal(committedCount, restored.Value);
    }

    private ServiceProvider StorageServices() => new ServiceCollection()
        .UsePersistentConfigurationStorage(Path.Combine(directory, "members.bin")).BuildServiceProvider();

    public void Dispose()
    {
        if (Directory.Exists(directory)) Directory.Delete(directory, recursive: true);
    }
}
