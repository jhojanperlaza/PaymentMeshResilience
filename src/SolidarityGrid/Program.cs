using DotNext.Net.Cluster.Consensus.Raft;
using DotNext.Net.Cluster.Consensus.Raft.Http;
using DotNext.Net.Cluster.Consensus.Raft.Membership;
using DotNext.Net.Cluster.Messaging;
using Microsoft.AspNetCore.Connections;
using SolidarityGrid.Api;
using SolidarityGrid.Infrastructure.Messaging;
using SolidarityGrid.Infrastructure.Raft;
using SolidarityGrid.Observability;

if (args is ["--healthcheck"])
{
    using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
    try
    {
        var port = Environment.GetEnvironmentVariable("Grid__ApiPort") ?? "8080";
        using var response = await client.GetAsync($"http://127.0.0.1:{port}/health/live");
        return response.IsSuccessStatusCode ? 0 : 1;
    }
    catch (HttpRequestException) { return 1; }
    catch (TaskCanceledException) { return 1; }
}

if (args is ["--probe-no-quorum"])
{
    using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(8) };
    try
    {
        var port = Environment.GetEnvironmentVariable("Grid__ApiPort") ?? "8080";
        var baseAddress = $"http://127.0.0.1:{port}";
        using var ready = await client.GetAsync(baseAddress + "/health/ready");
        using var request = new HttpRequestMessage(HttpMethod.Post, baseAddress + "/pay");
        request.Headers.Add("Idempotency-Key", "partition-probe-" + Guid.NewGuid().ToString("N"));
        request.Content = new StringContent("{\"durationSeconds\":5}", System.Text.Encoding.UTF8, "application/json");
        using var payment = await client.SendAsync(request);
        return ready.StatusCode == System.Net.HttpStatusCode.ServiceUnavailable &&
            payment.StatusCode == System.Net.HttpStatusCode.ServiceUnavailable ? 0 : 1;
    }
    catch (HttpRequestException) { return 1; }
    catch (TaskCanceledException) { return 1; }
}

var builder = WebApplication.CreateBuilder(args);
var settings = builder.Configuration.GetSection("Grid").Get<NodeSettings>() ?? new();
settings.Validate();
StorageFormat.Check(settings.DataPath);
builder.Logging.ClearProviders();
builder.Logging.AddJsonConsole();
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(settings.ApiPort);
    options.ListenAnyIP(settings.TransportPort);
});
builder.Services.AddSingleton(settings);
builder.Services.UsePersistentConfigurationStorage(Path.GetFullPath(Path.Combine(settings.DataPath, "members.bin")));
builder.Services.UseStateMachine<PaymentStateMachine>(new() { Location = Path.Combine(settings.DataPath, "wal") });
builder.Services.AddSingleton<IInputChannel, PingChannel>();
builder.Services.AddSingleton<IInputChannel, SubmitPaymentChannel>();
builder.Services.AddSingleton<PaymentWorker>();
builder.Services.AddSingleton<IInputChannel, StartProcessingChannel>();
builder.Services.AddSingleton<IInputChannel, ProcessingStartedChannel>();
builder.Services.AddSingleton<PaymentCoordinator>();
builder.Services.AddSingleton<IInputChannel, ProcessingCompletedChannel>();
builder.Services.AddSingleton<PaymentAcceptance>();
builder.Services.AddSingleton<ClusterDiagnostics>();
builder.JoinCluster((config, _, _) =>
{
    config.PublicEndPoint = settings.Members[settings.NodeId];
    config.ColdStart = false;
    config.LowerElectionTimeout = 1000;
    config.UpperElectionTimeout = 2000;
    config.RequestTimeout = TimeSpan.FromSeconds(3);
});
builder.Services.AddHostedService<ClusterObserver>();
builder.Services.AddHostedService<PaymentWorker>(provider => provider.GetRequiredService<PaymentWorker>());
builder.Services.AddHostedService<PaymentCoordinator>(provider => provider.GetRequiredService<PaymentCoordinator>());

await using var app = builder.Build();
await app.RestoreStateAsync<PaymentStateMachine>();
await FixedMembership.InitializeAsync(app.Services.GetRequiredService<IClusterConfigurationStorage<UriEndPoint>>(), settings);

app.MapWhen(context => context.Connection.LocalPort == settings.TransportPort,
    transport => transport.UseConsensusProtocolHandler());
app.MapDiagnostics();
app.MapPayments();
await app.RunAsync();
return 0;
