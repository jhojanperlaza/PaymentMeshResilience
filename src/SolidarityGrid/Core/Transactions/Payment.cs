namespace SolidarityGrid.Core.Transactions;

public sealed record Payment(Guid TransactionId, string IdempotencyKey, string RequestFingerprint,
    int DurationSeconds, string Status, string? AssignedNodeId, int Attempt, DateTimeOffset CreatedAt,
    DateTimeOffset? CompletedAt = null, string? Result = null);

public sealed record AcceptPayment(int Version, Guid TransactionId, string IdempotencyKey,
    string RequestFingerprint, int DurationSeconds, DateTimeOffset CreatedAt);

public sealed record AssignPayment(int Version, Guid TransactionId, string AssignedNodeId, int Attempt,
    DateTimeOffset AssignedAt);

public sealed record CompletePayment(int Version, Guid TransactionId, string AssignedNodeId, int Attempt,
    DateTimeOffset CompletedAt, string Result);

public sealed record ReassignPayment(int Version, Guid TransactionId, int ExpectedAttempt,
    string ExpectedAssignedNodeId, string NewAssignedNodeId, int NewAttempt, DateTimeOffset ReassignedAt);

public sealed record PaymentCommandEnvelope(int Version, string Type, System.Text.Json.JsonElement Payload);
