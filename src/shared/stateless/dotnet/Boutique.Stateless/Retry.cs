// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

using StackExchange.Redis;

namespace Boutique.Stateless;

public enum RetryClass
{
    Permanent,
    Conflict,
    Dependency
}

public static class Retry
{
    public static RetryClass Classify(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        if (exception is AggregateConflictException)
        {
            return RetryClass.Conflict;
        }
        if (exception is OperationCanceledException or ArgumentException)
        {
            return RetryClass.Permanent;
        }
        if (exception is RedisConnectionException or RedisTimeoutException)
        {
            return RetryClass.Dependency;
        }
        var message = exception.Message.ToUpperInvariant();
        if (new[] { "MOVED ", "ASK ", "TRYAGAIN", "CLUSTERDOWN", "LOADING ",
                    "CONNECTION RESET", "CONNECTION REFUSED", "I/O TIMEOUT" }
            .Any(message.Contains))
        {
            return RetryClass.Dependency;
        }
        if (new[] { "WRONG LAST SEQUENCE", "WRONG LAST SEQ", "REVISION MISMATCH" }
            .Any(message.Contains))
        {
            return RetryClass.Conflict;
        }
        return RetryClass.Permanent;
    }

    public static TimeSpan Backoff(int attempt, TimeSpan minimum, TimeSpan maximum)
    {
        attempt = Math.Max(attempt, 0);
        if (minimum <= TimeSpan.Zero)
        {
            minimum = TimeSpan.FromMilliseconds(1);
        }
        if (maximum < minimum)
        {
            maximum = minimum;
        }
        var capMilliseconds = minimum.TotalMilliseconds;
        for (var step = 0; step < attempt && capMilliseconds < maximum.TotalMilliseconds; step++)
        {
            capMilliseconds = Math.Min(capMilliseconds * 2, maximum.TotalMilliseconds);
        }
        return TimeSpan.FromMilliseconds(Random.Shared.NextDouble() * capMilliseconds);
    }
}
