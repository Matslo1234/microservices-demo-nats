// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

using System.Collections.Concurrent;
using System.Text.Json;
using Boutique.Commands.V1;
using Boutique.Common.V1;
using Boutique.Events.V1;
using Boutique.Stateless;
using cartservice.cartstore;
using cartservice.logging;
using cartservice.messaging;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;
using Xunit;

namespace cartservice.tests;

public sealed class CartServiceTests
{
    private static readonly DateTime CommandTime =
        DateTime.SpecifyKind(new DateTime(2026, 7, 27, 12, 0, 0), DateTimeKind.Utc);

    [Fact]
    public async Task DuplicateAfterPublishTimeoutRepublishesExactStoredResult()
    {
        var backend = new FakeAtomicAggregateStore();
        var first = Worker(backend);
        var second = Worker(backend);
        var command = AddCommand("command-duplicate", "user-duplicate", 0, 2);
        var envelope = Envelope(command, "boutique.cart.AddItem.v1", command.ExpectedCartVersion);
        CartStoredResult? firstAttempt = null;

        await Assert.ThrowsAsync<TimeoutException>(() =>
            first.Processor.HandleAddItemAsync(
                command,
                envelope,
                (result, _) =>
                {
                    firstAttempt = result;
                    throw new TimeoutException("ambiguous JetStream acknowledgement");
                }));

        CartStoredResult? recovered = null;
        var outcome = await second.Processor.HandleAddItemAsync(
            command,
            envelope,
            (result, _) =>
            {
                recovered = result;
                return Task.CompletedTask;
            });

        Assert.True(outcome.Duplicate);
        Assert.NotNull(firstAttempt);
        Assert.NotNull(recovered);
        Assert.Equal(firstAttempt!.Subject, recovered!.Subject);
        Assert.Equal(firstAttempt.MessageId, recovered.MessageId);
        Assert.Equal(firstAttempt.Data.ToArray(), recovered.Data.ToArray());
        Assert.Equal(
            ResultMessageIds.Derive(command.CommandId, "cart.mutation"),
            recovered.MessageId);
        var cart = await second.Store.GetCartAsync(command.UserId);
        Assert.Equal(2, Assert.Single(cart.Items).Quantity);
        Assert.Equal(1UL, (await backend.LoadAsync(command.UserId)).Version);
    }

    [Fact]
    public async Task NewCommandUsesOneAggregateLoadAndOneAtomicCommit()
    {
        var backend = new FakeAtomicAggregateStore();
        var worker = Worker(backend);
        var command = AddCommand("command-round-trips", "user-round-trips", 0, 1);

        await worker.Processor.HandleAddItemAsync(
            command,
            Envelope(command, "boutique.cart.AddItem.v1", 0),
            (_, _) => Task.CompletedTask);

        Assert.Equal(1, backend.LoadCalls);
        Assert.Equal(0, backend.LoadResultCalls);
        Assert.Equal(1, backend.CommitCalls);
    }

    [Fact]
    public async Task ConcurrentExpectedVersionConflictCommitsOneMutationAndOneRejection()
    {
        var backend = new FakeAtomicAggregateStore();
        var workers = new[] { Worker(backend), Worker(backend) };
        var commands = new[]
        {
            AddCommand("command-conflict-a", "user-conflict", 0, 2),
            AddCommand("command-conflict-b", "user-conflict", 0, 7)
        };
        var published = new ConcurrentBag<CartStoredResult>();

        await Task.WhenAll(commands.Select((command, index) =>
            workers[index].Processor.HandleAddItemAsync(
                command,
                Envelope(command, "boutique.cart.AddItem.v1", 0),
                (result, _) =>
                {
                    published.Add(result);
                    return Task.CompletedTask;
                })));

        Assert.Equal(2, published.Count);
        var envelopes = published
            .Select(result => MessageEnvelope.Parser.ParseFrom(result.Data.Span))
            .ToArray();
        Assert.Single(envelopes, envelope =>
            envelope.MessageType == "boutique.cart.ItemAdded.v1");
        var rejectedEnvelope = Assert.Single(envelopes, envelope =>
            envelope.MessageType == "boutique.cart.CommandRejected.v1");
        Assert.True(rejectedEnvelope.Data.TryUnpack<CartCommandRejectedEvent>(out var rejected));
        Assert.Equal("CART_VERSION_CONFLICT", rejected.Failure.Code);
        Assert.Equal(1UL, rejected.CurrentCartVersion);

        var cart = await workers[0].Store.GetCartAsync("user-conflict");
        Assert.Equal(1UL, (await backend.LoadAsync("user-conflict")).Version);
        Assert.Contains(Assert.Single(cart.Items).Quantity, new[] { 2, 7 });
    }

    [Fact]
    public async Task AmbiguousRedisFailoverRecoversTheCommittedJournal()
    {
        var backend = new FakeAtomicAggregateStore { FailAfterCommitOnce = true };
        var worker = Worker(backend);
        var command = AddCommand("command-failover", "user-failover", 0, 3);
        var publications = new List<CartStoredResult>();

        var outcome = await worker.Processor.HandleAddItemAsync(
            command,
            Envelope(command, "boutique.cart.AddItem.v1", 0),
            (result, _) =>
            {
                publications.Add(result);
                return Task.CompletedTask;
            });

        Assert.True(outcome.Duplicate);
        Assert.Equal(1, outcome.DependencyRetries);
        Assert.Single(publications);
        Assert.Equal(1UL, (await backend.LoadAsync(command.UserId)).Version);
        Assert.Equal(3, Assert.Single((await worker.Store.GetCartAsync(command.UserId)).Items).Quantity);
        var metrics = worker.Metrics.Render(true, true);
        Assert.Contains("boutique_cart_redis_retries_total{service=\"cartservice\"} 1", metrics);
        Assert.Contains("boutique_result_republishes_total{service=\"cartservice\"} 1", metrics);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(10)]
    public async Task ReplicaMatrixPreservesOneLogicalTransitionUnderLoad(int replicaCount)
    {
        const int aggregateCount = 120;
        const int deliveriesPerCommand = 3;
        var backend = new FakeAtomicAggregateStore();
        var workers = Enumerable.Range(0, replicaCount)
            .Select(_ => Worker(backend))
            .ToArray();
        var publications = new ConcurrentBag<(string UserId, CartStoredResult Result)>();
        var deliveries = new List<Task<CartCommandCommit>>();

        for (var aggregate = 0; aggregate < aggregateCount; aggregate++)
        {
            var userId = $"scale-{replicaCount}-user-{aggregate:D4}";
            var command = AddCommand(
                $"scale-{replicaCount}-command-{aggregate:D4}",
                userId,
                0,
                1);
            var envelope = Envelope(command, "boutique.cart.AddItem.v1", 0);
            for (var delivery = 0; delivery < deliveriesPerCommand; delivery++)
            {
                var worker = workers[(aggregate + delivery) % workers.Length];
                deliveries.Add(worker.Processor.HandleAddItemAsync(
                    command,
                    envelope,
                    (result, _) =>
                    {
                        publications.Add((userId, result));
                        return Task.CompletedTask;
                    }));
            }
        }

        var outcomes = await Task.WhenAll(deliveries);
        Assert.Equal(
            aggregateCount * (deliveriesPerCommand - 1),
            outcomes.Count(outcome => outcome.Duplicate));
        Assert.Equal(aggregateCount * deliveriesPerCommand, publications.Count);
        Assert.Equal(
            aggregateCount,
            publications.Select(item => item.Result.MessageId).Distinct().Count());

        foreach (var group in publications.GroupBy(item => item.UserId))
        {
            Assert.Single(group.Select(item => item.Result.MessageId).Distinct());
            Assert.Single(group.Select(item => Convert.ToBase64String(item.Result.Data.Span)).Distinct());
            var cart = await workers[0].Store.GetCartAsync(group.Key);
            Assert.Equal(1, Assert.Single(cart.Items).Quantity);
            Assert.Equal(1UL, (await backend.LoadAsync(group.Key)).Version);
        }
    }

    [Fact]
    public async Task InvalidFirstCommandStoresAStableVersionZeroRejection()
    {
        var backend = new FakeAtomicAggregateStore();
        var worker = Worker(backend);
        var command = AddCommand("command-invalid", "user-invalid", 0, 0);
        CartStoredResult? published = null;

        var first = await worker.Processor.HandleAddItemAsync(
            command,
            Envelope(command, "boutique.cart.AddItem.v1", 0),
            (result, _) =>
            {
                published = result;
                return Task.CompletedTask;
            });
        var second = await worker.Processor.HandleAddItemAsync(
            command,
            Envelope(command, "boutique.cart.AddItem.v1", 0),
            (_, _) => Task.CompletedTask);

        Assert.False(first.Duplicate);
        Assert.True(second.Duplicate);
        Assert.Equal(0UL, (await backend.LoadAsync(command.UserId)).Version);
        var resultEnvelope = MessageEnvelope.Parser.ParseFrom(published!.Data.Span);
        Assert.Equal(0UL, resultEnvelope.AggregateVersion);
        Assert.True(resultEnvelope.Data.TryUnpack<CartCommandRejectedEvent>(out var rejected));
        Assert.Equal("INVALID_ARGUMENT", rejected.Failure.Code);
    }

    [Fact]
    public async Task LiveRedisClusterContinuesAfterReplicaTakeover()
    {
        var address = Environment.GetEnvironmentVariable("CART_REDIS_TEST_ADDR");
        if (string.IsNullOrWhiteSpace(address))
        {
            return;
        }

        var options = ConfigurationOptions.Parse(address);
        options.AbortOnConnectFail = false;
        options.AllowAdmin = true;
        options.ConnectTimeout = 5_000;
        options.AsyncTimeout = 5_000;
        options.SyncTimeout = 5_000;
        options.ReconnectRetryPolicy = new ExponentialRetry(100, 1_000);
        await using var connection = await ConnectionMultiplexer.ConnectAsync(options);
        var prefix = $"cart:phase4-live:{Guid.NewGuid():N}";
        var metrics = new CartMetrics();
        var store = new RedisAggregateCartStore(
            new RedisAtomicAggregateStore(connection.GetDatabase(), prefix),
            metrics,
            NullLogger<RedisAggregateCartStore>.Instance,
            connection);
        var processor = new CartCommandProcessor(store, metrics);

        await ApplyWave(processor, 0, 90);
        var replica = connection.GetServers()
            .FirstOrDefault(server => server.IsConnected && server.IsReplica);
        Assert.NotNull(replica);
        await replica!.ExecuteAsync("CLUSTER", "FAILOVER", "TAKEOVER");

        await ApplyWave(processor, 90, 180);
        for (var index = 0; index < 180; index++)
        {
            var cart = await store.GetCartAsync($"live-user-{index:D4}");
            Assert.Equal(1, Assert.Single(cart.Items).Quantity);
        }
    }

    [Fact]
    public void ConsoleFormatterWritesJsonWithSeverityAndStructuredFields()
    {
        var formatter = new SeverityJsonConsoleFormatter();
        var state = new List<KeyValuePair<string, object>>
        {
            new("topic", "boutique.cmd.cart.clear.v1"),
            new("attempt", 2),
            new("{OriginalFormat}", "Command received from {topic}")
        };
        var entry = new LogEntry<List<KeyValuePair<string, object>>>(
            LogLevel.Warning,
            "cartservice.tests",
            new EventId(7, "CommandReceived"),
            state,
            null,
            (_, _) => "Command received from boutique.cmd.cart.clear.v1");
        using var output = new StringWriter();

        formatter.Write(in entry, null!, output);

        using var document = JsonDocument.Parse(output.ToString());
        var root = document.RootElement;
        Assert.Equal("WARNING", root.GetProperty("severity").GetString());
        Assert.Equal("cartservice.tests", root.GetProperty("name").GetString());
        Assert.Equal("boutique.cmd.cart.clear.v1", root.GetProperty("topic").GetString());
        Assert.Equal(2, root.GetProperty("attempt").GetInt32());
    }

    private static WorkerFixture Worker(FakeAtomicAggregateStore backend)
    {
        var metrics = new CartMetrics();
        var store = new RedisAggregateCartStore(
            backend,
            metrics,
            NullLogger<RedisAggregateCartStore>.Instance);
        return new WorkerFixture(
            store,
            new CartCommandProcessor(store, metrics),
            metrics);
    }

    private static async Task ApplyWave(
        CartCommandProcessor processor,
        int start,
        int end)
    {
        await Task.WhenAll(Enumerable.Range(start, end - start).Select(index =>
        {
            var command = AddCommand(
                $"live-command-{index:D4}",
                $"live-user-{index:D4}",
                0,
                1);
            return processor.HandleAddItemAsync(
                command,
                Envelope(command, "boutique.cart.AddItem.v1", 0),
                (_, _) => Task.CompletedTask);
        }));
    }

    private static CartAddItemCommand AddCommand(
        string commandId,
        string userId,
        ulong expectedVersion,
        int quantity) =>
        new()
        {
            CommandId = commandId,
            UserId = userId,
            ProductId = "OLJCESPC7Z",
            Quantity = quantity,
            ExpectedCartVersion = expectedVersion
        };

    private static MessageEnvelope Envelope<T>(
        T command,
        string messageType,
        ulong expectedVersion)
        where T : IMessage<T>
    {
        var commandId = command switch
        {
            CartAddItemCommand add => add.CommandId,
            CartClearCommand clear => clear.CommandId,
            _ => throw new ArgumentOutOfRangeException(nameof(command))
        };
        var userId = command switch
        {
            CartAddItemCommand add => add.UserId,
            CartClearCommand clear => clear.UserId,
            _ => throw new ArgumentOutOfRangeException(nameof(command))
        };
        return new MessageEnvelope
        {
            MessageId = commandId,
            MessageType = messageType,
            SchemaVersion = 1,
            OccurredAt = Timestamp.FromDateTime(CommandTime),
            Producer = "cartservice.tests",
            AggregateType = "cart",
            AggregateId = userId,
            AggregateVersion = expectedVersion,
            CorrelationId = commandId,
            Traceparent = "00-00000000000000000000000000000001-0000000000000001-01",
            Data = Any.Pack(command)
        };
    }

    private sealed record WorkerFixture(
        RedisAggregateCartStore Store,
        CartCommandProcessor Processor,
        CartMetrics Metrics);

    private sealed class FakeAtomicAggregateStore : IAtomicAggregateStore
    {
        private readonly object _gate = new();
        private readonly Dictionary<string, Aggregate> _aggregates = new();
        private readonly Dictionary<(string AggregateId, string InputId), byte[]> _journals = new();

        public bool FailAfterCommitOnce { get; init; }
        public int LoadCalls { get; private set; }
        public int LoadResultCalls { get; private set; }
        public int CommitCalls { get; private set; }
        private bool _failedAfterCommit;

        public Task<AtomicAggregateSnapshot> LoadAsync(
            string aggregateId,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            lock (_gate)
            {
                LoadCalls++;
                var aggregate = Current(aggregateId);
                return Task.FromResult(new AtomicAggregateSnapshot(
                    aggregate.Version,
                    aggregate.State.ToArray()));
            }
        }

        public Task<ReadOnlyMemory<byte>?> LoadResultAsync(
            string aggregateId,
            string inputMessageId,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            lock (_gate)
            {
                LoadResultCalls++;
                if (_journals.TryGetValue(
                    (aggregateId, inputMessageId),
                    out var journal))
                {
                    return Task.FromResult<ReadOnlyMemory<byte>?>(journal.ToArray());
                }
                return Task.FromResult<ReadOnlyMemory<byte>?>(null);
            }
        }

        public Task<AtomicCommitOutcome> CommitAsync(
            AtomicCommitRequest request,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            lock (_gate)
            {
                CommitCalls++;
                var key = (request.AggregateId, request.InputMessageId);
                var current = Current(request.AggregateId);
                if (_journals.TryGetValue(key, out var existing))
                {
                    return Task.FromResult(new AtomicCommitOutcome(
                        current.Version,
                        existing.ToArray(),
                        true));
                }
                if (current.Version != request.ExpectedVersion)
                {
                    throw new AggregateConflictException(
                        request.ExpectedVersion,
                        current.Version);
                }

                var nextVersion = current.Version;
                if (request.AdvanceVersion)
                {
                    nextVersion++;
                    _aggregates[request.AggregateId] = new Aggregate(
                        nextVersion,
                        request.NextState.ToArray());
                }
                var journal = request.Journal.ToArray();
                _journals[key] = journal;
                if (FailAfterCommitOnce && !_failedAfterCommit)
                {
                    _failedAfterCommit = true;
                    throw new InvalidOperationException(
                        "CLUSTERDOWN simulated primary failover after commit");
                }
                return Task.FromResult(new AtomicCommitOutcome(
                    nextVersion,
                    journal.ToArray(),
                    false));
            }
        }

        private Aggregate Current(string aggregateId) =>
            _aggregates.TryGetValue(aggregateId, out var aggregate)
                ? aggregate
                : new Aggregate(0, []);

        private sealed record Aggregate(ulong Version, byte[] State);
    }
}
