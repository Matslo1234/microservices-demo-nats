// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Boutique.Commands.V1;
using Boutique.Common.V1;
using cartservice.cartstore;
using Google.Protobuf;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Net;
using System.Diagnostics;

namespace cartservice.messaging;

public interface ICartMessagingHealth
{
    bool Ready { get; }
}

public sealed class NatsCartCommandWorker : BackgroundService, ICartMessagingHealth
{
    private static readonly TimeSpan InitializationRetryDelay = TimeSpan.FromSeconds(1);
    private const int DefaultCommandConcurrency = 8;
    private const int MaximumCommandConcurrency = 32;
    private const int QueueCapacityPerPartition = 4;

    private readonly IConfiguration _configuration;
    private readonly CartCommandProcessor _processor;
    private readonly ICatalogProjection _catalog;
    private readonly ILogger<NatsCartCommandWorker> _logger;
    private volatile bool _consumerEstablished;
    private volatile bool _ready;

    public NatsCartCommandWorker(
        IConfiguration configuration,
        CartCommandProcessor processor,
        ICatalogProjection catalog,
        ILogger<NatsCartCommandWorker> logger)
    {
        _configuration = configuration;
        _processor = processor;
        _catalog = catalog;
        _logger = logger;
    }

    public bool Ready => !_configuration.GetValue("NATS_REQUIRED", false) || _ready;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_configuration.GetValue("NATS_REQUIRED", false))
        {
            return;
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunConnectionAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                _ready = false;
                _consumerEstablished = false;
                _logger.LogWarning(
                    exception,
                    "Cart NATS initialization interrupted; retrying");
                await Task.Delay(InitializationRetryDelay, stoppingToken);
            }
        }
    }

    private async Task RunConnectionAsync(CancellationToken stoppingToken)
    {
        var regionId = Required("REGION_ID");
        var clusterName = Required("K8S_CLUSTER_NAME");
        var opts = new NatsOpts
        {
            Url = Required("NATS_URL"),
            Name = $"cartservice/phase4/{regionId}/{clusterName}",
            AuthOpts = new NatsAuthOpts
            {
                Username = Required("NATS_USER"),
                Password = Required("NATS_PASSWORD")
            },
            TlsOpts = new NatsTlsOpts
            {
                Mode = TlsMode.Require,
                CaFile = Required("NATS_CA_FILE")
            },
            ConnectTimeout = Duration("NATS_CONNECT_TIMEOUT", TimeSpan.FromSeconds(2)),
            ReconnectWaitMin = Duration("NATS_RECONNECT_WAIT", TimeSpan.FromSeconds(2)),
            ReconnectWaitMax = Duration("NATS_RECONNECT_WAIT", TimeSpan.FromSeconds(2)),
            MaxReconnectRetry = Integer("NATS_MAX_RECONNECTS", -1),
            PingInterval = Duration("NATS_PING_INTERVAL", TimeSpan.FromSeconds(20)),
            MaxPingOut = Integer("NATS_MAX_PINGS_OUT", 2),
            RequestTimeout = Duration("NATS_PUBLISH_TIMEOUT", TimeSpan.FromSeconds(5)),
            RetryOnInitialConnect = true,
            DrainSubscriptionsOnDispose = true
        };

        await using var client = new NatsClient(opts);
        client.Connection.ConnectionOpened += (_, _) =>
        {
            _ready = _consumerEstablished;
            _logger.LogInformation("NATS connection is ready");
            return ValueTask.CompletedTask;
        };
        client.Connection.ConnectionDisconnected += (_, _) =>
        {
            _ready = false;
            _logger.LogWarning("NATS disconnected");
            return ValueTask.CompletedTask;
        };

        try
        {
            await client.ConnectAsync();
            var jetStream = client.CreateJetStreamContext();
            var catalogConsumer = await jetStream.CreateOrUpdateConsumerAsync(
                "BOUTIQUE_EVENTS",
                new ConsumerConfig("cart-catalog-v1")
                {
                    AckPolicy = ConsumerConfigAckPolicy.Explicit,
                    AckWait = TimeSpan.FromSeconds(30),
                    DeliverPolicy = ConsumerConfigDeliverPolicy.All,
                    FilterSubject = "boutique.evt.catalog.>",
                    // The snapshot-completed marker is the readiness barrier.
                    // Keep one event in flight across all replicas so it cannot
                    // overtake an earlier product projection update.
                    MaxAckPending = 1,
                    MaxDeliver = 10
                },
                stoppingToken);
            var catalogTask = ConsumeCatalogAsync(catalogConsumer, stoppingToken);
            var catalogReadyTask = _catalog.WaitUntilReadyAsync(stoppingToken);
            if (await Task.WhenAny(catalogTask, catalogReadyTask) == catalogTask)
            {
                await catalogTask;
                throw new InvalidOperationException(
                    "Cart catalog consumer stopped before the projection became ready.");
            }
            await catalogReadyTask;

            var commandConsumer = await jetStream.CreateOrUpdateConsumerAsync(
                "BOUTIQUE_COMMANDS",
                new ConsumerConfig("cart-commands-v1")
                {
                    AckPolicy = ConsumerConfigAckPolicy.Explicit,
                    AckWait = TimeSpan.FromSeconds(30),
                    DeliverPolicy = ConsumerConfigDeliverPolicy.All,
                    FilterSubject = "boutique.cmd.cart.>",
                    MaxAckPending = 256,
                    MaxDeliver = 10
                },
                stoppingToken);
            _consumerEstablished = true;
            _ready = true;
            var commandTask = ConsumeCommandsAsync(
                jetStream,
                commandConsumer,
                BoundedInteger(
                    "CART_COMMAND_CONCURRENCY",
                    DefaultCommandConcurrency,
                    1,
                    MaximumCommandConcurrency),
                stoppingToken);
            var completed = await Task.WhenAny(catalogTask, commandTask);
            await completed;
            if (!stoppingToken.IsCancellationRequested)
            {
                throw new InvalidOperationException(
                    "A cart NATS consumer stopped unexpectedly.");
            }
            await Task.WhenAll(catalogTask, commandTask);
        }
        finally
        {
            _consumerEstablished = false;
            _ready = false;
        }
    }

    private async Task ConsumeCatalogAsync(
        INatsJSConsumer consumer,
        CancellationToken stoppingToken)
    {
        _logger.LogInformation("Cart catalog projection consumer started");
        await foreach (var message in consumer.ConsumeAsync<byte[]>(
            opts: new NatsJSConsumeOpts { MaxMsgs = 1, ThresholdMsgs = 1 },
            cancellationToken: stoppingToken))
        {
            var correlationId = "unknown";
            var messageId = "unknown";
            try
            {
                if (message.Data == null)
                {
                    throw new InvalidOperationException("Catalog event is empty.");
                }
                var envelope = MessageEnvelope.Parser.ParseFrom(message.Data);
                correlationId = string.IsNullOrWhiteSpace(envelope.CorrelationId)
                    ? "unknown"
                    : envelope.CorrelationId;
                messageId = string.IsNullOrWhiteSpace(envelope.MessageId)
                    ? "unknown"
                    : envelope.MessageId;
                using var activity = CartTelemetry.StartConsumer(
                    envelope,
                    message.Subject,
                    "event");
                CartTelemetry.Inject(envelope);
                await _catalog.ApplyAsync(
                    message.Subject,
                    envelope,
                    stoppingToken);
                await message.AckAsync(cancellationToken: stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                _logger.LogError(
                    exception,
                    "Catalog projection failed for {Topic} ({MessageId}, correlation_id={CorrelationId}); requesting redelivery",
                    message.Subject,
                    messageId,
                    correlationId);
                await message.NakAsync(cancellationToken: stoppingToken);
            }
        }
    }

    private async Task ConsumeCommandsAsync(
        INatsJSContext jetStream,
        INatsJSConsumer consumer,
        int concurrency,
        CancellationToken stoppingToken)
    {
        var dispatcher = new AggregatePartitionedDispatcher<QueuedCartCommand>(
            concurrency,
            QueueCapacityPerPartition,
            (command, cancellationToken) =>
                ProcessCommandAsync(jetStream, command, cancellationToken),
            stoppingToken);
        var consumeOptions = new NatsJSConsumeOpts
        {
            MaxMsgs = concurrency * 2,
            ThresholdMsgs = concurrency
        };
        _logger.LogInformation(
            "Cart command consumer started (concurrency={Concurrency}, prefetch={Prefetch})",
            concurrency,
            consumeOptions.MaxMsgs);

        try
        {
            await foreach (var message in consumer.ConsumeAsync<byte[]>(
                opts: consumeOptions,
                cancellationToken: stoppingToken))
            {
                try
                {
                    if (message.Data == null)
                    {
                        throw new InvalidOperationException("Cart command is empty.");
                    }
                    var envelope = MessageEnvelope.Parser.ParseFrom(message.Data);
                    var aggregateId = string.IsNullOrWhiteSpace(envelope.AggregateId)
                        ? envelope.MessageId
                        : envelope.AggregateId;
                    if (string.IsNullOrWhiteSpace(aggregateId))
                    {
                        throw new InvalidOperationException(
                            "Cart command aggregate identity is missing.");
                    }
                    await dispatcher.DispatchAsync(
                        aggregateId,
                        new QueuedCartCommand(
                            message.Subject,
                            envelope,
                            async cancellationToken =>
                                await message.AckAsync(
                                    cancellationToken: cancellationToken),
                            async cancellationToken =>
                                await message.NakAsync(
                                    cancellationToken: cancellationToken)),
                        stoppingToken);
                }
                catch (OperationCanceledException) when (
                    stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception exception)
                {
                    _logger.LogError(
                        exception,
                        "Cart command could not be queued (topic={Topic}); requesting redelivery",
                        message.Subject);
                    await message.NakAsync(cancellationToken: stoppingToken);
                }
            }
        }
        finally
        {
            await dispatcher.CompleteAsync();
        }
    }

    private async Task ProcessCommandAsync(
        INatsJSContext jetStream,
        QueuedCartCommand command,
        CancellationToken stoppingToken)
    {
        var envelope = command.Envelope;
        var correlationId = string.IsNullOrWhiteSpace(envelope.CorrelationId)
            ? "unknown"
            : envelope.CorrelationId;
        var messageId = string.IsNullOrWhiteSpace(envelope.MessageId)
            ? "unknown"
            : envelope.MessageId;
        using var activity = CartTelemetry.StartConsumer(
            envelope,
            command.Subject,
            "command");
        CartTelemetry.Inject(envelope);
        if (_logger.IsEnabled(LogLevel.Debug))
        {
            _logger.LogDebug(
                "NATS command received (topic={Topic}, message_id={MessageId}, correlation_id={CorrelationId})",
                command.Subject,
                messageId,
                correlationId);
        }

        try
        {
            switch (command.Subject)
            {
                case "boutique.cmd.cart.add-item.v1":
                    if (envelope.Data == null ||
                        !envelope.Data.TryUnpack<CartAddItemCommand>(out var add))
                    {
                        throw new InvalidOperationException(
                            "Cart add-item payload type is invalid.");
                    }
                    await _processor.HandleAddItemAsync(
                        add,
                        envelope,
                        (result, cancellationToken) =>
                            PublishResultAsync(
                                jetStream,
                                result,
                                correlationId,
                                cancellationToken),
                        stoppingToken);
                    break;
                case "boutique.cmd.cart.clear.v1":
                    if (envelope.Data == null ||
                        !envelope.Data.TryUnpack<CartClearCommand>(out var clear))
                    {
                        throw new InvalidOperationException(
                            "Cart clear payload type is invalid.");
                    }
                    await _processor.HandleClearAsync(
                        clear,
                        envelope,
                        (result, cancellationToken) =>
                            PublishResultAsync(
                                jetStream,
                                result,
                                correlationId,
                                cancellationToken),
                        stoppingToken);
                    break;
                default:
                    throw new InvalidOperationException(
                        $"Unsupported cart command subject {command.Subject}.");
            }
            await command.Ack(stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception exception)
        {
            activity?.SetStatus(ActivityStatusCode.Error, "operation failed");
            _logger.LogError(
                exception,
                "Cart command processing failed for {Topic} ({MessageId}, correlation_id={CorrelationId}); requesting redelivery",
                command.Subject,
                messageId,
                correlationId);
            try
            {
                await command.Nak(stoppingToken);
            }
            catch (OperationCanceledException) when (
                stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception nakException)
            {
                _logger.LogError(
                    nakException,
                    "Cart command redelivery request failed for {Topic} ({MessageId}, correlation_id={CorrelationId})",
                    command.Subject,
                    messageId,
                    correlationId);
            }
        }
    }

    private async Task PublishResultAsync(
        INatsJSContext jetStream,
        CartStoredResult result,
        string correlationId,
        CancellationToken stoppingToken)
    {
        using var activity = CartTelemetry.StartProducer(
            result.Subject,
            result.MessageId,
            correlationId);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        timeout.CancelAfter(Duration("NATS_PUBLISH_TIMEOUT", TimeSpan.FromSeconds(5)));
        var envelope = MessageEnvelope.Parser.ParseFrom(result.Data.ToArray());
        CartTelemetry.Inject(envelope);
        var data = envelope.ToByteArray();
        var acknowledgement = await jetStream.PublishAsync(
            subject: result.Subject,
            data: data,
            opts: new NatsJSPubOpts { MsgId = result.MessageId },
            cancellationToken: timeout.Token);
        acknowledgement.EnsureSuccess();
        if (_logger.IsEnabled(LogLevel.Debug))
        {
            _logger.LogDebug(
                "NATS event sent (topic={Topic}, message_id={MessageId}, correlation_id={CorrelationId})",
                result.Subject,
                result.MessageId,
                correlationId);
        }
    }

    private string Required(string name) =>
        _configuration[name] ??
        throw new InvalidOperationException(
            $"{name} is required when NATS_REQUIRED=true");

    private TimeSpan Duration(string name, TimeSpan fallback)
    {
        var value = _configuration[name];
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }
        if (value.EndsWith("ms", StringComparison.OrdinalIgnoreCase) &&
            double.TryParse(value[..^2], out var milliseconds))
        {
            return TimeSpan.FromMilliseconds(milliseconds);
        }
        if (value.EndsWith("s", StringComparison.OrdinalIgnoreCase) &&
            double.TryParse(value[..^1], out var seconds))
        {
            return TimeSpan.FromSeconds(seconds);
        }
        if (value.EndsWith("m", StringComparison.OrdinalIgnoreCase) &&
            double.TryParse(value[..^1], out var minutes))
        {
            return TimeSpan.FromMinutes(minutes);
        }
        throw new InvalidOperationException($"Invalid duration in {name}.");
    }

    private int Integer(string name, int fallback) =>
        int.TryParse(_configuration[name], out var value) ? value : fallback;

    private int BoundedInteger(
        string name,
        int fallback,
        int minimum,
        int maximum)
    {
        var value = Integer(name, fallback);
        if (value < minimum || value > maximum)
        {
            throw new InvalidOperationException(
                $"{name} must be between {minimum} and {maximum}.");
        }
        return value;
    }

    private sealed record QueuedCartCommand(
        string Subject,
        MessageEnvelope Envelope,
        Func<CancellationToken, Task> Ack,
        Func<CancellationToken, Task> Nak);
}
