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

namespace cartservice.messaging;

public interface ICartMessagingHealth
{
    bool Ready { get; }
}

public sealed class NatsCartCommandWorker : BackgroundService, ICartMessagingHealth
{
    private static readonly TimeSpan InitializationRetryDelay = TimeSpan.FromSeconds(1);

    private readonly IConfiguration _configuration;
    private readonly CartCommandProcessor _processor;
    private readonly ILogger<NatsCartCommandWorker> _logger;
    private volatile bool _consumerEstablished;
    private volatile bool _ready;

    public NatsCartCommandWorker(
        IConfiguration configuration,
        CartCommandProcessor processor,
        ILogger<NatsCartCommandWorker> logger)
    {
        _configuration = configuration;
        _processor = processor;
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
        var opts = new NatsOpts
        {
            Url = Required("NATS_URL"),
            Name = "cartservice/phase4",
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
            var consumer = await jetStream.CreateOrUpdateConsumerAsync(
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
            await ConsumeCommandsAsync(jetStream, consumer, stoppingToken);
        }
        finally
        {
            _consumerEstablished = false;
            _ready = false;
        }
    }

    private async Task ConsumeCommandsAsync(
        INatsJSContext jetStream,
        INatsJSConsumer consumer,
        CancellationToken stoppingToken)
    {
        await foreach (var message in consumer.ConsumeAsync<byte[]>(
            cancellationToken: stoppingToken))
        {
            var (correlationId, messageId) = MessageContext(message.Data);
            _logger.LogDebug(
                "NATS command received (topic={Topic}, message_id={MessageId}, correlation_id={CorrelationId})",
                message.Subject,
                messageId,
                correlationId);
            try
            {
                if (message.Data == null)
                {
                    throw new InvalidOperationException("Cart command is empty.");
                }
                var envelope = MessageEnvelope.Parser.ParseFrom(message.Data);
                switch (message.Subject)
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
                                PublishResultAsync(jetStream, result, cancellationToken),
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
                                PublishResultAsync(jetStream, result, cancellationToken),
                            stoppingToken);
                        break;
                    default:
                        throw new InvalidOperationException(
                            $"Unsupported cart command subject {message.Subject}.");
                }
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
                    "Cart command processing failed for {Topic} ({MessageId}, correlation_id={CorrelationId}); requesting redelivery",
                    message.Subject,
                    messageId,
                    correlationId);
                await message.NakAsync(cancellationToken: stoppingToken);
            }
        }
    }

    private async Task PublishResultAsync(
        INatsJSContext jetStream,
        CartStoredResult result,
        CancellationToken stoppingToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        timeout.CancelAfter(Duration("NATS_PUBLISH_TIMEOUT", TimeSpan.FromSeconds(5)));
        var acknowledgement = await jetStream.PublishAsync(
            subject: result.Subject,
            data: result.Data.ToArray(),
            opts: new NatsJSPubOpts { MsgId = result.MessageId },
            cancellationToken: timeout.Token);
        acknowledgement.EnsureSuccess();
        var (correlationId, _) = MessageContext(result.Data.ToArray());
        _logger.LogDebug(
            "NATS event sent (topic={Topic}, message_id={MessageId}, correlation_id={CorrelationId})",
            result.Subject,
            result.MessageId,
            correlationId);
    }

    private static (string CorrelationId, string MessageId) MessageContext(byte[]? data)
    {
        try
        {
            if (data == null)
            {
                return ("unknown", "unknown");
            }
            var envelope = MessageEnvelope.Parser.ParseFrom(data);
            return (
                string.IsNullOrWhiteSpace(envelope.CorrelationId)
                    ? "unknown"
                    : envelope.CorrelationId,
                string.IsNullOrWhiteSpace(envelope.MessageId)
                    ? "unknown"
                    : envelope.MessageId);
        }
        catch
        {
            return ("unknown", "unknown");
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
}
