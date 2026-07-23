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

using System;
using System.Threading;
using System.Threading.Tasks;
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
using StackExchange.Redis;

namespace cartservice.messaging
{
    public interface ICartMessagingHealth
    {
        bool Ready { get; }
    }

    public sealed class NatsOutboxRelay : BackgroundService, ICartMessagingHealth
    {
        private readonly IConfiguration _configuration;
        private readonly ICartCommandStore _cartStore;
        private readonly IDatabase _database;
        private readonly ILogger<NatsOutboxRelay> _logger;
        private volatile bool _consumerEstablished;
        private volatile bool _ready;

        public NatsOutboxRelay(IConfiguration configuration, IConnectionMultiplexer connection,
            ICartCommandStore cartStore, ILogger<NatsOutboxRelay> logger)
        {
            _configuration = configuration;
            _cartStore = cartStore;
            _database = connection.GetDatabase();
            _logger = logger;
        }

        public bool Ready => !_configuration.GetValue("NATS_REQUIRED", false) || _ready;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            if (!_configuration.GetValue("NATS_REQUIRED", false)) return;
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

            await Task.WhenAll(
                RelayOutboxAsync(jetStream, stoppingToken),
                ConsumeCommandsAsync(consumer, stoppingToken));
            _consumerEstablished = false;
            _ready = false;
        }

        private async Task RelayOutboxAsync(INatsJSContext jetStream, CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var pending = await _database.SortedSetRangeByRankAsync(
                    RedisOutboxCartStore.PendingKey, 0, 99, Order.Ascending);
                if (pending.Length == 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(200), stoppingToken);
                    continue;
                }

                foreach (var value in pending)
                {
                    if (stoppingToken.IsCancellationRequested) break;
                    var messageId = value.ToString();
                    var correlationId = "unknown";
                    var topic = "unknown";
                    var serialized = await _database.HashGetAsync(RedisOutboxCartStore.OutboxKey, messageId);
                    if (!serialized.HasValue)
                    {
                        await _database.SortedSetRemoveAsync(RedisOutboxCartStore.PendingKey, messageId);
                        continue;
                    }

                    try
                    {
                        var record = OutboxRecord.Parse(messageId, serialized.ToString());
                        topic = record.Subject;
                        (correlationId, _) = MessageContext(record.Data);
                        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                        timeout.CancelAfter(Duration("NATS_PUBLISH_TIMEOUT", TimeSpan.FromSeconds(5)));
                        var acknowledgement = await jetStream.PublishAsync(
                            subject: record.Subject,
                            data: record.Data,
                            opts: new NatsJSPubOpts { MsgId = record.MessageId },
                            cancellationToken: timeout.Token);
                        acknowledgement.EnsureSuccess();
                        _logger.LogDebug(
                            "NATS event sent (topic={topic}, message_id={message_id}, correlation_id={correlation_id})",
                            record.Subject, record.MessageId, correlationId);

                        var transaction = _database.CreateTransaction();
                        _ = transaction.SortedSetRemoveAsync(RedisOutboxCartStore.PendingKey, messageId);
                        _ = transaction.HashDeleteAsync(RedisOutboxCartStore.OutboxKey, messageId);
                        await transaction.ExecuteAsync();
                    }
                    catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
                    {
                        _logger.LogWarning(
                            "JetStream publish timed out for {topic} ({message_id}, correlation_id={correlation_id}); retaining outbox entry",
                            topic, messageId, correlationId);
                        await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                    }
                    catch (Exception exception)
                    {
                        _logger.LogError(exception,
                            "JetStream publish failed for {topic} ({message_id}, correlation_id={correlation_id}); retaining outbox entry",
                            topic, messageId, correlationId);
                        await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                    }
                }
            }
        }

        private async Task ConsumeCommandsAsync(INatsJSConsumer consumer, CancellationToken stoppingToken)
        {
            await foreach (var message in consumer.ConsumeAsync<byte[]>(cancellationToken: stoppingToken))
            {
                var (correlationId, messageId) = MessageContext(message.Data);
                _logger.LogDebug(
                    "NATS command received (topic={topic}, message_id={message_id}, correlation_id={correlation_id})",
                    message.Subject, messageId, correlationId);
                try
                {
                    if (message.Data == null) throw new InvalidOperationException("cart command is empty");
                    var envelope = MessageEnvelope.Parser.ParseFrom(message.Data);
                    switch (message.Subject)
                    {
                        case "boutique.cmd.cart.add-item.v1":
                            if (envelope.Data == null || !envelope.Data.TryUnpack<CartAddItemCommand>(out var add))
                                throw new InvalidOperationException("cart add-item payload type is invalid");
                            await _cartStore.HandleAddItemCommandAsync(add, envelope);
                            break;
                        case "boutique.cmd.cart.clear.v1":
                            if (envelope.Data == null || !envelope.Data.TryUnpack<CartClearCommand>(out var clear))
                                throw new InvalidOperationException("cart clear payload type is invalid");
                            await _cartStore.HandleClearCommandAsync(clear, envelope);
                            break;
                        default:
                            throw new InvalidOperationException($"unsupported cart command subject {message.Subject}");
                    }
                    await message.AckAsync(cancellationToken: stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception exception)
                {
                    _logger.LogError(exception,
                        "Cart command processing failed for {topic} ({message_id}, correlation_id={correlation_id}); requesting redelivery",
                        message.Subject, messageId, correlationId);
                    await message.NakAsync(cancellationToken: stoppingToken);
                }
            }
        }

        private static (string CorrelationId, string MessageId) MessageContext(byte[] data)
        {
            try
            {
                if (data == null) return ("unknown", "unknown");
                var envelope = MessageEnvelope.Parser.ParseFrom(data);
                return (
                    string.IsNullOrWhiteSpace(envelope.CorrelationId) ? "unknown" : envelope.CorrelationId,
                    string.IsNullOrWhiteSpace(envelope.MessageId) ? "unknown" : envelope.MessageId);
            }
            catch
            {
                return ("unknown", "unknown");
            }
        }

        private string Required(string name) =>
            _configuration[name] ?? throw new InvalidOperationException($"{name} is required when NATS_REQUIRED=true");

        private TimeSpan Duration(string name, TimeSpan fallback)
        {
            var value = _configuration[name];
            if (string.IsNullOrWhiteSpace(value)) return fallback;
            if (value.EndsWith("ms", StringComparison.OrdinalIgnoreCase) &&
                double.TryParse(value[..^2], out var milliseconds)) return TimeSpan.FromMilliseconds(milliseconds);
            if (value.EndsWith("s", StringComparison.OrdinalIgnoreCase) &&
                double.TryParse(value[..^1], out var seconds)) return TimeSpan.FromSeconds(seconds);
            if (value.EndsWith("m", StringComparison.OrdinalIgnoreCase) &&
                double.TryParse(value[..^1], out var minutes)) return TimeSpan.FromMinutes(minutes);
            throw new InvalidOperationException($"invalid duration in {name}");
        }

        private int Integer(string name, int fallback) =>
            int.TryParse(_configuration[name], out var value) ? value : fallback;
    }
}
