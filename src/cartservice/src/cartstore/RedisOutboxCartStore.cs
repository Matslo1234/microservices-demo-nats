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
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Boutique.Commands.V1;
using Boutique.Common.V1;
using Boutique.Events.V1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Hipstershop;
using StackExchange.Redis;

namespace cartservice.cartstore
{
    public sealed class RedisOutboxCartStore : IEventOutboxCartStore, ICartCommandStore
    {
        internal const string PendingKey = "cart:outbox:pending";
        internal const string OutboxKey = "cart:outbox:messages";

        private readonly IConnectionMultiplexer _connection;
        private readonly IDatabase _database;

        public RedisOutboxCartStore(IConnectionMultiplexer connection)
        {
            _connection = connection;
            _database = connection.GetDatabase();
        }

        public Task AddItemAsync(string userId, string productId, int quantity) =>
            AddItemWithEventAsync(userId, productId, quantity, Guid.CreateVersion7().ToString(), string.Empty, string.Empty);

        public Task EmptyCartAsync(string userId) =>
            EmptyCartWithEventAsync(userId, Guid.CreateVersion7().ToString(), string.Empty, string.Empty);

        public Task AddItemWithEventAsync(string userId, string productId, int quantity,
            string requestId, string traceparent, string tracestate)
        {
            if (quantity <= 0) throw new ArgumentOutOfRangeException(nameof(quantity), "Quantity must be positive.");
            return MutateAsync(userId, requestId, null, traceparent, tracestate, string.Empty, requestId,
                (cart, version) =>
            {
                var existing = cart.Items.SingleOrDefault(item => item.ProductId == productId);
                if (existing == null)
                {
                    existing = new CartItem { ProductId = productId, Quantity = quantity };
                    cart.Items.Add(existing);
                }
                else
                {
                    existing.Quantity += quantity;
                }
                var eventPayload = new CartItemAddedEvent
                {
                    CommandId = requestId,
                    UserId = userId,
                    ProductId = productId,
                    QuantityDelta = quantity,
                    ResultingQuantity = existing.Quantity,
                    Cart = Snapshot(cart, version)
                };
                return EventRecord(
                    "boutique.evt.cart.item-added.v1",
                    "boutique.cart.ItemAdded.v1",
                    userId,
                    version,
                    requestId,
                    string.Empty,
                    requestId,
                    traceparent,
                    tracestate,
                    eventPayload);
            });
        }

        public Task EmptyCartWithEventAsync(string userId, string requestId,
            string traceparent, string tracestate)
        {
            return MutateAsync(userId, requestId, null, traceparent, tracestate, string.Empty, requestId,
                (cart, version) =>
            {
                var priorIds = cart.Items.Select(item => item.ProductId).ToArray();
                cart.Items.Clear();
                var eventPayload = new CartClearedEvent
                {
                    CommandId = requestId,
                    UserId = userId,
                    Cart = Snapshot(cart, version),
                    Reason = "local-store-write"
                };
                eventPayload.PriorProductIds.AddRange(priorIds);
                return EventRecord(
                    "boutique.evt.cart.cleared.v1",
                    "boutique.cart.Cleared.v1",
                    userId,
                    version,
                    requestId,
                    string.Empty,
                    requestId,
                    traceparent,
                    tracestate,
                    eventPayload);
            });
        }

        public Task HandleAddItemCommandAsync(CartAddItemCommand command, MessageEnvelope envelope)
        {
            ValidateCommand(command.CommandId, command.UserId, envelope);
            if (string.IsNullOrWhiteSpace(command.ProductId) || command.Quantity <= 0)
            {
                return RejectAsync(command.UserId, command.CommandId, envelope, "INVALID_ARGUMENT",
                    "Product ID and a positive quantity are required.");
            }
            return MutateAsync(command.UserId, command.CommandId, command.ExpectedCartVersion,
                envelope.Traceparent, envelope.Tracestate, envelope.MessageId, envelope.CorrelationId,
                (cart, version) =>
                {
                    var existing = cart.Items.SingleOrDefault(item => item.ProductId == command.ProductId);
                    if (existing == null)
                    {
                        existing = new CartItem { ProductId = command.ProductId, Quantity = command.Quantity };
                        cart.Items.Add(existing);
                    }
                    else
                    {
                        existing.Quantity += command.Quantity;
                    }
                    var payload = new CartItemAddedEvent
                    {
                        CommandId = command.CommandId,
                        UserId = command.UserId,
                        ProductId = command.ProductId,
                        QuantityDelta = command.Quantity,
                        ResultingQuantity = existing.Quantity,
                        Cart = Snapshot(cart, version)
                    };
                    return EventRecord("boutique.evt.cart.item-added.v1", "boutique.cart.ItemAdded.v1",
                        command.UserId, version, command.CommandId, envelope.MessageId,
                        envelope.CorrelationId, envelope.Traceparent, envelope.Tracestate, payload);
                });
        }

        public Task HandleClearCommandAsync(CartClearCommand command, MessageEnvelope envelope)
        {
            ValidateClearCommand(command, envelope);
            return MutateAsync(command.UserId, command.CommandId, command.ExpectedCartVersion,
                envelope.Traceparent, envelope.Tracestate, envelope.MessageId, envelope.CorrelationId,
                (cart, version) =>
                {
                    var priorIds = cart.Items.Select(item => item.ProductId).ToArray();
                    cart.Items.Clear();
                    var payload = new CartClearedEvent
                    {
                        CommandId = command.CommandId,
                        UserId = command.UserId,
                        Cart = Snapshot(cart, version),
                        Reason = string.IsNullOrWhiteSpace(command.Reason) ? "user-request" : command.Reason,
                        OrderId = command.OrderId
                    };
                    payload.PriorProductIds.AddRange(priorIds);
                    return EventRecord("boutique.evt.cart.cleared.v1", "boutique.cart.Cleared.v1",
                        command.UserId, version, command.CommandId, envelope.MessageId,
                        envelope.CorrelationId, envelope.Traceparent, envelope.Tracestate, payload);
                });
        }

        public async Task<Cart> GetCartAsync(string userId)
        {
            try
            {
                var current = await _database.StringGetAsync(DataKey(userId));
                if (current.HasValue) return Cart.Parser.ParseFrom((byte[])current!);

                // Read the pre-Phase-2 cache key during rolling deployment so an
                // existing cart is not hidden before its next mutation.
                var legacy = await _database.HashGetAsync(userId, "data");
                if (legacy.HasValue) return Cart.Parser.ParseFrom((byte[])legacy!);
                return new Cart();
            }
            catch (Exception exception)
            {
                throw StorageException(exception);
            }
        }

        public bool Ping() => _connection.IsConnected;

        private async Task MutateAsync(string userId, string requestId, ulong? expectedVersion,
            string traceparent, string tracestate, string causationId, string correlationId,
            Func<Cart, ulong, OutboxRecord> createRecord)
        {
            if (string.IsNullOrWhiteSpace(userId))
                throw new ArgumentException("User ID is required.", nameof(userId));

            var dataKey = DataKey(userId);
            var versionKey = VersionKey(userId);
            var inboxKey = InboxKey(userId, requestId);
            try
            {
                for (var attempt = 0; attempt < 20; attempt++)
                {
                    if (await _database.KeyExistsAsync(inboxKey)) return;

                    var reads = await Task.WhenAll(
                        _database.StringGetAsync(dataKey),
                        _database.StringGetAsync(versionKey));
                    var cart = reads[0].HasValue
                        ? Cart.Parser.ParseFrom((byte[])reads[0]!)
                        : await ReadLegacyCartAsync(userId);
                    cart.UserId = userId;
                    var currentVersion = reads[1].HasValue ? ulong.Parse(reads[1].ToString()) : 0UL;
                    if (expectedVersion.HasValue && expectedVersion.Value != currentVersion)
                    {
                        var rejected = RejectionRecord(userId, requestId, currentVersion, causationId,
                            correlationId, traceparent, tracestate, "CART_VERSION_CONFLICT",
                            "The cart changed before this command was applied.");
                        if (await CommitOutboxOnlyAsync(versionKey, inboxKey, currentVersion, rejected)) return;
                        continue;
                    }
                    var nextVersion = checked(currentVersion + 1);
                    var record = createRecord(cart, nextVersion);
                    var serializedRecord = record.Serialize();

                    var transaction = _database.CreateTransaction();
                    transaction.AddCondition(currentVersion == 0
                        ? Condition.KeyNotExists(versionKey)
                        : Condition.StringEqual(versionKey, currentVersion.ToString()));
                    transaction.AddCondition(Condition.KeyNotExists(inboxKey));
                    _ = transaction.StringSetAsync(dataKey, cart.ToByteArray());
                    _ = transaction.StringSetAsync(versionKey, nextVersion.ToString());
                    _ = transaction.StringSetAsync(inboxKey, record.MessageId, TimeSpan.FromDays(7));
                    _ = transaction.HashSetAsync(OutboxKey, record.MessageId, serializedRecord);
                    _ = transaction.SortedSetAddAsync(PendingKey, record.MessageId,
                        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                    if (await transaction.ExecuteAsync()) return;
                }
                throw new InvalidOperationException("cart update conflicted too many times");
            }
            catch (Exception exception)
            {
                throw StorageException(exception);
            }
        }

        private async Task RejectAsync(string userId, string requestId, MessageEnvelope envelope,
            string code, string safeMessage)
        {
            if (string.IsNullOrWhiteSpace(userId) || string.IsNullOrWhiteSpace(requestId))
                throw new InvalidOperationException("command user ID and command ID are required");
            var versionKey = VersionKey(userId);
            var inboxKey = InboxKey(userId, requestId);
            for (var attempt = 0; attempt < 20; attempt++)
            {
                if (await _database.KeyExistsAsync(inboxKey)) return;
                var storedVersion = await _database.StringGetAsync(versionKey);
                var currentVersion = storedVersion.HasValue ? ulong.Parse(storedVersion.ToString()) : 0UL;
                var rejected = RejectionRecord(userId, requestId, currentVersion, envelope.MessageId,
                    envelope.CorrelationId, envelope.Traceparent, envelope.Tracestate, code, safeMessage);
                if (await CommitOutboxOnlyAsync(versionKey, inboxKey, currentVersion, rejected)) return;
            }
            throw new InvalidOperationException("cart rejection conflicted too many times");
        }

        private async Task<bool> CommitOutboxOnlyAsync(string versionKey, string inboxKey,
            ulong currentVersion, OutboxRecord record)
        {
            var transaction = _database.CreateTransaction();
            transaction.AddCondition(currentVersion == 0
                ? Condition.KeyNotExists(versionKey)
                : Condition.StringEqual(versionKey, currentVersion.ToString()));
            transaction.AddCondition(Condition.KeyNotExists(inboxKey));
            _ = transaction.StringSetAsync(inboxKey, record.MessageId, TimeSpan.FromDays(7));
            _ = transaction.HashSetAsync(OutboxKey, record.MessageId, record.Serialize());
            _ = transaction.SortedSetAddAsync(PendingKey, record.MessageId,
                DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            return await transaction.ExecuteAsync();
        }

        private async Task<Cart> ReadLegacyCartAsync(string userId)
        {
            var legacy = await _database.HashGetAsync(userId, "data");
            return legacy.HasValue ? Cart.Parser.ParseFrom((byte[])legacy!) : new Cart();
        }

        private static OutboxRecord RejectionRecord(string userId, string commandId, ulong currentVersion,
            string causationId, string correlationId, string traceparent, string tracestate,
            string code, string safeMessage)
        {
            var payload = new CartCommandRejectedEvent
            {
                CommandId = commandId,
                UserId = userId,
                CurrentCartVersion = currentVersion,
                Failure = new Failure { Code = code, Retryable = false, SafeMessage = safeMessage }
            };
            return EventRecord("boutique.evt.cart.command-rejected.v1", "boutique.cart.CommandRejected.v1",
                userId, currentVersion, commandId, causationId, correlationId, traceparent, tracestate, payload);
        }

        private static OutboxRecord EventRecord(string subject, string messageType, string userId,
            ulong version, string requestId, string causationId, string correlationId,
            string traceparent, string tracestate, IMessage payload)
        {
            var messageId = Guid.CreateVersion7().ToString();
            var envelope = new MessageEnvelope
            {
                MessageId = messageId,
                MessageType = messageType,
                SchemaVersion = 1,
                OccurredAt = Timestamp.FromDateTime(DateTime.UtcNow),
                Producer = "cartservice/phase4",
                AggregateType = "cart",
                AggregateId = userId,
                AggregateVersion = version,
                CorrelationId = string.IsNullOrWhiteSpace(correlationId) ? requestId : correlationId,
                CausationId = causationId ?? string.Empty,
                Traceparent = traceparent ?? string.Empty,
                Tracestate = tracestate ?? string.Empty,
                Data = Any.Pack(payload)
            };
            return new OutboxRecord(subject, messageId, envelope.ToByteArray());
        }

        private static void ValidateCommand(string commandId, string userId, MessageEnvelope envelope)
        {
            if (string.IsNullOrWhiteSpace(commandId) || string.IsNullOrWhiteSpace(userId))
                throw new InvalidOperationException("command ID and user ID are required");
            if (envelope.SchemaVersion != 1 || envelope.Data == null)
                throw new InvalidOperationException("unsupported or empty command envelope");
            if (!string.Equals(commandId, envelope.MessageId, StringComparison.Ordinal))
                throw new InvalidOperationException("command ID must match envelope message ID");
            if (!string.Equals(userId, envelope.AggregateId, StringComparison.Ordinal))
                throw new InvalidOperationException("command user ID must match envelope aggregate ID");
        }

        private static void ValidateClearCommand(CartClearCommand command, MessageEnvelope envelope)
        {
            if (string.Equals(command.CommandId, envelope.MessageId, StringComparison.Ordinal))
            {
                ValidateCommand(command.CommandId, command.UserId, envelope);
                return;
            }

            // Early Phase 5 checkout releases used a stable business command ID
            // but a separate durable envelope ID. Permit only those retained
            // order-completion commands so their carts can recover; all other
            // commands continue to require matching IDs.
            if (string.IsNullOrWhiteSpace(command.CommandId) || string.IsNullOrWhiteSpace(command.UserId) ||
                envelope.SchemaVersion != 1 || envelope.Data == null ||
                command.Reason != "order-completed" || string.IsNullOrWhiteSpace(command.OrderId) ||
                envelope.Producer != "checkoutservice/phase5" || envelope.MessageType != "boutique.cart.Clear.v1" ||
                envelope.AggregateType != "cart" || envelope.AggregateId != command.UserId ||
                envelope.CorrelationId != command.OrderId)
            {
                throw new InvalidOperationException("command ID must match envelope message ID");
            }
        }

        private static CartSnapshot Snapshot(Cart cart, ulong version)
        {
            var snapshot = new CartSnapshot { UserId = cart.UserId, CartVersion = version };
            snapshot.Items.AddRange(cart.Items
                .OrderBy(item => item.ProductId)
                .Select(item => new CartLine { ProductId = item.ProductId, Quantity = item.Quantity }));
            return snapshot;
        }

        private static string DataKey(string userId) => $"cart:{userId}:data";
        private static string VersionKey(string userId) => $"cart:{userId}:version";
        private static string InboxKey(string userId, string requestId) =>
            $"cart:{userId}:inbox:{Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(requestId))).ToLowerInvariant()}";

        private static InvalidOperationException StorageException(Exception exception) =>
            new($"Can't access cart storage. {exception.Message}", exception);
    }

    public sealed record OutboxRecord(string Subject, string MessageId, byte[] Data)
    {
        public string Serialize() => $"{Subject}\n{Convert.ToBase64String(Data)}";

        public static OutboxRecord Parse(string messageId, string serialized)
        {
            var separator = serialized.IndexOf('\n');
            if (separator <= 0 || separator == serialized.Length - 1)
                throw new FormatException("invalid cart outbox record");
            return new OutboxRecord(
                serialized[..separator],
                messageId,
                Convert.FromBase64String(serialized[(separator + 1)..]));
        }
    }
}
