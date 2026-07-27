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

using System.Buffers.Binary;
using System.Text;
using Boutique.Commands.V1;
using Boutique.Common.V1;
using Boutique.Events.V1;
using Boutique.Stateless;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Hipstershop;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace cartservice.cartstore;

public sealed class RedisAggregateCartStore : ICartStore, ICartCommandStore
{
    private const string ResultSlot = "cart.mutation";
    private const int MaximumStoreAttempts = 8;
    private static readonly TimeSpan ResultRetention = TimeSpan.FromDays(8);
    private static readonly TimeSpan MinimumRetryDelay = TimeSpan.FromMilliseconds(5);
    private static readonly TimeSpan MaximumRetryDelay = TimeSpan.FromMilliseconds(250);

    private readonly IAtomicAggregateStore _aggregates;
    private readonly IConnectionMultiplexer? _connection;
    private readonly CartMetrics _metrics;
    private readonly ILogger<RedisAggregateCartStore> _logger;

    public RedisAggregateCartStore(
        IAtomicAggregateStore aggregates,
        CartMetrics metrics,
        ILogger<RedisAggregateCartStore> logger,
        IConnectionMultiplexer? connection = null)
    {
        _aggregates = aggregates;
        _metrics = metrics;
        _logger = logger;
        _connection = connection;
    }

    public Task<CartCommandCommit> HandleAddItemCommandAsync(
        CartAddItemCommand command,
        MessageEnvelope envelope,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(command);
        ValidateEnvelope(
            command.CommandId,
            command.UserId,
            command.ExpectedCartVersion,
            "boutique.cart.AddItem.v1",
            envelope);
        return ExecuteAsync(
            command.UserId,
            envelope,
            (cart, currentVersion) =>
            {
                if (string.IsNullOrWhiteSpace(command.ProductId) || command.Quantity <= 0)
                {
                    return Rejection(
                        cart,
                        currentVersion,
                        command.CommandId,
                        command.UserId,
                        "INVALID_ARGUMENT",
                        "Product ID and a positive quantity are required.");
                }
                if (command.ExpectedCartVersion != currentVersion)
                {
                    return Rejection(
                        cart,
                        currentVersion,
                        command.CommandId,
                        command.UserId,
                        "CART_VERSION_CONFLICT",
                        "The cart changed before this command was applied.");
                }

                var next = cart.Clone();
                var existing = next.Items.SingleOrDefault(
                    item => item.ProductId == command.ProductId);
                if (existing == null)
                {
                    existing = new CartItem
                    {
                        ProductId = command.ProductId,
                        Quantity = command.Quantity
                    };
                    next.Items.Add(existing);
                }
                else
                {
                    try
                    {
                        existing.Quantity = checked(existing.Quantity + command.Quantity);
                    }
                    catch (OverflowException)
                    {
                        return Rejection(
                            cart,
                            currentVersion,
                            command.CommandId,
                            command.UserId,
                            "CART_QUANTITY_LIMIT",
                            "The requested quantity is too large.");
                    }
                }

                var nextVersion = checked(currentVersion + 1);
                return new CartTransition(
                    next,
                    true,
                    "boutique.evt.cart.item-added.v1",
                    "boutique.cart.ItemAdded.v1",
                    new CartItemAddedEvent
                    {
                        CommandId = command.CommandId,
                        UserId = command.UserId,
                        ProductId = command.ProductId,
                        QuantityDelta = command.Quantity,
                        ResultingQuantity = existing.Quantity,
                        Cart = Snapshot(next, nextVersion)
                    },
                    nextVersion);
            },
            cancellationToken);
    }

    public Task<CartCommandCommit> HandleClearCommandAsync(
        CartClearCommand command,
        MessageEnvelope envelope,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(command);
        ValidateEnvelope(
            command.CommandId,
            command.UserId,
            command.ExpectedCartVersion,
            "boutique.cart.Clear.v1",
            envelope);
        return ExecuteAsync(
            command.UserId,
            envelope,
            (cart, currentVersion) =>
            {
                if (command.ExpectedCartVersion != currentVersion)
                {
                    return Rejection(
                        cart,
                        currentVersion,
                        command.CommandId,
                        command.UserId,
                        "CART_VERSION_CONFLICT",
                        "The cart changed before this command was applied.");
                }

                var priorIds = cart.Items
                    .Select(item => item.ProductId)
                    .OrderBy(productId => productId, StringComparer.Ordinal)
                    .ToArray();
                var next = new Cart { UserId = command.UserId };
                var nextVersion = checked(currentVersion + 1);
                var payload = new CartClearedEvent
                {
                    CommandId = command.CommandId,
                    UserId = command.UserId,
                    Cart = Snapshot(next, nextVersion),
                    Reason = string.IsNullOrWhiteSpace(command.Reason)
                        ? "user-request"
                        : command.Reason,
                    OrderId = command.OrderId
                };
                payload.PriorProductIds.AddRange(priorIds);
                return new CartTransition(
                    next,
                    true,
                    "boutique.evt.cart.cleared.v1",
                    "boutique.cart.Cleared.v1",
                    payload,
                    nextVersion);
            },
            cancellationToken);
    }

    public async Task<Cart> GetCartAsync(
        string userId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(userId);
        var snapshot = await _aggregates.LoadAsync(userId, cancellationToken);
        return ReadCart(userId, snapshot.State);
    }

    public bool Ping() => _connection?.IsConnected ?? true;

    private async Task<CartCommandCommit> ExecuteAsync(
        string userId,
        MessageEnvelope envelope,
        Func<Cart, ulong, CartTransition> transition,
        CancellationToken cancellationToken)
    {
        Exception? lastError = null;
        var conflicts = 0;
        var dependencies = 0;
        for (var attempt = 0; attempt < MaximumStoreAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var stored = await _aggregates.LoadResultAsync(
                    userId,
                    envelope.MessageId,
                    cancellationToken);
                if (stored.HasValue)
                {
                    return new CartCommandCommit(
                        CartResultJournal.Parse(stored.Value.Span),
                        true,
                        conflicts,
                        dependencies);
                }

                var current = await _aggregates.LoadAsync(userId, cancellationToken);
                var cart = ReadCart(userId, current.State);
                var selected = transition(cart, current.Version);
                var result = BuildResult(envelope, userId, selected);
                var journal = CartResultJournal.Serialize(result);
                var commit = await _aggregates.CommitAsync(
                    new AtomicCommitRequest(
                        userId,
                        envelope.MessageId,
                        current.Version,
                        selected.State.ToByteArray(),
                        journal,
                        ResultRetention,
                        selected.AdvanceVersion),
                    cancellationToken);
                return new CartCommandCommit(
                    CartResultJournal.Parse(commit.Journal.Span),
                    commit.Duplicate,
                    conflicts,
                    dependencies);
            }
            catch (Exception exception) when (
                exception is not OperationCanceledException &&
                Retry.Classify(exception) is RetryClass.Conflict or RetryClass.Dependency)
            {
                lastError = exception;
                var retryClass = Retry.Classify(exception);
                if (retryClass == RetryClass.Conflict)
                {
                    conflicts++;
                    _metrics.RecordStateConflict();
                }
                else
                {
                    dependencies++;
                    _metrics.RecordRedisRetry();
                }
                _logger.LogWarning(
                    exception,
                    "Cart Redis operation interrupted; retrying (user_id={UserId}, command_id={CommandId}, attempt={Attempt}, retry_class={RetryClass})",
                    userId,
                    envelope.MessageId,
                    attempt + 1,
                    retryClass);
                if (attempt + 1 < MaximumStoreAttempts)
                {
                    await Task.Delay(
                        Retry.Backoff(attempt, MinimumRetryDelay, MaximumRetryDelay),
                        cancellationToken);
                }
            }
        }

        throw new InvalidOperationException(
            $"Cart command {envelope.MessageId} could not be committed after {MaximumStoreAttempts} attempts.",
            lastError);
    }

    private static CartStoredResult BuildResult(
        MessageEnvelope input,
        string userId,
        CartTransition transition)
    {
        var metadata = ResultEnvelopes.CreateMetadata(
            new InputEnvelopeIdentity(
                input.MessageId,
                input.CorrelationId,
                input.Traceparent,
                input.Tracestate),
            ResultSlot,
            transition.MessageType,
            input.OccurredAt.ToDateTimeOffset(),
            "cartservice/phase4",
            "cart",
            userId,
            transition.ResultVersion);
        var result = new MessageEnvelope
        {
            MessageId = metadata.MessageId,
            MessageType = metadata.MessageType,
            SchemaVersion = metadata.SchemaVersion,
            OccurredAt = Timestamp.FromDateTime(metadata.OccurredAt.UtcDateTime),
            Producer = metadata.Producer,
            AggregateType = metadata.AggregateType,
            AggregateId = metadata.AggregateId,
            AggregateVersion = metadata.AggregateVersion,
            CorrelationId = metadata.CorrelationId,
            CausationId = metadata.CausationId,
            Traceparent = metadata.Traceparent,
            Tracestate = metadata.Tracestate,
            Data = Any.Pack(transition.Payload)
        };
        return new CartStoredResult(
            transition.Subject,
            metadata.MessageId,
            result.ToByteArray());
    }

    private static CartTransition Rejection(
        Cart cart,
        ulong currentVersion,
        string commandId,
        string userId,
        string code,
        string safeMessage) =>
        new(
            cart,
            false,
            "boutique.evt.cart.command-rejected.v1",
            "boutique.cart.CommandRejected.v1",
            new CartCommandRejectedEvent
            {
                CommandId = commandId,
                UserId = userId,
                CurrentCartVersion = currentVersion,
                Failure = new Failure
                {
                    Code = code,
                    Retryable = false,
                    SafeMessage = safeMessage
                }
            },
            currentVersion);

    private static Cart ReadCart(string userId, ReadOnlyMemory<byte> state)
    {
        var cart = state.IsEmpty ? new Cart() : Cart.Parser.ParseFrom(state.ToArray());
        if (!string.IsNullOrEmpty(cart.UserId) &&
            !string.Equals(cart.UserId, userId, StringComparison.Ordinal))
        {
            throw new InvalidDataException("Stored cart owner does not match its aggregate key.");
        }
        cart.UserId = userId;
        return cart;
    }

    private static CartSnapshot Snapshot(Cart cart, ulong version)
    {
        var snapshot = new CartSnapshot
        {
            UserId = cart.UserId,
            CartVersion = version
        };
        snapshot.Items.AddRange(
            cart.Items
                .OrderBy(item => item.ProductId, StringComparer.Ordinal)
                .Select(item => new CartLine
                {
                    ProductId = item.ProductId,
                    Quantity = item.Quantity
                }));
        return snapshot;
    }

    private static void ValidateEnvelope(
        string commandId,
        string userId,
        ulong expectedVersion,
        string messageType,
        MessageEnvelope envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        if (string.IsNullOrWhiteSpace(commandId) ||
            string.IsNullOrWhiteSpace(userId) ||
            string.IsNullOrWhiteSpace(envelope.MessageId) ||
            string.IsNullOrWhiteSpace(envelope.CorrelationId))
        {
            throw new InvalidOperationException(
                "Command ID, user ID, message ID, and correlation ID are required.");
        }
        if (!string.Equals(commandId, envelope.MessageId, StringComparison.Ordinal) ||
            !string.Equals(userId, envelope.AggregateId, StringComparison.Ordinal) ||
            !string.Equals(envelope.AggregateType, "cart", StringComparison.Ordinal) ||
            !string.Equals(envelope.MessageType, messageType, StringComparison.Ordinal) ||
            envelope.AggregateVersion != expectedVersion ||
            envelope.SchemaVersion != 1 ||
            envelope.OccurredAt == null ||
            envelope.Data == null)
        {
            throw new InvalidOperationException(
                "Cart command and envelope identities do not match.");
        }
        _ = envelope.OccurredAt.ToDateTimeOffset();
    }

    private sealed record CartTransition(
        Cart State,
        bool AdvanceVersion,
        string Subject,
        string MessageType,
        IMessage Payload,
        ulong ResultVersion);
}

internal static class CartResultJournal
{
    private const byte FormatVersion = 1;
    private const int MaximumFieldLength = 16 * 1024 * 1024;

    public static byte[] Serialize(CartStoredResult result)
    {
        var subject = Encoding.UTF8.GetBytes(result.Subject);
        var messageId = Encoding.UTF8.GetBytes(result.MessageId);
        var data = result.Data.ToArray();
        var length = checked(
            1 + sizeof(int) + subject.Length +
            sizeof(int) + messageId.Length +
            sizeof(int) + data.Length);
        var journal = new byte[length];
        journal[0] = FormatVersion;
        var offset = 1;
        Write(subject, journal, ref offset);
        Write(messageId, journal, ref offset);
        Write(data, journal, ref offset);
        return journal;
    }

    public static CartStoredResult Parse(ReadOnlySpan<byte> journal)
    {
        if (journal.IsEmpty || journal[0] != FormatVersion)
        {
            throw new InvalidDataException("Unsupported cart result-journal format.");
        }
        var offset = 1;
        var subject = Encoding.UTF8.GetString(Read(journal, ref offset));
        var messageId = Encoding.UTF8.GetString(Read(journal, ref offset));
        var data = Read(journal, ref offset).ToArray();
        if (offset != journal.Length ||
            string.IsNullOrWhiteSpace(subject) ||
            string.IsNullOrWhiteSpace(messageId) ||
            data.Length == 0)
        {
            throw new InvalidDataException("Invalid cart result journal.");
        }
        return new CartStoredResult(subject, messageId, data);
    }

    private static void Write(
        ReadOnlySpan<byte> value,
        Span<byte> destination,
        ref int offset)
    {
        BinaryPrimitives.WriteInt32BigEndian(destination[offset..], value.Length);
        offset += sizeof(int);
        value.CopyTo(destination[offset..]);
        offset += value.Length;
    }

    private static ReadOnlySpan<byte> Read(
        ReadOnlySpan<byte> source,
        ref int offset)
    {
        if (source.Length - offset < sizeof(int))
        {
            throw new InvalidDataException("Truncated cart result journal.");
        }
        var length = BinaryPrimitives.ReadInt32BigEndian(source[offset..]);
        offset += sizeof(int);
        if (length < 0 ||
            length > MaximumFieldLength ||
            source.Length - offset < length)
        {
            throw new InvalidDataException("Invalid cart result-journal field length.");
        }
        var result = source.Slice(offset, length);
        offset += length;
        return result;
    }
}
