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

using System.Diagnostics;
using Boutique.Commands.V1;
using Boutique.Common.V1;
using cartservice.cartstore;

namespace cartservice.messaging;

public sealed class CartCommandProcessor
{
    private readonly ICartCommandStore _store;
    private readonly CartMetrics _metrics;

    public CartCommandProcessor(ICartCommandStore store, CartMetrics metrics)
    {
        _store = store;
        _metrics = metrics;
    }

    public Task<CartCommandCommit> HandleAddItemAsync(
        CartAddItemCommand command,
        MessageEnvelope envelope,
        Func<CartStoredResult, CancellationToken, Task> publish,
        CancellationToken cancellationToken = default) =>
        HandleAsync(
            "add-item",
            () => _store.HandleAddItemCommandAsync(command, envelope, cancellationToken),
            publish,
            cancellationToken);

    public Task<CartCommandCommit> HandleClearAsync(
        CartClearCommand command,
        MessageEnvelope envelope,
        Func<CartStoredResult, CancellationToken, Task> publish,
        CancellationToken cancellationToken = default) =>
        HandleAsync(
            "clear",
            () => _store.HandleClearCommandAsync(command, envelope, cancellationToken),
            publish,
            cancellationToken);

    private async Task<CartCommandCommit> HandleAsync(
        string command,
        Func<Task<CartCommandCommit>> commit,
        Func<CartStoredResult, CancellationToken, Task> publish,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(publish);
        var started = Stopwatch.GetTimestamp();
        try
        {
            var outcome = await commit();
            await publish(outcome.Result, cancellationToken);
            _metrics.RecordCommand(
                command,
                "published",
                Stopwatch.GetElapsedTime(started),
                outcome.Duplicate);
            return outcome;
        }
        catch
        {
            _metrics.RecordCommand(
                command,
                "failed",
                Stopwatch.GetElapsedTime(started),
                false);
            throw;
        }
    }
}
