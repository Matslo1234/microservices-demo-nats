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

using System.Threading.Channels;

namespace cartservice.messaging;

internal sealed class AggregatePartitionedDispatcher<T>
{
    private readonly Channel<T>[] _partitions;
    private readonly Task[] _workers;
    private int _completed;

    public AggregatePartitionedDispatcher(
        int partitionCount,
        int capacityPerPartition,
        Func<T, CancellationToken, Task> handler,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(partitionCount, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(capacityPerPartition, 1);
        ArgumentNullException.ThrowIfNull(handler);

        _partitions = new Channel<T>[partitionCount];
        _workers = new Task[partitionCount];
        for (var index = 0; index < partitionCount; index++)
        {
            var channel = Channel.CreateBounded<T>(
                new BoundedChannelOptions(capacityPerPartition)
                {
                    AllowSynchronousContinuations = false,
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleReader = true,
                    SingleWriter = true
                });
            _partitions[index] = channel;
            _workers[index] = RunPartitionAsync(
                channel.Reader,
                handler,
                cancellationToken);
        }
    }

    public int PartitionCount => _partitions.Length;

    public ValueTask DispatchAsync(
        string aggregateId,
        T item,
        CancellationToken cancellationToken = default) =>
        _partitions[PartitionFor(aggregateId)].Writer.WriteAsync(
            item,
            cancellationToken);

    public int PartitionFor(string aggregateId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(aggregateId);
        return (int)(
            (uint)StringComparer.Ordinal.GetHashCode(aggregateId) %
            (uint)_partitions.Length);
    }

    public async Task CompleteAsync()
    {
        if (Interlocked.Exchange(ref _completed, 1) == 0)
        {
            foreach (var partition in _partitions)
            {
                partition.Writer.TryComplete();
            }
        }
        await Task.WhenAll(_workers);
    }

    private static async Task RunPartitionAsync(
        ChannelReader<T> reader,
        Func<T, CancellationToken, Task> handler,
        CancellationToken cancellationToken)
    {
        await foreach (var item in reader.ReadAllAsync(cancellationToken))
        {
            await handler(item, cancellationToken);
        }
    }
}
