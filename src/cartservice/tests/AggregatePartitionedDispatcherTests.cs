// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

using System.Collections.Concurrent;
using cartservice.messaging;
using Xunit;

namespace cartservice.tests;

public sealed class AggregatePartitionedDispatcherTests
{
    [Fact]
    public async Task PreservesPartitionOrderWhileOtherPartitionsMakeProgress()
    {
        const string firstAggregate = "aggregate-a";
        var firstStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var otherCompleted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var completed = new ConcurrentQueue<string>();

        var dispatcher = new AggregatePartitionedDispatcher<WorkItem>(
            partitionCount: 4,
            capacityPerPartition: 4,
            async (item, cancellationToken) =>
            {
                if (item.Name == "first")
                {
                    firstStarted.SetResult();
                    await releaseFirst.Task.WaitAsync(cancellationToken);
                }
                completed.Enqueue(item.Name);
                if (item.Name == "other")
                {
                    otherCompleted.SetResult();
                }
            },
            CancellationToken.None);
        var otherAggregate = Enumerable.Range(0, 100)
            .Select(index => $"aggregate-other-{index}")
            .First(candidate =>
                dispatcher.PartitionFor(candidate) !=
                dispatcher.PartitionFor(firstAggregate));

        await dispatcher.DispatchAsync(
            firstAggregate,
            new WorkItem("first"));
        await dispatcher.DispatchAsync(
            firstAggregate,
            new WorkItem("second"));
        await dispatcher.DispatchAsync(
            otherAggregate,
            new WorkItem("other"));

        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await otherCompleted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.DoesNotContain("second", completed);

        releaseFirst.SetResult();
        await dispatcher.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(5));

        var completionOrder = completed.ToArray();
        Assert.Equal(3, completionOrder.Length);
        Assert.Contains("other", completionOrder);
        Assert.True(
            Array.IndexOf(completionOrder, "first") <
            Array.IndexOf(completionOrder, "second"));
    }

    private sealed record WorkItem(string Name);
}
