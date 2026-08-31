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

using System.Globalization;
using System.Text;
using Boutique.Stateless;

namespace cartservice;

public sealed class CartMetrics
{
    private const int AddItem = 0;
    private const int Clear = 1;
    private const int Failed = 0;
    private const int Published = 1;
    private static readonly string[] CommandNames = ["add-item", "clear"];
    private static readonly string[] OutcomeNames = ["failed", "published"];

    // The command and outcome labels are a closed set. Fixed cells avoid a
    // composite-string allocation and ConcurrentDictionary operations on every
    // command while retaining atomic updates across partition workers.
    private readonly long[] _handlerOutcomes = new long[4];
    private readonly long[] _commandDurationCount = new long[2];
    private readonly double[] _commandDurationSum = new double[2];
    private long _inboxHits;
    private long _redisRetries;
    private long _resultRepublishes;
    private long _stateConflicts;

    public void RecordCommand(
        string command,
        string outcome,
        TimeSpan elapsed,
        bool duplicate)
    {
        var commandIndex = command switch
        {
            "add-item" => AddItem,
            "clear" => Clear,
            _ => throw new ArgumentOutOfRangeException(nameof(command))
        };
        var outcomeIndex = outcome switch
        {
            "failed" => Failed,
            "published" => Published,
            _ => throw new ArgumentOutOfRangeException(nameof(outcome))
        };
        Interlocked.Increment(
            ref _handlerOutcomes[(commandIndex * OutcomeNames.Length) + outcomeIndex]);
        Interlocked.Increment(ref _commandDurationCount[commandIndex]);
        AtomicAdd(ref _commandDurationSum[commandIndex], elapsed.TotalSeconds);
        if (duplicate)
        {
            Interlocked.Increment(ref _inboxHits);
            Interlocked.Increment(ref _resultRepublishes);
        }
    }

    public void RecordStateConflict() =>
        Interlocked.Increment(ref _stateConflicts);

    public void RecordRedisRetry() =>
        Interlocked.Increment(ref _redisRetries);

    public string Render(bool cartReady, bool natsReady)
    {
        var output = new StringBuilder();
        output.AppendLine(
            "# HELP boutique_dependency_ready Whether a cart dependency is currently ready.");
        output.AppendLine("# TYPE boutique_dependency_ready gauge");
        output.Append("boutique_dependency_ready{service=\"cartservice\",dependency=\"cart_store\"} ")
            .AppendLine(cartReady ? "1" : "0");
        output.Append("boutique_dependency_ready{service=\"cartservice\",dependency=\"nats\"} ")
            .AppendLine(natsReady ? "1" : "0");

        output.AppendLine(
            $"# HELP {MetricNames.HandlerOutcomes} Completed cart command-handler outcomes.");
        output.AppendLine($"# TYPE {MetricNames.HandlerOutcomes} counter");
        for (var commandIndex = 0; commandIndex < CommandNames.Length; commandIndex++)
        {
            for (var outcomeIndex = 0; outcomeIndex < OutcomeNames.Length; outcomeIndex++)
            {
                var value = Interlocked.Read(
                    ref _handlerOutcomes[(commandIndex * OutcomeNames.Length) + outcomeIndex]);
                if (value == 0)
                {
                    continue;
                }
                output.Append(MetricNames.HandlerOutcomes)
                    .Append("{service=\"cartservice\",handler=\"")
                    .Append(CommandNames[commandIndex])
                    .Append("\",outcome=\"")
                    .Append(OutcomeNames[outcomeIndex])
                    .Append("\"} ")
                    .AppendLine(value.ToString(CultureInfo.InvariantCulture));
            }
        }

        Counter(
            output,
            MetricNames.StateConflicts,
            "Aggregate-local Redis commit conflicts retried by cartservice.",
            Interlocked.Read(ref _stateConflicts));
        Counter(
            output,
            MetricNames.ResultRepublishes,
            "Stored cart results republished after an inbox hit.",
            Interlocked.Read(ref _resultRepublishes));
        Counter(
            output,
            "boutique_cart_inbox_hits_total",
            "Cart commands whose exact stored result was loaded from the inbox.",
            Interlocked.Read(ref _inboxHits));
        Counter(
            output,
            "boutique_cart_redis_retries_total",
            "Retryable Redis dependency interruptions observed by cartservice.",
            Interlocked.Read(ref _redisRetries));

        output.AppendLine(
            "# HELP boutique_cart_command_duration_seconds Cart command commit and result-publication latency.");
        output.AppendLine("# TYPE boutique_cart_command_duration_seconds summary");
        for (var index = 0; index < CommandNames.Length; index++)
        {
            var count = Interlocked.Read(ref _commandDurationCount[index]);
            if (count == 0)
            {
                continue;
            }
            var sum = Interlocked.CompareExchange(
                ref _commandDurationSum[index],
                0,
                0);
            output.Append("boutique_cart_command_duration_seconds_count{command=\"")
                .Append(CommandNames[index])
                .Append("\"} ")
                .AppendLine(count.ToString(CultureInfo.InvariantCulture));
            output.Append("boutique_cart_command_duration_seconds_sum{command=\"")
                .Append(CommandNames[index])
                .Append("\"} ")
                .AppendLine(sum.ToString("R", CultureInfo.InvariantCulture));
        }
        return output.ToString();
    }

    private static void Counter(
        StringBuilder output,
        string name,
        string help,
        long value)
    {
        output.Append("# HELP ").Append(name).Append(' ').AppendLine(help);
        output.Append("# TYPE ").Append(name).AppendLine(" counter");
        output.Append(name)
            .Append("{service=\"cartservice\"} ")
            .AppendLine(value.ToString(CultureInfo.InvariantCulture));
    }

    private static void AtomicAdd(ref double target, double value)
    {
        var current = Volatile.Read(ref target);
        while (true)
        {
            var updated = current + value;
            var observed = Interlocked.CompareExchange(ref target, updated, current);
            if (observed.Equals(current))
            {
                return;
            }
            current = observed;
        }
    }
}
