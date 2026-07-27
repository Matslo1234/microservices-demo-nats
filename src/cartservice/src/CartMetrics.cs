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

using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using Boutique.Stateless;

namespace cartservice;

public sealed class CartMetrics
{
    private readonly ConcurrentDictionary<string, long> _handlerOutcomes = new();
    private readonly ConcurrentDictionary<string, long> _commandDurationCount = new();
    private readonly ConcurrentDictionary<string, double> _commandDurationSum = new();
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
        _handlerOutcomes.AddOrUpdate(
            $"{command}\0{outcome}",
            1,
            static (_, current) => current + 1);
        _commandDurationCount.AddOrUpdate(
            command,
            1,
            static (_, current) => current + 1);
        _commandDurationSum.AddOrUpdate(
            command,
            elapsed.TotalSeconds,
            (_, current) => current + elapsed.TotalSeconds);
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
        foreach (var (key, value) in _handlerOutcomes.OrderBy(entry => entry.Key, StringComparer.Ordinal))
        {
            var separator = key.IndexOf('\0');
            output.Append(MetricNames.HandlerOutcomes)
                .Append("{service=\"cartservice\",handler=\"")
                .Append(key.AsSpan(0, separator))
                .Append("\",outcome=\"")
                .Append(key.AsSpan(separator + 1))
                .Append("\"} ")
                .AppendLine(value.ToString(CultureInfo.InvariantCulture));
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
        foreach (var command in _commandDurationCount.Keys.OrderBy(value => value, StringComparer.Ordinal))
        {
            _commandDurationCount.TryGetValue(command, out var count);
            _commandDurationSum.TryGetValue(command, out var sum);
            output.Append("boutique_cart_command_duration_seconds_count{command=\"")
                .Append(command)
                .Append("\"} ")
                .AppendLine(count.ToString(CultureInfo.InvariantCulture));
            output.Append("boutique_cart_command_duration_seconds_sum{command=\"")
                .Append(command)
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
}
