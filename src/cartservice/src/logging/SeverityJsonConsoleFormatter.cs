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
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging.Console;

namespace cartservice.logging
{
    public sealed class SeverityJsonConsoleFormatter : ConsoleFormatter
    {
        public const string FormatterName = "severity-json";

        private static readonly HashSet<string> ReservedProperties = new(StringComparer.OrdinalIgnoreCase)
        {
            "timestamp",
            "severity",
            "name",
            "message",
            "event_id",
            "event_name",
            "exception"
        };

        public SeverityJsonConsoleFormatter() : base(FormatterName)
        {
        }

        public override void Write<TState>(
            in LogEntry<TState> logEntry,
            IExternalScopeProvider scopeProvider,
            TextWriter textWriter)
        {
            var message = logEntry.Formatter(logEntry.State, logEntry.Exception);
            if (string.IsNullOrEmpty(message) && logEntry.Exception == null)
            {
                return;
            }

            var buffer = new ArrayBufferWriter<byte>();
            using (var writer = new Utf8JsonWriter(buffer))
            {
                writer.WriteStartObject();
                writer.WriteString("timestamp", DateTimeOffset.UtcNow);
                writer.WriteString("severity", Severity(logEntry.LogLevel));
                writer.WriteString("name", logEntry.Category);
                writer.WriteString("message", message);

                if (logEntry.EventId.Id != 0)
                {
                    writer.WriteNumber("event_id", logEntry.EventId.Id);
                }
                if (!string.IsNullOrWhiteSpace(logEntry.EventId.Name))
                {
                    writer.WriteString("event_name", logEntry.EventId.Name);
                }

                WriteStructuredState(writer, logEntry.State);

                if (logEntry.Exception != null)
                {
                    writer.WriteString("exception", logEntry.Exception.ToString());
                }

                writer.WriteEndObject();
            }

            textWriter.Write(Encoding.UTF8.GetString(buffer.WrittenSpan));
            textWriter.WriteLine();
        }

        private static void WriteStructuredState<TState>(Utf8JsonWriter writer, TState state)
        {
            if (state is not IEnumerable<KeyValuePair<string, object>> properties)
            {
                return;
            }

            foreach (var property in properties)
            {
                if (property.Key == "{OriginalFormat}" || ReservedProperties.Contains(property.Key))
                {
                    continue;
                }

                writer.WritePropertyName(property.Key);
                WriteValue(writer, property.Value);
            }
        }

        private static void WriteValue(Utf8JsonWriter writer, object value)
        {
            switch (value)
            {
                case null:
                    writer.WriteNullValue();
                    break;
                case bool boolean:
                    writer.WriteBooleanValue(boolean);
                    break;
                case byte number:
                    writer.WriteNumberValue(number);
                    break;
                case sbyte number:
                    writer.WriteNumberValue(number);
                    break;
                case short number:
                    writer.WriteNumberValue(number);
                    break;
                case ushort number:
                    writer.WriteNumberValue(number);
                    break;
                case int number:
                    writer.WriteNumberValue(number);
                    break;
                case uint number:
                    writer.WriteNumberValue(number);
                    break;
                case long number:
                    writer.WriteNumberValue(number);
                    break;
                case ulong number:
                    writer.WriteNumberValue(number);
                    break;
                case float number:
                    writer.WriteNumberValue(number);
                    break;
                case double number:
                    writer.WriteNumberValue(number);
                    break;
                case decimal number:
                    writer.WriteNumberValue(number);
                    break;
                case DateTime dateTime:
                    writer.WriteStringValue(dateTime);
                    break;
                case DateTimeOffset dateTimeOffset:
                    writer.WriteStringValue(dateTimeOffset);
                    break;
                case Guid guid:
                    writer.WriteStringValue(guid);
                    break;
                default:
                    writer.WriteStringValue(Convert.ToString(value, CultureInfo.InvariantCulture));
                    break;
            }
        }

        private static string Severity(LogLevel logLevel) =>
            logLevel switch
            {
                LogLevel.Trace => "DEBUG",
                LogLevel.Debug => "DEBUG",
                LogLevel.Information => "INFO",
                LogLevel.Warning => "WARNING",
                LogLevel.Error => "ERROR",
                LogLevel.Critical => "CRITICAL",
                _ => "DEFAULT"
            };
    }
}
