// Copyright 2018 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text.Json;
using System.Threading.Tasks;
using cartservice.cartstore;
using cartservice.logging;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace cartservice.tests
{
    public class CartServiceTests
    {
        private readonly IHostBuilder _host = new HostBuilder().ConfigureWebHost(webBuilder =>
        {
            webBuilder.UseStartup<Startup>().UseTestServer();
        });

        [Fact]
        public async Task EmptyStoreReturnsEmptyCart()
        {
            using var server = await _host.StartAsync();
            var store = server.Services.GetRequiredService<ICartStore>();
            var cart = await store.GetCartAsync(Guid.NewGuid().ToString());
            Assert.NotNull(cart);
            Assert.Empty(cart.Items);
        }

        [Fact]
        public async Task StoreMutationsRemainAvailableToTheNatsWorker()
        {
            using var server = await _host.StartAsync();
            var store = server.Services.GetRequiredService<ICartStore>();
            var userId = Guid.NewGuid().ToString();
            await store.AddItemAsync(userId, "1", 1);
            await store.AddItemAsync(userId, "1", 1);
            var cart = await store.GetCartAsync(userId);
            Assert.Equal(userId, cart.UserId);
            Assert.Single(cart.Items);
            Assert.Equal(2, cart.Items[0].Quantity);
            await store.EmptyCartAsync(userId);
            Assert.Empty((await store.GetCartAsync(userId)).Items);
        }

        [Fact]
        public async Task HttpHealthAndReadinessAreLocal()
        {
            using var server = await _host.StartAsync();
            var client = server.GetTestClient();
            Assert.Equal(HttpStatusCode.OK, (await client.GetAsync("/healthz")).StatusCode);
            Assert.Equal(HttpStatusCode.OK, (await client.GetAsync("/readyz")).StatusCode);
            var metrics = await client.GetStringAsync("/metrics");
            Assert.Contains("boutique_dependency_ready", metrics);
        }

        [Fact]
        public void ConsoleFormatterWritesJsonWithSeverityAndStructuredFields()
        {
            var formatter = new SeverityJsonConsoleFormatter();
            var state = new List<KeyValuePair<string, object>>
            {
                new("topic", "boutique.cmd.cart.clear.v1"),
                new("attempt", 2),
                new("{OriginalFormat}", "Command received from {topic}")
            };
            var entry = new LogEntry<List<KeyValuePair<string, object>>>(
                LogLevel.Warning,
                "cartservice.tests",
                new EventId(7, "CommandReceived"),
                state,
                null,
                (_, _) => "Command received from boutique.cmd.cart.clear.v1");
            using var output = new StringWriter();

            formatter.Write(in entry, null, output);

            using var document = JsonDocument.Parse(output.ToString());
            var root = document.RootElement;
            Assert.Equal("WARNING", root.GetProperty("severity").GetString());
            Assert.Equal("cartservice.tests", root.GetProperty("name").GetString());
            Assert.Equal("Command received from boutique.cmd.cart.clear.v1",
                root.GetProperty("message").GetString());
            Assert.Equal("boutique.cmd.cart.clear.v1", root.GetProperty("topic").GetString());
            Assert.Equal(2, root.GetProperty("attempt").GetInt32());
            Assert.Equal(7, root.GetProperty("event_id").GetInt32());
            Assert.Equal("CommandReceived", root.GetProperty("event_name").GetString());
        }
    }
}
