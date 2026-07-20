// Copyright 2018 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

using System;
using System.Net;
using System.Threading.Tasks;
using cartservice.cartstore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
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
    }
}
