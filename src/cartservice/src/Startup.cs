using System;
using System.Linq;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using cartservice.cartstore;
using Boutique.Stateless;
using StackExchange.Redis;
using cartservice.messaging;

namespace cartservice
{
    public class Startup
    {
        private string cartStoreDescription = "Cart store is not configured";

        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }
        
        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit https://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddRouting();

            var redisAddress = Configuration["REDIS_ADDR"];
            if (string.IsNullOrWhiteSpace(redisAddress))
            {
                throw new InvalidOperationException(
                    "REDIS_ADDR is required for the aggregate-local cart store.");
            }

            cartStoreDescription = "Using aggregate-local Redis Cluster cart store";
            services.AddSingleton<CartMetrics>();
            services.AddSingleton<IConnectionMultiplexer>(_ =>
            {
                var redisOptions = ConfigurationOptions.Parse(redisAddress);
                redisOptions.AbortOnConnectFail = false;
                redisOptions.ResolveDns = true;
                redisOptions.ConnectRetry = 5;
                redisOptions.ConnectTimeout = 2_000;
                redisOptions.AsyncTimeout = 5_000;
                redisOptions.SyncTimeout = 5_000;
                redisOptions.KeepAlive = 20;
                redisOptions.ReconnectRetryPolicy =
                    new ExponentialRetry(1_000, 10_000);
                return ConnectionMultiplexer.Connect(redisOptions);
            });
            services.AddSingleton<IAtomicAggregateStore>(provider =>
                new RedisAtomicAggregateStore(
                    provider.GetRequiredService<IConnectionMultiplexer>().GetDatabase(),
                    "cart:v1"));
            services.AddSingleton<RedisAggregateCartStore>();
            services.AddSingleton<ICartStore>(provider =>
                provider.GetRequiredService<RedisAggregateCartStore>());
            services.AddSingleton<ICartCommandStore>(provider =>
                provider.GetRequiredService<RedisAggregateCartStore>());
            services.AddSingleton<CartCommandProcessor>();
            services.AddSingleton<NatsCartCommandWorker>();
            services.AddSingleton<ICartMessagingHealth>(provider =>
                provider.GetRequiredService<NatsCartCommandWorker>());
            services.AddHostedService(provider =>
                provider.GetRequiredService<NatsCartCommandWorker>());
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env, ILogger<Startup> logger)
        {
            logger.LogInformation("{CartStoreDescription}", cartStoreDescription);

            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseRouting();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapGet("/healthz", async context =>
                {
                    await context.Response.WriteAsync("ok\n");
                });
                endpoints.MapGet("/readyz", async context =>
                {
                    var cartStore = context.RequestServices.GetRequiredService<ICartStore>();
                    var messaging = context.RequestServices.GetServices<ICartMessagingHealth>();
                    if (!cartStore.Ping() || !messaging.All(health => health.Ready))
                    {
                        context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
                        await context.Response.WriteAsync("cart dependencies are not ready\n");
                        return;
                    }
                    await context.Response.WriteAsync("ok\n");
                });
                endpoints.MapGet("/metrics", async context =>
                {
                    var cartStore = context.RequestServices.GetRequiredService<ICartStore>();
                    var messaging = context.RequestServices.GetServices<ICartMessagingHealth>();
                    var metrics = context.RequestServices.GetRequiredService<CartMetrics>();
                    context.Response.ContentType = "text/plain; version=0.0.4";
                    await context.Response.WriteAsync(metrics.Render(
                        cartStore.Ping(),
                        messaging.All(health => health.Ready)));
                });
            });
        }
    }
}
