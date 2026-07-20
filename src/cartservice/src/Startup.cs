using System;
using System.Linq;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using cartservice.cartstore;
using Microsoft.Extensions.Caching.StackExchangeRedis;
using StackExchange.Redis;
using cartservice.messaging;

namespace cartservice
{
    public class Startup
    {
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

            string redisAddress = Configuration["REDIS_ADDR"];
            string spannerProjectId = Configuration["SPANNER_PROJECT"];
            string spannerConnectionString = Configuration["SPANNER_CONNECTION_STRING"];
            string alloyDBConnectionString = Configuration["ALLOYDB_PRIMARY_IP"];

            if (!string.IsNullOrEmpty(redisAddress))
            {
                services.AddSingleton<IConnectionMultiplexer>(_ =>
                {
                    var redisOptions = ConfigurationOptions.Parse(redisAddress);
                    redisOptions.AbortOnConnectFail = false;
                    return ConnectionMultiplexer.Connect(redisOptions);
                });
                services.AddSingleton<RedisOutboxCartStore>();
                services.AddSingleton<ICartStore>(provider => provider.GetRequiredService<RedisOutboxCartStore>());
                services.AddSingleton<ICartCommandStore>(provider => provider.GetRequiredService<RedisOutboxCartStore>());
                services.AddSingleton<NatsOutboxRelay>();
                services.AddSingleton<ICartMessagingHealth>(provider => provider.GetRequiredService<NatsOutboxRelay>());
                services.AddHostedService(provider => provider.GetRequiredService<NatsOutboxRelay>());
            }
            else if (!string.IsNullOrEmpty(spannerProjectId) || !string.IsNullOrEmpty(spannerConnectionString))
            {
                services.AddSingleton<ICartStore, SpannerCartStore>();
            }
            else if (!string.IsNullOrEmpty(alloyDBConnectionString))
            {
                Console.WriteLine("Creating AlloyDB cart store");
                services.AddSingleton<ICartStore, AlloyDBCartStore>();
            }
            else
            {
                Console.WriteLine("Redis cache host(hostname+port) was not specified. Starting a cart service using in memory store");
                services.AddDistributedMemoryCache();
                services.AddSingleton<ICartStore, RedisCartStore>();
            }
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
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
                    context.Response.ContentType = "text/plain; version=0.0.4";
                    await context.Response.WriteAsync(
                        $"boutique_dependency_ready{{service=\"cartservice\",dependency=\"cart_store\"}} {(cartStore.Ping() ? 1 : 0)}\n" +
                        $"boutique_dependency_ready{{service=\"cartservice\",dependency=\"nats\"}} {(messaging.All(health => health.Ready) ? 1 : 0)}\n");
                });
            });
        }
    }
}
