using Microsoft.Extensions.Options;
using Microsoft.Azure.Cosmos;
using RoomToRentRadar.Adapters.Outbound.Persistence.CosmosDb;
using RoomToRentRadar.Adapters.Outbound.Persistence.CosmosDb.Configurations;
using RoomToRentRadar.Adapters.Outbound.Persistence.CosmosDb.Context;
using RoomToRentRadar.Bootstrap.DependencyInjection;

var builder = WebApplication.CreateBuilder(args);

builder.Services
    .AddApplicationUseCases()
    .AddOutboundAdapters(builder.Configuration, builder.Environment)
    .AddRoomDetailServices(builder.Configuration)
    .AddInboundAdapters(builder.Configuration)
    .AddProfileFeature();

var app = builder.Build();

app.UseExceptionHandler();
app.UseHttpsRedirection();
app.UseRateLimiter();
app.UseAuthentication();
app.UseAuthorization();
app.MapControllers();

await using (var scope = app.Services.CreateAsyncScope())
{
    var cosmosSettings = scope.ServiceProvider.GetRequiredService<IOptions<CosmosDbOptions>>().Value;
    try
    {
        var cosmosDbContext = scope.ServiceProvider.GetRequiredService<ICosmosDbContext>();
        await cosmosDbContext.EnsureDatabaseAndContainersAsync();

        if (!string.Equals(cosmosSettings.AccountEndpoint, "in-memory", StringComparison.OrdinalIgnoreCase))
        {
            var initializer = scope.ServiceProvider.GetRequiredService<CosmosDbInitializer>();
            await initializer.InitializeAsync();
        }
    }
    catch (CosmosException ex)
    {
        if (app.Environment.IsDevelopment())
        {
            app.Logger.LogWarning(ex, "Could not initialize Cosmos DB at startup in Development. Ensure Cosmos Emulator is running if working locally.");
        }
        else
        {
            app.Logger.LogCritical(ex, "Cosmos DB initialization failed in non-development environment ({EnvironmentName}). Failing fast.", app.Environment.EnvironmentName);
            throw;
        }
    }
    catch (Exception ex)
    {
        if (app.Environment.IsDevelopment())
        {
            app.Logger.LogWarning(ex, "Cosmos DB initialization encountered an exception at startup in Development: {Message}", ex.Message);
        }
        else
        {
            app.Logger.LogCritical(ex, "Cosmos DB initialization encountered a critical failure in non-development environment ({EnvironmentName}). Failing fast: {Message}", app.Environment.EnvironmentName, ex.Message);
            throw;
        }
    }
}

await app.RunAsync();

public partial class Program;
