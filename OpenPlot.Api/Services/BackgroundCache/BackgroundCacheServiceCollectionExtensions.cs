using Microsoft.Extensions.DependencyInjection;

namespace OpenPlot.Services.BackgroundCache;

public static class BackgroundCacheServiceCollectionExtensions
{
    public static IServiceCollection AddOpenPlotBackgroundCache(
        this IServiceCollection services)
    {
        services.AddHttpContextAccessor();

        services.AddSingleton<IBackgroundCacheCoordinator, BackgroundCacheCoordinator>();

        services.AddSingleton<BackgroundCacheQueue>();

        services.AddSingleton<IBackgroundCacheQueue>(sp =>
            sp.GetRequiredService<BackgroundCacheQueue>());

        services.AddHostedService<BackgroundCacheWorker>();

        return services;
    }
}
