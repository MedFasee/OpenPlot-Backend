using System.Threading;
using ons.configit.configuration.provider;
using OpenPlot.Api.Configuration;
using Serilog;
using Serilog.Events;

ThreadPool.GetMinThreads(out var worker, out var io);
ThreadPool.SetMinThreads(
    workerThreads: Math.Max(worker, 200),
    completionPortThreads: Math.Max(io, 200)
);

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .MinimumLevel.Override("Microsoft", LogEventLevel.Fatal)
    .Enrich.FromLogContext().WriteTo.Console()
    .WriteTo.File("logs/api-.log", 
    rollingInterval: RollingInterval.Day).CreateLogger();

var builder = WebApplication.CreateBuilder(args);

builder.Configuration.AddConfigITConfiguration(
    url: Environment.GetEnvironmentVariable("ConfigITr"),
    apiKey: Environment.GetEnvironmentVariable("ConfigITapiKey"),
    environment: Environment.GetEnvironmentVariable("ConfigITamb"),
    pacote: Environment.GetEnvironmentVariable("ConfigITpacote"),
    configItJsonFullPath: null,
    httpClient: null
);



builder.Host.UseSerilog();
builder
    .ConfigureOpenPlotWebHost()
    .AddOpenPlotApiServices();

// ======================================================================
// PIPELINE
// ======================================================================
var app = builder.Build();

app.UseOpenPlotApiPipeline()
   .MapOpenPlotApiEndpoints();

app.Run();

public partial class Program
{
}
