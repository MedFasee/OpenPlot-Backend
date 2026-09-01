FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY ["Directory.Build.props", "./"]
COPY ["OpenPlot.ExportWorker/OpenPlot.ExportWorker.csproj", "OpenPlot.ExportWorker/"]
COPY ["OpenPlot.ExportWorker/packages.lock.json", "OpenPlot.ExportWorker/"]
RUN dotnet restore "OpenPlot.ExportWorker/OpenPlot.ExportWorker.csproj"

COPY . .
RUN dotnet publish "OpenPlot.ExportWorker/OpenPlot.ExportWorker.csproj" \
    -c Release \
    -o /app/publish \
    --no-restore \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
RUN mkdir -p /data/exports /app/logs
ENTRYPOINT ["dotnet", "OpenPlot.ExportWorker.dll"]
