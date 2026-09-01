FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY ["Directory.Build.props", "./"]
COPY ["OpenPlot.Ingestor.Gsf/OpenPlot.Ingestor.Gsf.csproj", "OpenPlot.Ingestor.Gsf/"]
COPY ["OpenPlot.Ingestor.Gsf/packages.lock.json", "OpenPlot.Ingestor.Gsf/"]
RUN dotnet restore "OpenPlot.Ingestor.Gsf/OpenPlot.Ingestor.Gsf.csproj"

COPY . .
RUN dotnet publish "OpenPlot.Ingestor.Gsf/OpenPlot.Ingestor.Gsf.csproj" \
    -c Release \
    -o /app/publish \
    --no-restore \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "OpenPlot.Ingestor.Gsf.dll"]
