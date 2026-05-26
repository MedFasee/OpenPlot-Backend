FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY ["OpenPlot.Ingestor.Gsf/OpenPlot.Ingestor.Gsf.csproj", "OpenPlot.Ingestor.Gsf/"]
RUN dotnet restore "OpenPlot.Ingestor.Gsf/OpenPlot.Ingestor.Gsf.csproj"

COPY . .
RUN dotnet publish "OpenPlot.Ingestor.Gsf/OpenPlot.Ingestor.Gsf.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:9.0 AS final
WORKDIR /app

COPY --from=build /app/publish .

RUN mkdir -p /data/xml

ENTRYPOINT ["dotnet", "OpenPlot.Ingestor.Gsf.dll"]
