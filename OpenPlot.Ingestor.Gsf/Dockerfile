FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY OpenPlot.Ingestor.Gsf/OpenPlot.Ingestor.Gsf.csproj OpenPlot.Ingestor.Gsf/
RUN dotnet restore OpenPlot.Ingestor.Gsf/OpenPlot.Ingestor.Gsf.csproj

COPY . .
RUN dotnet publish OpenPlot.Ingestor.Gsf/OpenPlot.Ingestor.Gsf.csproj -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "OpenPlot.Ingestor.Gsf.dll"]