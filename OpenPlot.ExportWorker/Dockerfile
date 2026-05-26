FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY ["OpenPlot.ExportWorker/OpenPlot.ExportWorker.csproj", "OpenPlot.ExportWorker/"]
RUN dotnet restore "OpenPlot.ExportWorker/OpenPlot.ExportWorker.csproj"

COPY . .
RUN dotnet publish "OpenPlot.ExportWorker/OpenPlot.ExportWorker.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app

COPY --from=build /app/publish .

RUN mkdir -p /data/exports

ENTRYPOINT ["dotnet", "OpenPlot.ExportWorker.dll"]
