FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY ["OpenPlot.Api/OpenPlot.csproj", "OpenPlot.Api/"]
COPY ["NuGet.config", "./"]
RUN dotnet restore "OpenPlot.Api/OpenPlot.csproj" --configfile ./NuGet.config

COPY . .
RUN dotnet publish "OpenPlot.Api/OpenPlot.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app

COPY --from=build /app/publish .

RUN mkdir -p /app/logs /data/xml /data/exports

EXPOSE 7011
ENV ASPNETCORE_URLS=http://+:7011
ENTRYPOINT ["dotnet", "OpenPlot.dll"]
