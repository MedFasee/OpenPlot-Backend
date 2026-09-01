FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

ARG NUGET_CONFIG=NuGet.config
ARG ENABLE_ONS_CONFIGIT=true

COPY ["Directory.Build.props", "./"]
COPY ["NuGet.config", "./"]
COPY ["NuGet.dev.config", "./"]
COPY ["OpenPlot.Api/OpenPlot.csproj", "OpenPlot.Api/"]
COPY ["OpenPlot.Api/packages.lock.json", "OpenPlot.Api/"]

RUN dotnet restore "OpenPlot.Api/OpenPlot.csproj" \
    --configfile ./${NUGET_CONFIG} \
    -p:EnableOnsConfigItPackage=${ENABLE_ONS_CONFIGIT}

COPY . .
RUN dotnet publish "OpenPlot.Api/OpenPlot.csproj" \
    -c Release \
    -o /app/publish \
    --no-restore \
    /p:UseAppHost=false \
    -p:EnableOnsConfigItPackage=${ENABLE_ONS_CONFIGIT}

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app

COPY --from=build /app/publish .
RUN mkdir -p /app/logs /data/xml /data/exports

EXPOSE 7011
ENV ASPNETCORE_URLS=http://+:7011
ENTRYPOINT ["dotnet", "OpenPlot.dll"]
