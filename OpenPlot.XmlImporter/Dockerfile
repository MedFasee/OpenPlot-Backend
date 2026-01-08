FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY OpenPlot.XmlImporter/OpenPlot.XmlImporter.csproj OpenPlot.XmlImporter/
RUN dotnet restore OpenPlot.XmlImporter/OpenPlot.XmlImporter.csproj

COPY . .
RUN dotnet publish OpenPlot.XmlImporter/OpenPlot.XmlImporter.csproj -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "OpenPlot.XmlImporter.dll"]