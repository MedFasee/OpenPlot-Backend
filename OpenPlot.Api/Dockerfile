FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY OpenPlot.Api/OpenPlot.csproj OpenPlot.Api/
RUN dotnet restore OpenPlot.Api/OpenPlot.csproj

COPY . .
RUN dotnet publish OpenPlot.Api/OpenPlot.csproj -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app/publish .

EXPOSE 8080
ENV ASPNETCORE_URLS=http://+:8080
ENTRYPOINT ["dotnet", "OpenPlot.dll"]