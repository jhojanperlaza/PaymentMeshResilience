FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /source
COPY Directory.Build.props ./
COPY src/SolidarityGrid/SolidarityGrid.csproj src/SolidarityGrid/packages.lock.json src/SolidarityGrid/
RUN dotnet restore src/SolidarityGrid/SolidarityGrid.csproj --locked-mode
COPY src/SolidarityGrid/ src/SolidarityGrid/
RUN dotnet publish src/SolidarityGrid/SolidarityGrid.csproj -c Release --no-restore -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app ./
RUN mkdir /data && chown app:app /data
USER app
EXPOSE 8080 8081
ENTRYPOINT ["dotnet", "SolidarityGrid.dll"]
