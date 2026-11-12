# syntax=docker/dockerfile:1
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
RUN apt-get update \
 && apt-get install -y --no-install-recommends g++ make \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN make -C native
RUN dotnet publish src/JobPolls.Publisher -c Release -o /out/publisher
RUN dotnet publish src/JobPolls.Consumer  -c Release -o /out/consumer
RUN dotnet publish src/JobPolls.Api       -c Release -o /out/api

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS api
WORKDIR /app
COPY --from=build /out/api ./
COPY --from=build /src/native/build/libjobpolls_native.so ./
ENV ASPNETCORE_URLS=http://+:8080
ENTRYPOINT ["dotnet", "JobPolls.Api.dll"]

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS consumer
WORKDIR /app
COPY --from=build /out/consumer ./
COPY --from=build /src/native/build/libjobpolls_native.so ./
ENTRYPOINT ["dotnet", "JobPolls.Consumer.dll"]

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS publisher
WORKDIR /app
COPY --from=build /out/publisher ./
ENTRYPOINT ["dotnet", "JobPolls.Publisher.dll"]
