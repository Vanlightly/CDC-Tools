FROM microsoft/dotnet:2.0-sdk
ARG source
WORKDIR /app
COPY ${source:-obj/Docker/publish} .
ENTRYPOINT ["dotnet", "CdcTools.CdcToKafka.Streaming.dll"]