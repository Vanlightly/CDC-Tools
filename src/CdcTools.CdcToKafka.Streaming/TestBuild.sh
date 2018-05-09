#!/bin/bash
set -ev

dotnet publish -c Release -o ./obj/Docker/publish
docker build -t cdctools.cdc-to-kafka-streaming .
docker tag cdctools.cdc-to-kafka-streaming jackvanlightly/cdctools.cdc-to-kafka-streaming:latest-test
docker push jackvanlightly/cdctools.cdc-to-kafka-streaming:latest-test