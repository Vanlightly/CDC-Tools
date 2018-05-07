#!/bin/bash
set -ev

dotnet publish -c Release -o ./obj/Docker/publish
docker build -t cdctools.kafka-to-redshift .
docker tag cdctools.kafka-to-redshift jackvanlightly/cdctools.kafka-to-redshift:latest
docker push jackvanlightly/cdctools.kafka-to-redshift:latest