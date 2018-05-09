#!/bin/bash

set -ev

dotnet publish -c Release -o ./obj/Docker/publish
docker build -t cdctools.cdc-to-redshift .
docker tag cdctools.cdc-to-redshift jackvanlightly/cdctools.cdc-to-redshift:latest-test
docker push jackvanlightly/cdctools.cdc-to-redshift:latest-test