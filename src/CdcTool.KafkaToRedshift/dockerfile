FROM microsoft/dotnet:2.0-sdk

ADD ./Docker/install-redshift-drivers.sh /tmp/install-redshift-drivers.sh
ADD ./Docker/env.sh /tmp/env.sh

RUN /tmp/install-redshift-drivers.sh

ADD ./Docker/odbc.ini /etc/odbc.ini
ADD ./Docker/odbcinst.ini /etc/odbcinst.ini
ADD ./Docker/amazon.redshiftodbc.ini /opt/amazon/redshiftodbc/lib/64/amazon.redshiftodbc.ini

RUN /tmp/env.sh

ARG source
WORKDIR /app
COPY ${source:-obj/Docker/publish} .
ENTRYPOINT ["dotnet", "CdcTools.KafkaToRedshift.dll"]