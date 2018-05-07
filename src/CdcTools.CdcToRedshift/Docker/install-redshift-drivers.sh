#!/bin/bash

# Install the Redshift ODBC driver manager
apt-get update \
	&& apt-get install -y --no-install-recommends unixodbc


if ! wget https://s3.amazonaws.com/redshift-downloads/drivers/odbc/1.4.1.1001/AmazonRedshiftODBC-64-bit-1.4.1.1001-1.x86_64.deb; then
    echo 'Failed to download Redshift ODBC Driver!' 1>&2
    exit 1
fi

# Install the Redshift ODBC driver
apt install ./AmazonRedshiftODBC-64-bit-1.4.1.1001-1.x86_64.deb




