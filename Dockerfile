FROM --platform=linux/amd64 python:3.8.12

COPY Pipfile Pipfile.lock /src/

WORKDIR /src
RUN apt -y update \
    && apt -y upgrade \
    && apt install -y wget gnupg \
    && apt-get -y install curl unixodbc unixodbc-dev libssl-dev build-essential libsnappy-dev python3-dev libpq-dev libodbc1 odbcinst1debian2

# Required for msodbcsql17 and mssql-tools
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get -y update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17 mssql-tools

RUN pip install pipenv
RUN pipenv install --system --deploy --ignore-pipfile

USER root

# Copy openssl config file to allow lower tls version (1.0)
COPY openssl.cnf /etc/ssl/openssl.cnf

CMD python3
