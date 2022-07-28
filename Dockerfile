FROM --platform=linux/amd64 python:3.8.12

COPY Pipfile Pipfile.lock /src/

WORKDIR /src
RUN apt -y update \
    && apt -y upgrade \
    && apt install -y wget gnupg

RUN pip install pipenv
RUN pipenv install --system --deploy --ignore-pipfile

CMD python3
