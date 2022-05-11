FROM python:3.9-alpine3.15

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apk add build-base

RUN pip install --upgrade pip

RUN adduser -D movies_api
USER movies_api
WORKDIR /home/movies_api
COPY --chown=movies_api:movies_api ./requirements.txt requirements.txt
RUN pip install --user -r requirements.txt

ENV PATH="/home/movies_api/.local/bin:${PATH}"

COPY --chown=movies_api:movies_api ./src .
COPY --chown=movies_api:movies_api .env .env