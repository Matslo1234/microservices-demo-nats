# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

ARG BUILDPLATFORM=linux/amd64
FROM --platform=$BUILDPLATFORM python:3.14.6-alpine@sha256:26730869004e2b9c4b9ad09cab8625e81d256d1ce97e72df5520e806b1709f92 AS base

FROM base AS builder

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apk update \
    && apk add --no-cache g++ linux-headers \
    && rm -rf /var/cache/apk/*

COPY src/benchmarkservice/requirements.txt .
RUN pip install --prefix=/install -r requirements.txt

FROM base

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apk update \
    && apk add --no-cache libstdc++ \
    && rm -rf /var/cache/apk/*

WORKDIR /benchmarkservice
COPY --from=builder /install /usr/local
COPY src/benchmarkservice .

EXPOSE 8080
USER 1000
ENTRYPOINT ["python", "app.py"]
