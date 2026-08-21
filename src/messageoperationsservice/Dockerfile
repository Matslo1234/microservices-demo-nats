# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

ARG BUILDPLATFORM=linux/amd64
FROM --platform=$BUILDPLATFORM golang:1.26.4-alpine@sha256:3ad57304ad93bbec8548a0437ad9e06a455660655d9af011d58b993f6f615648 AS builder
ARG TARGETOS=linux
ARG TARGETARCH=amd64
WORKDIR /workspace/src/messageoperationsservice
COPY src/messageoperationsservice/go.mod src/messageoperationsservice/go.sum ./
COPY protos /workspace/protos
RUN go mod download
COPY src/messageoperationsservice .
RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} CGO_ENABLED=0 go build -ldflags="-s -w" -o /messageoperationsservice .

FROM gcr.io/distroless/static
WORKDIR /app
COPY --from=builder /messageoperationsservice /app/messageoperationsservice
EXPOSE 8080
USER 1000
ENTRYPOINT ["/app/messageoperationsservice"]
