# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

ARG BUILDPLATFORM=linux/amd64
FROM --platform=$BUILDPLATFORM golang:1.26.4-alpine@sha256:3ad57304ad93bbec8548a0437ad9e06a455660655d9af011d58b993f6f615648 AS builder
ARG TARGETOS=linux
ARG TARGETARCH=amd64
WORKDIR /workspace/src/storefrontprojectionservice
COPY src/storefrontprojectionservice/go.mod src/storefrontprojectionservice/go.sum ./
COPY protos /workspace/protos
RUN go mod download
COPY src/storefrontprojectionservice .
RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} CGO_ENABLED=0 go build -ldflags="-s -w" -o /storefrontprojectionservice .

FROM gcr.io/distroless/static
WORKDIR /app
COPY --from=builder /storefrontprojectionservice /app/storefrontprojectionservice
EXPOSE 8080
USER 1000
ENTRYPOINT ["/app/storefrontprojectionservice"]
