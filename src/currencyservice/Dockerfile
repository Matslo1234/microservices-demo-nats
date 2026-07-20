# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Define a default value so it's not empty if the builder fails to provide it
ARG BUILDPLATFORM=linux/amd64

FROM --platform=$BUILDPLATFORM node:22-alpine@sha256:16e22a550f3863206a3f701448c45f7912c6896a62de43add43bb9c86130c3e2 AS builder

# Some packages (e.g. @google-cloud/profiler) require additional
# deps for post-install scripts
RUN apk add --update --no-cache \
    python3 \
    make \
    g++

WORKDIR /workspace/src/currencyservice

COPY src/currencyservice/package*.json ./

# Profiling is disabled in the Kubernetes manifest. Ignore native profiler
# install hooks so the runtime can move to Node 22 for the NATS v3 client even
# though the legacy optional pprof dependency has no Node 22 Alpine binary.
RUN npm ci --omit=dev --ignore-scripts

FROM alpine:3.24.1@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b

RUN apk add --no-cache nodejs

WORKDIR /workspace/src/currencyservice

COPY --from=builder /workspace/src/currencyservice/node_modules ./node_modules

COPY protos /workspace/protos
COPY src/currencyservice .

EXPOSE 8080

ENTRYPOINT [ "node", "server.js" ]
