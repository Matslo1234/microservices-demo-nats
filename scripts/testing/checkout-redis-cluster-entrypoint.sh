#!/bin/sh

set -eu

for port in 7100 7101 7102 7103 7104 7105; do
  node_dir="/tmp/checkout-redis-${port}"
  mkdir -p "${node_dir}"
  redis-server \
    --port "${port}" \
    --bind 0.0.0.0 \
    --protected-mode no \
    --cluster-enabled yes \
    --cluster-config-file nodes.conf \
    --cluster-node-timeout 1000 \
    --cluster-announce-ip 127.0.0.1 \
    --cluster-announce-port "${port}" \
    --cluster-announce-bus-port "$((port + 10000))" \
    --appendonly no \
    --save "" \
    --dir "${node_dir}" \
    --daemonize yes
done

redis-cli --cluster create \
  127.0.0.1:7100 \
  127.0.0.1:7101 \
  127.0.0.1:7102 \
  127.0.0.1:7103 \
  127.0.0.1:7104 \
  127.0.0.1:7105 \
  --cluster-replicas 1 \
  --cluster-yes

while :; do
  sleep 60
done
