# Supercluster code changes

Date: 2026-08-21

This change set implements the repository-side work needed to preserve the
existing single-cluster deployment while making regional NATS clusters and
application placement explicit. The checked-in inventory contains only the
`local` primary and selects `standalone`, so no WAN gateway, external DNS, or
second Kubernetes context is required to run the application today.

## Region and deployment contract

- Added `kubernetes-manifests/regions/inventory.yaml` as the validated source
  of region, Kubernetes context, NATS cluster/role, gateway, stream owner,
  durable, regional bucket, and live-subject identity.
- Added fail-closed inventory and rendered-manifest validators. They reject
  invalid/duplicate identities and assets, unknown or incomplete peer sets,
  context/role mismatches, JetStream domains, global control-plane resources
  in a secondary, and primary-only application workloads in a secondary.
- Added regional application overlays. The local primary retains the complete
  application. The secondary template contains only frontend, payment, and
  storefront projection; cart/checkout, both Redis clusters, bootstrap/owner
  services, operations, benchmark, and their dependent resources are removed.
- Reworked `scripts/nats/deploy.sh` to require `--context`, `--region`, and
  `--role`, render and validate before applying, and mutate exactly one
  explicit context. `scripts/nats/generate-secrets.sh` now also requires an
  inventory-matched context and region.

## NATS topology and security

- Parameterized NATS cluster/server identity. Server names include region and
  pod ordinal, server tags include region, NATS cluster, and actual node, and
  local routes remain limited to the three members in one Kubernetes region.
- Added an explicit standalone overlay, a one-member local supercluster
  overlay, and a secondary supercluster template. Gateway overlays provide an
  mTLS listener on `7222`, per-ordinal LoadBalancer Services and advertised
  DNS, reconnect/backoff controls, exact remote cluster names, and no
  `jetstream.domain`.
- Added a fail-closed secondary gateway NetworkPolicy template. Its RFC 5737
  CIDR must be replaced from the region inventory after confirming the load
  balancer's source-IP behavior. All non-gateway NATS ports remain regional.
- Enabled exporter `gatewayz` metrics and added gateway disconnect, RTT, and
  pending-byte alerts plus gateway traffic/latency dashboard panels.
- Kept regional NATS authentication, client/route TLS, and each cluster's
  JetStream encryption key separate. Gateway PKI now has its own
  `nats-gateway-tls` contract. Every supercluster member refuses to start setup
  without a pre-provisioned shared-trust gateway Secret.
- Moved `FRONTEND_COOKIE_KEY`, payment signing/verification keys, and the
  shipping provider secret into a separate create-only
  `global-application-secrets`. A secondary refuses to generate these values;
  startup logs expose only short SHA-256 fingerprints. Regional credential
  rotation preserves global business keys, TLS, and the JetStream encryption
  key.

## JetStream ownership and regional state

- Split bootstrap into primary/global and per-region Jobs. Only the primary
  reconciles `BOUTIQUE_COMMANDS`, `BOUTIQUE_EVENTS`, `BOUTIQUE_DLQ`,
  `BOUTIQUE_ADVISORIES`, `BOOTSTRAP_CLAIMS`, `RECOMMENDATION_CATALOG`, and
  `DLQ_CASES`.
- Added explicit `placement.cluster` to every global stream and equivalent
  `--cluster` placement to every KV/object store. Both bootstraps inspect the
  backing streams and fail on unexpected placement, replicas, storage,
  history, TTL, or size limits.
- The regional bootstrap creates only suffixed storefront and benchmark
  assets, including `STOREFRONT_*_LOCAL`, `BENCHMARK_RUNS_LOCAL`, and
  `BENCHMARK_ARTIFACTS_LOCAL` in the current standalone cluster.
- Parameterized the projection rebuild/reset manifests so guarded maintenance
  operations target the configured regional durable and buckets.

## Application behavior

- Storefront projection now validates and uses its configured source stream,
  region-qualified durable, five regional KV buckets, and regional live-event
  prefix. It remains unready until KV/query setup is complete and the initial
  consumer replay has zero pending and zero acknowledgement-pending messages;
  reconnect readiness waits for catch-up again.
- Added projection metrics for pending, acknowledgement-pending, last event,
  event age, region, Kubernetes/NATS cluster, stream owner, stream, consumer,
  and bucket identity. The projection HPA source now uses the projection's own
  region-labelled lag signal.
- Frontend requires a shared cookie key, no longer derives it from a regional
  NATS password, and subscribes only to
  `boutique.live.operation.<region>.<order-id>`. The projector publishes the
  matching scoped subject.
- Added region and Kubernetes-cluster identity to every language adapter's
  NATS connection name and regional telemetry injection. Payment and shipping
  consume the global application Secret; frontend/payment/shipping log safe
  configuration fingerprints where applicable.
- The secondary template omits payment's global-backlog HPA so multiple
  regions do not scale independently from the same work-queue consumer.

## Release and documentation surfaces

- Release generation validates the inventory and each NATS/application region
  render independently, emits `release/regions/<region>-<role>.yaml`, and
  regenerates the standalone and benchmark bundles from source.
- Benchmark KV/object bucket names and the storefront projection scaling
  signal are region-aware.
- Updated the root/Kubernetes deployment guidance, NATS client contract,
  service-interaction description, runbook commands, and historical static
  guardrails for the split bootstrap and regional assets.

## Single-cluster behavior

The default and `fresh-cluster` NATS entry points use
`nats/overlays/standalone`. They keep cluster name `BOUTIQUE`, do not enable a
gateway listener, run both global and local regional bootstrap Jobs, and inject
the `local` identity into the existing complete application. This is the mode
to use until a second cluster and its infrastructure are available.

## Verification completed

- Inventory validation and standalone/local-supercluster/secondary rendered
  manifest validation passed for both NATS and application overlays.
- The pinned `nats:2.14.3-alpine` server accepted the fully substituted
  standalone, one-member local supercluster, and secondary gateway configs
  with its configuration test mode.
- The pinned `natsio/nats-box:0.19.7-nonroot` CLI was used to verify the
  placement flags, and all embedded setup/global/regional shell scripts pass
  `sh -n`; deployment/secret scripts pass `bash -n`.
- Go tests passed for checkout, frontend, message operations, product catalog,
  shipping, and storefront projection. Currency/payment Node tests and all
  available benchmark Python tests passed. Python modules compile, and Phase 4
  and Phase 5 static contract checks pass. The full Stateless Phase 1 and
  Phase 2 verification scripts also pass.
- Email/recommendation unit tests could not run in this workspace because their
  Python dependencies are not installed. The Java Gradle cache is read-only
  and `dotnet` is unavailable, so those full test suites were not run here.

## Infrastructure and live exercises still required

The repository cannot complete infrastructure exit gates while only the local
cluster exists. Before changing an inventory entry to `supercluster`, operators
must provision private cross-region routing, real ordinal DNS/load balancers,
approved CIDRs, shared gateway PKI with correct SANs, identical global
application secrets, zonal storage, global HTTP traffic management, and
off-region NATS/Redis backups. A second cluster is also required for gateway
CA/hostname rejection tests, cross-region publish acknowledgements and
deduplication, queue geo-affinity, projection parity/rebuild, cookie/token
parity, partition behavior, metadata-quorum exercises, and measured RPO/RTO.
