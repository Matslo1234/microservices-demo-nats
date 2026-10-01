# NATS supercluster overlays

Each directory represents exactly one regional NATS cluster. The active AWS
pair is `eu-central-1` (primary) and `us-west-2` (secondary). The `local`
overlay enables the gateway listener and three ordinal LoadBalancer Services,
but has no remote gateway seeds because no second cluster is available yet.
It therefore starts safely as a one-member supercluster.

Before connecting another region:

1. replace the `NATS_GATEWAY_DOMAIN` and all three certificate DNS names with
   private, externally resolvable names backed by the ordinal Services;
2. pre-create `nats-gateway-tls` in every region from the same gateway CA;
3. add each remote cluster to `gateway.conf` by exact cluster name and three
   `tls://host:7222` URLs, set `reject_unknown_cluster: true`, and keep the
   client, route, monitor, and exporter ports private; and
4. replace the deliberately non-routable CIDR in
   `secondary-template/gateway-network-policy.yaml` with the exact ingress and
   egress CIDRs declared in the region inventory. Confirm whether the gateway
   LoadBalancer preserves source IPs before choosing ingress CIDRs.

Example remote entry:

```conf
gateways: [
  {
    name: "BOUTIQUE-us-west-2"
    urls: [
      tls://nats-0.gw.us-west-2.example.net:7222,
      tls://nats-1.gw.us-west-2.example.net:7222,
      tls://nats-2.gw.us-west-2.example.net:7222
    ]
  }
]
```

For an empty AWS pair, deploy the primary first. The deployment helper uses
`eu-central-1-bootstrap` to elect the initial three-node metadata leader, then
uses `us-west-2-join` to add one secondary voter before scaling the secondary
to three. This staged cold start avoids a 3-vs-3 WAN vote split.

Do not set `jetstream.domain` in a regional overlay. Global and regional asset
placement depends on all clusters sharing the same JetStream account view.
