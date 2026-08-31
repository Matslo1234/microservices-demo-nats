#!/usr/bin/env python3
"""Validate the deployment inventory and an optional deployment target.

The inventory is JSON syntax stored as YAML, so validation needs only the
Python standard library and remains usable from deployment and CI images.
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import re
import sys
from pathlib import Path


REGION = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
ASSET = re.compile(r"^[A-Z][A-Z0-9_]{0,127}$")
DURABLE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
GATEWAY_ENDPOINT = re.compile(
    r"^(?:[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?):7222$"
)
REQUIRED = {
    "region_id", "region_key", "k8s_cluster_name", "k8s_context",
    "nats_cluster_name",
    "nats_cluster_role", "nats_mode", "gateway_endpoints",
    "gateway_source_cidrs", "remote_gateways", "stream_owner_region",
    "storefront_event_stream", "storefront_projection_durable",
    "storefront_personalization_stream", "storefront_personalization_durable",
    "storefront_products_bucket", "storefront_carts_bucket",
    "storefront_context_bucket", "storefront_orders_bucket",
    "storefront_operations_bucket", "benchmark_runs_bucket",
    "benchmark_artifacts_bucket", "live_operation_prefix",
    "primary_only_workloads",
}
ASSET_FIELDS = (
    "storefront_products_bucket", "storefront_carts_bucket",
    "storefront_context_bucket", "storefront_orders_bucket",
    "storefront_operations_bucket", "benchmark_runs_bucket",
    "benchmark_artifacts_bucket",
)


def fail(message: str) -> None:
    raise ValueError(message)


def load_inventory(path: Path) -> list[dict[str, object]]:
    data = json.loads(path.read_text())
    regions = data.get("regions")
    if not isinstance(regions, list) or not regions:
        fail("inventory must contain a non-empty regions list")
    return regions


def validate(regions: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    indexed: dict[str, dict[str, object]] = {}
    cluster_names: set[str] = set()
    asset_names: set[str] = set()
    durables: set[str] = set()
    primary_count = 0
    for item in regions:
        missing = REQUIRED - item.keys()
        if missing:
            fail(f"region is missing fields: {sorted(missing)}")
        region_id = str(item["region_id"])
        if not REGION.fullmatch(region_id):
            fail(f"invalid region_id {region_id!r}")
        if region_id in indexed:
            fail(f"duplicate region_id {region_id!r}")
        expected_key = region_id.upper().replace("-", "_")
        if item["region_key"] != expected_key:
            fail(f"region_key for {region_id} must be {expected_key}")
        if not REGION.fullmatch(str(item["k8s_cluster_name"])):
            fail(f"invalid k8s_cluster_name for {region_id}")
        context = str(item["k8s_context"])
        if not context or len(context) > 1024 or any(character.isspace() for character in context):
            fail(f"invalid k8s_context for {region_id}")
        cluster_name = str(item["nats_cluster_name"])
        if not DURABLE.fullmatch(cluster_name) or cluster_name in cluster_names:
            fail(f"invalid or duplicate NATS cluster name {cluster_name!r}")
        cluster_names.add(cluster_name)
        role = item["nats_cluster_role"]
        if role not in {"primary", "secondary"}:
            fail(f"invalid role for {region_id}: {role!r}")
        primary_count += role == "primary"
        if role == "secondary" and item["primary_only_workloads"]:
            fail(f"secondary region {region_id} enables primary-only workloads")
        if not isinstance(item["primary_only_workloads"], bool):
            fail(f"primary_only_workloads for {region_id} must be boolean")
        if item["nats_mode"] not in {"standalone", "supercluster"}:
            fail(f"invalid nats_mode for {region_id}")
        endpoints = item["gateway_endpoints"]
        if not isinstance(endpoints, list) or any(
            not isinstance(endpoint, str) or not GATEWAY_ENDPOINT.fullmatch(endpoint)
            for endpoint in endpoints
        ):
            fail(f"invalid gateway_endpoints for {region_id}")
        if len(endpoints) != len(set(endpoints)):
            fail(f"duplicate gateway endpoint for {region_id}")
        if item["nats_mode"] == "supercluster" and len(endpoints) < 2:
            fail(f"supercluster region {region_id} needs at least two gateway endpoints")
        cidrs = item["gateway_source_cidrs"]
        if not isinstance(cidrs, list):
            fail(f"gateway_source_cidrs for {region_id} must be a list")
        for cidr in cidrs:
            try:
                ipaddress.ip_network(cidr, strict=True)
            except (TypeError, ValueError) as error:
                fail(f"invalid gateway CIDR {cidr!r} for {region_id}: {error}")
        if not isinstance(item["remote_gateways"], list) or any(
            not isinstance(remote, str) for remote in item["remote_gateways"]
        ):
            fail(f"remote_gateways for {region_id} must be a list of region IDs")
        durable = str(item["storefront_projection_durable"])
        if not DURABLE.fullmatch(durable) or region_id not in durable or durable in durables:
            fail(f"invalid, unscoped, or duplicate projection durable {durable!r}")
        durables.add(durable)
        personalization_durable = str(item["storefront_personalization_durable"])
        if (
            not DURABLE.fullmatch(personalization_durable)
            or region_id not in personalization_durable
            or personalization_durable in durables
        ):
            fail(
                "invalid, unscoped, or duplicate personalization durable "
                f"{personalization_durable!r}"
            )
        durables.add(personalization_durable)
        for field in ASSET_FIELDS:
            asset = str(item[field])
            if not ASSET.fullmatch(asset) or not asset.endswith("_" + expected_key):
                fail(f"{field} for {region_id} is not region-qualified")
            if asset in asset_names:
                fail(f"duplicate regional asset name {asset!r}")
            asset_names.add(asset)
        wanted_prefix = f"boutique.live.operation.{region_id}."
        if item["live_operation_prefix"] != wanted_prefix:
            fail(f"live_operation_prefix for {region_id} must be {wanted_prefix}")
        indexed[region_id] = item
    if primary_count != 1:
        fail(f"inventory must contain exactly one primary region, found {primary_count}")
    primary_region = next(
        region_id for region_id, item in indexed.items()
        if item["nats_cluster_role"] == "primary"
    )
    event_streams = {str(item["storefront_event_stream"]) for item in indexed.values()}
    if len(event_streams) != 1:
        fail("every region must use the same global storefront event stream")
    personalization_streams = {
        str(item["storefront_personalization_stream"])
        for item in indexed.values()
    }
    if len(personalization_streams) != 1:
        fail("every region must use the same global personalization stream")
    if event_streams == personalization_streams:
        fail("critical storefront events and personalization must use distinct streams")
    for region_id, item in indexed.items():
        if item["stream_owner_region"] != primary_region:
            fail(
                f"stream_owner_region for {region_id} must be primary region "
                f"{primary_region!r}"
            )
        for remote in item["remote_gateways"]:
            if remote == region_id or remote not in indexed:
                fail(f"region {region_id} references unknown remote {remote!r}")
        if len(indexed) > 1:
            if item["nats_mode"] != "supercluster":
                fail(f"multi-region inventory requires supercluster mode for {region_id}")
            expected_remotes = set(indexed) - {region_id}
            if set(item["remote_gateways"]) != expected_remotes:
                fail(
                    f"remote_gateways for {region_id} must list every other region: "
                    f"{sorted(expected_remotes)}"
                )
            if not item["gateway_source_cidrs"]:
                fail(f"multi-region gateway CIDRs are required for {region_id}")
    return indexed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--region")
    parser.add_argument("--context")
    parser.add_argument("--role", choices=("primary", "secondary"))
    parser.add_argument("--print-mode", action="store_true")
    parser.add_argument("--print-role", action="store_true")
    args = parser.parse_args()
    try:
        indexed = validate(load_inventory(args.inventory))
        if args.region:
            if args.region not in indexed:
                fail(f"unknown region {args.region!r}")
            selected = indexed[args.region]
            if args.context and args.context != selected["k8s_context"]:
                fail(
                    f"kubectl context {args.context!r} does not match inventory "
                    f"context {selected['k8s_context']!r}"
                )
            if args.role and args.role != selected["nats_cluster_role"]:
                fail(
                    f"requested role {args.role!r} does not match inventory "
                    f"role {selected['nats_cluster_role']!r}"
                )
            if args.print_mode:
                print(selected["nats_mode"])
            if args.print_role:
                print(selected["nats_cluster_role"])
    except (OSError, json.JSONDecodeError, ValueError) as error:
        print(f"region inventory validation failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
