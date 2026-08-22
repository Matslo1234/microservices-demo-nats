#!/usr/bin/env python3
"""Fail closed on unsafe rendered regional manifests."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


PRIMARY_ONLY = (
    "cartservice", "checkoutservice", "redis-cart-cluster",
    "redis-checkout-cluster", "messageoperationsservice",
    "productcatalogservice", "currencyservice", "recommendationservice",
    "shippingservice", "emailservice", "adservice", "benchmarkservice",
)


def documents(content: str) -> list[str]:
    return re.split(r"(?m)^---\s*$", content)


def identity(document: str) -> tuple[str, str]:
    kind = re.search(r"(?m)^kind:\s*([^\s#]+)", document)
    name = ""
    in_metadata = False
    for line in document.splitlines():
        if line == "metadata:":
            in_metadata = True
            continue
        if in_metadata and line and not line.startswith((" ", "\t")):
            break
        if in_metadata:
            match = re.match(r"^  name:\s*([^\s#]+)", line)
            if match:
                name = match.group(1)
                break
    return (kind.group(1) if kind else "", name)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--role", choices=("primary", "secondary"), required=True)
    parser.add_argument("--application", action="store_true")
    args = parser.parse_args()
    try:
        content = args.manifest.read_text()
        if not args.application:
            if re.search(r"(?m)^\s*(jetstream\.)?domain\s*[:=]", content):
                raise ValueError("jetstream.domain must remain unset")
            config_match = re.search(
                r"(?ms)^\s+nats\.conf:\s*\|\s*\n(?P<config>.*?)(?=\n---|\Z)", content
            )
            if config_match and re.search(r"(?m)^\s*domain\s*[:=]", config_match.group("config")):
                raise ValueError("rendered NATS config sets a JetStream domain")
            if args.role == "secondary":
                forbidden = {
                    ("ConfigMap", "nats-global-bootstrap"),
                    ("Job", "nats-global-bootstrap"),
                    ("PersistentVolumeClaim", "nats-backups"),
                    ("CronJob", "nats-backup"),
                    ("Deployment", "nats-advisory-watcher"),
                }
                found = {identity(document) for document in documents(content)}
                overlap = forbidden & found
                if overlap:
                    raise ValueError(f"secondary NATS manifest contains primary resources: {sorted(overlap)}")
        elif args.role == "secondary":
            found_names = {identity(document)[1] for document in documents(content)}
            overlap = sorted(set(PRIMARY_ONLY) & found_names)
            if overlap:
                raise ValueError(f"secondary application contains primary-only resources: {overlap}")
        if f"REGION_ID: {args.region}" not in content:
            raise ValueError(f"manifest does not contain REGION_ID {args.region!r}")
        if f"NATS_CLUSTER_ROLE: {args.role}" not in content:
            raise ValueError(f"manifest does not contain NATS_CLUSTER_ROLE {args.role!r}")
        if f"storefront-projection-{args.region}-v1" not in content:
            raise ValueError("rendered projection durable is not region-qualified")
    except (OSError, ValueError) as error:
        print(f"rendered manifest validation failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
