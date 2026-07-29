#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/apply-latest.sh [--dry-run]

Find the most recently pushed Docker Hub tag for every first-party image used
by release/* and benchmark/benchmark-*, then update each image's sha256 digest
in those manifests and scripts/generate-release-manifests.py. Existing tags in
the manifests are preserved.

Environment variables:
  DOCKERHUB_USERNAME       Docker Hub namespace to update (default: matslo123)
  DOCKERHUB_API_BASE       Docker Hub API base URL
                           (default: https://hub.docker.com/v2)
  DOCKER_REGISTRY_BASE     Docker registry API base URL
                           (default: https://registry-1.docker.io)
  DOCKER_REGISTRY_AUTH_BASE
                           Docker registry token service
                           (default: https://auth.docker.io)
  DOCKER_REGISTRY_SERVICE  Registry audience used when requesting a token
                           (default: registry.docker.io)
EOF
}

dry_run=false
case "${1:-}" in
  "")
    ;;
  --dry-run)
    dry_run=true
    ;;
  -h|--help)
    usage
    exit 0
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
release_manifest_generator="${script_dir}/generate-release-manifests.py"

dockerhub_username="${DOCKERHUB_USERNAME:-matslo123}"
dockerhub_api_base="${DOCKERHUB_API_BASE:-https://hub.docker.com/v2}"
docker_registry_base="${DOCKER_REGISTRY_BASE:-https://registry-1.docker.io}"
docker_registry_auth_base="${DOCKER_REGISTRY_AUTH_BASE:-https://auth.docker.io}"
docker_registry_service="${DOCKER_REGISTRY_SERVICE:-registry.docker.io}"

if [[ ! "${dockerhub_username}" =~ ^[a-z0-9]+([._-][a-z0-9]+)*$ ]]; then
  echo "invalid Docker Hub namespace: ${dockerhub_username}" >&2
  exit 1
fi

for required_command in curl jq awk sed sort; do
  if ! command -v "${required_command}" >/dev/null 2>&1; then
    echo "${required_command} is required but was not found in PATH" >&2
    exit 1
  fi
done

shopt -s nullglob
manifest_candidates=(
  "${repo_root}"/release/*
  "${repo_root}"/benchmark/benchmark-*
)
manifest_files=()
for candidate in "${manifest_candidates[@]}"; do
  if [[ -f "${candidate}" ]]; then
    manifest_files+=("${candidate}")
  fi
done

if ((${#manifest_files[@]} == 0)); then
  echo "no manifests found under release/* or benchmark/benchmark-*" >&2
  exit 1
fi

# Treat images in the configured Docker Hub namespace as first-party application
# images. Infrastructure and upstream Online Boutique images are left untouched.
mapfile -t services < <(
  awk '$1 == "image:" { print $2 }' "${manifest_files[@]}" |
    while IFS= read -r image_reference; do
      image_reference="${image_reference#\"}"
      image_reference="${image_reference%\"}"
      image_reference="${image_reference#\'}"
      image_reference="${image_reference%\'}"

      repository="${image_reference%%@*}"
      repository="${repository%%:*}"
      if [[ "${repository}" == "${dockerhub_username}/"* ]]; then
        service="${repository#"${dockerhub_username}/"}"
        if [[ -n "${service}" && "${service}" != */* ]]; then
          printf '%s\n' "${service}"
        fi
      fi
    done |
    sort -u
)

if ((${#services[@]} == 0)); then
  echo "no ${dockerhub_username} application images found in target manifests" >&2
  exit 1
fi

if [[ ! -f "${release_manifest_generator}" ]]; then
  echo "release manifest generator not found: ${release_manifest_generator}" >&2
  exit 1
fi

# Ensure every image that will be updated has exactly one corresponding
# DEFAULT_IMAGE_DIGESTS entry before resolving remote state or changing files.
for service in "${services[@]}"; do
  dictionary_entry_count="$(
    awk -v service="${service}" '
      $0 == "DEFAULT_IMAGE_DIGESTS = {" {
        in_dictionary = 1
        next
      }
      in_dictionary && $0 == "}" {
        in_dictionary = 0
      }
      in_dictionary {
        line = $0
        sub(/^[[:space:]]*"/, "", line)
        split(line, fields, "\"")
        if (fields[1] == service) {
          count++
        }
      }
      END {
        print count + 0
      }
    ' "${release_manifest_generator}"
  )"
  if [[ "${dictionary_entry_count}" != 1 ]]; then
    echo "expected exactly one DEFAULT_IMAGE_DIGESTS entry for ${service}, found ${dictionary_entry_count}" >&2
    exit 1
  fi
done

fetch_latest_tag() {
  local service="$1"
  local url
  local response
  local candidate
  local candidate_pushed
  local candidate_tag
  local best_pushed=""
  local best_tag=""
  local page_count=0

  url="${dockerhub_api_base%/}/namespaces/${dockerhub_username}/repositories/${service}/tags?page_size=100"

  while [[ -n "${url}" ]]; do
    page_count=$((page_count + 1))
    if ((page_count > 100)); then
      echo "refusing to follow more than 100 tag pages for ${dockerhub_username}/${service}" >&2
      return 1
    fi

    if ! response="$(
      curl \
        --fail \
        --silent \
        --show-error \
        --location \
        --retry 3 \
        --retry-delay 1 \
        --max-time 30 \
        "${url}"
    )"; then
      echo "failed to query Docker Hub tags for ${dockerhub_username}/${service}" >&2
      return 1
    fi

    if ! jq -e '.results | type == "array"' >/dev/null <<<"${response}"; then
      echo "Docker Hub returned an invalid tag response for ${dockerhub_username}/${service}" >&2
      return 1
    fi

    candidate="$(
      jq -r \
        '
          [
            .results[]
            | select((.tag_status // .status // "active") == "active")
            | . as $tag
            | ($tag.tag_last_pushed // $tag.last_updated // empty) as $pushed
            | select(($tag.name | type) == "string")
            | select(($pushed | type) == "string")
            | {
                pushed: $pushed,
                tag: $tag.name
              }
          ]
          | if length == 0 then
              empty
            else
              max_by(.pushed)
              | [.pushed, .tag]
              | @tsv
            end
        ' <<<"${response}"
    )"

    if [[ -n "${candidate}" ]]; then
      IFS=$'\t' read -r candidate_pushed candidate_tag <<<"${candidate}"
      if [[ -z "${best_pushed}" || "${candidate_pushed}" > "${best_pushed}" ]]; then
        best_pushed="${candidate_pushed}"
        best_tag="${candidate_tag}"
      fi
    fi

    url="$(jq -r '.next // empty' <<<"${response}")"
  done

  if [[ -z "${best_tag}" ]]; then
    echo "no active tag found for ${dockerhub_username}/${service}" >&2
    return 1
  fi

  printf '%s\t%s\n' "${best_tag}" "${best_pushed}"
}

resolve_registry_digest() {
  local service="$1"
  local tag="$2"
  local token_response
  local token
  local manifest_headers
  local digest

  if [[ ! "${tag}" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]]; then
    echo "Docker Hub returned an invalid tag for ${dockerhub_username}/${service}: ${tag}" >&2
    return 1
  fi

  if ! token_response="$(
    curl \
      --fail \
      --silent \
      --show-error \
      --location \
      --retry 3 \
      --retry-delay 1 \
      --max-time 30 \
      --get \
      --data-urlencode "service=${docker_registry_service}" \
      --data-urlencode "scope=repository:${dockerhub_username}/${service}:pull" \
      "${docker_registry_auth_base%/}/token"
  )"; then
    echo "failed to request a registry token for ${dockerhub_username}/${service}" >&2
    return 1
  fi

  token="$(jq -r '.token // .access_token // empty' <<<"${token_response}")"
  if [[ -z "${token}" ]]; then
    echo "registry token service returned no token for ${dockerhub_username}/${service}" >&2
    return 1
  fi

  # Docker Hub's tags API can briefly report a new push timestamp with the
  # previous digest. Resolve the selected tag through the registry, whose
  # Docker-Content-Digest header is authoritative for pulls.
  if ! manifest_headers="$(
    curl \
      --fail \
      --silent \
      --show-error \
      --location \
      --retry 3 \
      --retry-delay 1 \
      --max-time 30 \
      --head \
      --header "Authorization: Bearer ${token}" \
      --header "Accept: application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json" \
      "${docker_registry_base%/}/v2/${dockerhub_username}/${service}/manifests/${tag}"
  )"; then
    echo "failed to resolve ${dockerhub_username}/${service}:${tag} through the registry" >&2
    return 1
  fi

  digest="$(
    awk '
      tolower($1) == "docker-content-digest:" {
        gsub(/\r/, "", $2)
        value = $2
      }
      END {
        print value
      }
    ' <<<"${manifest_headers}"
  )"

  if [[ ! "${digest}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
    echo "registry returned no valid sha256 digest for ${dockerhub_username}/${service}:${tag}" >&2
    return 1
  fi

  printf '%s\n' "${digest}"
}

declare -A latest_digests

echo "Resolving ${#services[@]} application images from Docker Hub..."
for service in "${services[@]}"; do
  latest_tag_info="$(fetch_latest_tag "${service}")"
  IFS=$'\t' read -r latest_tag latest_push_time <<<"${latest_tag_info}"
  latest_digest="$(resolve_registry_digest "${service}" "${latest_tag}")"
  latest_digests["${service}"]="${latest_digest}"
  printf '  %s/%s:%s -> %s (pushed %s)\n' \
    "${dockerhub_username}" \
    "${service}" \
    "${latest_tag}" \
    "${latest_digest}" \
    "${latest_push_time}"
done

if [[ "${dry_run}" == true ]]; then
  echo "Dry run complete; no files were changed."
  exit 0
fi

for service in "${services[@]}"; do
  # Docker Hub names only require escaping dots when embedded in this ERE.
  image_pattern="${dockerhub_username//./\\.}/${service//./\\.}"
  dictionary_key_pattern="${service//./\\.}"
  digest="${latest_digests["${service}"]}"
  digest_value="${digest#sha256:}"

  for manifest_file in "${manifest_files[@]}"; do
    sed -E -i \
      "s|^([[:space:]]*image:[[:space:]]+[\"']?${image_pattern})(:[^@[:space:]\"'#]+)?(@sha256:[[:xdigit:]]{64})?([\"']?[[:space:]]*(#.*)?)$|\\1\\2@${digest}\\4|" \
      "${manifest_file}"
  done

  sed -E -i \
    "s|^([[:space:]]*\"${dictionary_key_pattern}\":[[:space:]]*\")[[:xdigit:]]{64}(\"[[:space:]]*,[[:space:]]*)$|\\1${digest_value}\\2|" \
    "${release_manifest_generator}"
done

echo "Updated application image digests in ${#manifest_files[@]} manifest files and ${release_manifest_generator}."
