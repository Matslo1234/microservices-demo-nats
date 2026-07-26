#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/apply-latest.sh [--dry-run]

Find the most recently pushed Docker Hub tag for every first-party image used
by release/* and benchmark/benchmark-*, then update each image's sha256 digest.
Existing tags in the manifests are preserved.

Environment variables:
  DOCKERHUB_USERNAME  Docker Hub namespace to update (default: matslo123)
  DOCKERHUB_API_BASE  Docker Hub API base URL (default: https://hub.docker.com/v2)
  PLATFORM            Platform used if the API omits a tag-level digest
                      (default: linux/amd64)
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

dockerhub_username="${DOCKERHUB_USERNAME:-matslo123}"
dockerhub_api_base="${DOCKERHUB_API_BASE:-https://hub.docker.com/v2}"
target_platform="${PLATFORM:-linux/amd64}"
target_os="${target_platform%%/*}"
target_arch="${target_platform#*/}"

if [[ ! "${dockerhub_username}" =~ ^[a-z0-9]+([._-][a-z0-9]+)*$ ]]; then
  echo "invalid Docker Hub namespace: ${dockerhub_username}" >&2
  exit 1
fi

if [[ "${target_os}" == "${target_arch}" || -z "${target_os}" || -z "${target_arch}" ]]; then
  echo "PLATFORM must have the form os/architecture (for example, linux/amd64)" >&2
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

fetch_latest_image() {
  local service="$1"
  local url
  local response
  local candidate
  local candidate_pushed
  local candidate_tag
  local candidate_digest
  local best_pushed=""
  local best_tag=""
  local best_digest=""
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
        --arg os "${target_os}" \
        --arg arch "${target_arch}" \
        '
          [
            .results[]
            | select((.tag_status // .status // "active") == "active")
            | . as $tag
            | (
                $tag.digest
                // (
                  [
                    $tag.images[]?
                    | select(.os == $os and .architecture == $arch)
                    | .digest
                  ]
                  | first
                )
                // empty
              ) as $digest
            | ($tag.tag_last_pushed // $tag.last_updated // empty) as $pushed
            | select(($tag.name | type) == "string")
            | select(($pushed | type) == "string")
            | select(($digest | type) == "string")
            | select($digest | test("^sha256:[0-9a-f]{64}$"))
            | {
                pushed: $pushed,
                tag: $tag.name,
                digest: $digest
              }
          ]
          | if length == 0 then
              empty
            else
              max_by(.pushed)
              | [.pushed, .tag, .digest]
              | @tsv
            end
        ' <<<"${response}"
    )"

    if [[ -n "${candidate}" ]]; then
      IFS=$'\t' read -r candidate_pushed candidate_tag candidate_digest <<<"${candidate}"
      if [[ -z "${best_pushed}" || "${candidate_pushed}" > "${best_pushed}" ]]; then
        best_pushed="${candidate_pushed}"
        best_tag="${candidate_tag}"
        best_digest="${candidate_digest}"
      fi
    fi

    url="$(jq -r '.next // empty' <<<"${response}")"
  done

  if [[ -z "${best_digest}" ]]; then
    echo "no active sha256 image found for ${dockerhub_username}/${service}" >&2
    return 1
  fi

  printf '%s\t%s\t%s\n' "${best_tag}" "${best_digest}" "${best_pushed}"
}

declare -A latest_digests

echo "Resolving ${#services[@]} application images from Docker Hub..."
for service in "${services[@]}"; do
  latest_image="$(fetch_latest_image "${service}")"
  IFS=$'\t' read -r latest_tag latest_digest latest_push_time <<<"${latest_image}"
  latest_digests["${service}"]="${latest_digest}"
  printf '  %s/%s:%s -> %s (pushed %s)\n' \
    "${dockerhub_username}" \
    "${service}" \
    "${latest_tag}" \
    "${latest_digest}" \
    "${latest_push_time}"
done

if [[ "${dry_run}" == true ]]; then
  echo "Dry run complete; no manifests were changed."
  exit 0
fi

for service in "${services[@]}"; do
  # Docker Hub names only require escaping dots when embedded in this ERE.
  image_pattern="${dockerhub_username//./\\.}/${service//./\\.}"
  digest="${latest_digests["${service}"]}"

  for manifest_file in "${manifest_files[@]}"; do
    sed -E -i \
      "s|^([[:space:]]*image:[[:space:]]+[\"']?${image_pattern})(:[^@[:space:]\"'#]+)?(@sha256:[[:xdigit:]]{64})?([\"']?[[:space:]]*(#.*)?)$|\\1\\2@${digest}\\4|" \
      "${manifest_file}"
  done
done

echo "Updated application image digests in ${#manifest_files[@]} manifest files."
