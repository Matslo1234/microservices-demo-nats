#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

dockerhub_username="${DOCKERHUB_USERNAME:-matslo123}"
image_tag="${IMAGE_TAG:-v0.10.7}"
target_platform="${PLATFORM:-linux/amd64}"

builds=(
  "adservice . src/adservice/Dockerfile"
  "cartservice . src/cartservice/src/Dockerfile"
  "checkoutservice . src/checkoutservice/Dockerfile"
  "currencyservice . src/currencyservice/Dockerfile"
  "emailservice . src/emailservice/Dockerfile"
  "frontend . src/frontend/Dockerfile"
  "loadgenerator src/loadgenerator Dockerfile"
  "paymentservice . src/paymentservice/Dockerfile"
  "productcatalogservice . src/productcatalogservice/Dockerfile"
  "recommendationservice src/recommendationservice Dockerfile"
  "shippingservice . src/shippingservice/Dockerfile"
  "storefrontprojectionservice . src/storefrontprojectionservice/Dockerfile"
)

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required but was not found in PATH" >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "cannot connect to the Docker daemon; check that Docker is running and accessible" >&2
  exit 1
fi

echo "Building ${#builds[@]} images for ${target_platform}..."
for item in "${builds[@]}"; do
  read -r service context dockerfile <<< "${item}"
  image="${dockerhub_username}/${service}:${image_tag}"

  echo "Building ${image} from ${context}"
  docker build \
    --platform "${target_platform}" \
    --file "${repo_root}/${dockerfile}" \
    --tag "${image}" \
    "${repo_root}/${context}"
done

echo "All images built successfully. Pushing to Docker Hub..."
for item in "${builds[@]}"; do
  read -r service _ _ <<< "${item}"
  image="${dockerhub_username}/${service}:${image_tag}"

  echo "Pushing ${image}"
  docker push "${image}"
done

echo "Successfully built and pushed all images to ${dockerhub_username} with tag ${image_tag}."
