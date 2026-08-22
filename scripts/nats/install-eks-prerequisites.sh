#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cluster_name=""
context=""
region=""
vpc_id=""
lbc_chart_version="3.5.0"

usage() {
  echo "usage: $0 --cluster-name NAME --context CONTEXT --region REGION --vpc-id VPC_ID [--lbc-chart-version VERSION]" >&2
}

while (($#)); do
  case "$1" in
    --cluster-name) cluster_name="${2:-}"; shift 2 ;;
    --context) context="${2:-}"; shift 2 ;;
    --region) region="${2:-}"; shift 2 ;;
    --vpc-id) vpc_id="${2:-}"; shift 2 ;;
    --lbc-chart-version) lbc_chart_version="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "${cluster_name}" || -z "${context}" || -z "${region}" || -z "${vpc_id}" ]]; then
  usage
  exit 2
fi
if [[ ! "${cluster_name}" =~ ^[A-Za-z0-9_-]+$ ||
      ! "${region}" =~ ^[a-z]{2}-[a-z]+-[0-9]+$ ||
      ! "${vpc_id}" =~ ^vpc-[0-9a-f]+$ ||
      ! "${lbc_chart_version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "invalid cluster, region, VPC, or chart-version argument" >&2
  exit 2
fi
for command in aws curl helm kubectl; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "${command} is required" >&2
    exit 1
  }
done
kubectl config get-contexts "${context}" >/dev/null 2>&1 || {
  echo "kubectl context ${context@Q} does not exist" >&2
  exit 1
}

account_id="$(aws sts get-caller-identity --query Account --output text)"
trust_file="${repo_root}/kubernetes-manifests/aws/eks-pod-identity-trust-policy.json"
ebs_role="AmazonEKSPodIdentityEBSCSIDriverRole"
ebs_role_arn="arn:aws:iam::${account_id}:role/${ebs_role}"
lbc_role="AmazonEKSPodIdentityAWSLoadBalancerControllerRole"
lbc_role_arn="arn:aws:iam::${account_id}:role/${lbc_role}"
lbc_policy="AWSLoadBalancerControllerIAMPolicy"
lbc_policy_arn="arn:aws:iam::${account_id}:policy/${lbc_policy}"

ensure_role() {
  local role_name=$1 description=$2
  if ! aws iam get-role --role-name "${role_name}" >/dev/null 2>&1; then
    aws iam create-role --role-name "${role_name}" \
      --assume-role-policy-document "file://${trust_file}" \
      --description "${description}" >/dev/null
  fi
}

ensure_association() {
  local namespace=$1 service_account=$2 role_arn=$3 association
  association="$(aws eks list-pod-identity-associations \
    --cluster-name "${cluster_name}" --region "${region}" \
    --query "associations[?namespace=='${namespace}' && serviceAccount=='${service_account}'].associationId | [0]" \
    --output text)"
  if [[ "${association}" == None ]]; then
    aws eks create-pod-identity-association \
      --cluster-name "${cluster_name}" --region "${region}" \
      --namespace "${namespace}" --service-account "${service_account}" \
      --role-arn "${role_arn}" >/dev/null
  fi
}

ensure_role "${ebs_role}" "EKS Pod Identity role for the EBS CSI driver"
aws iam attach-role-policy --role-name "${ebs_role}" \
  --policy-arn arn:aws:iam::aws:policy/AmazonEBSCSIDriverPolicyV2
ensure_association kube-system ebs-csi-controller-sa "${ebs_role_arn}"

kubernetes_version="$(aws eks describe-cluster --name "${cluster_name}" --region "${region}" --query cluster.version --output text)"
ebs_version="$(aws eks describe-addon-versions --addon-name aws-ebs-csi-driver \
  --kubernetes-version "${kubernetes_version}" --region "${region}" \
  --query 'addons[0].addonVersions[0].addonVersion' --output text)"
if aws eks describe-addon --cluster-name "${cluster_name}" --region "${region}" \
    --addon-name aws-ebs-csi-driver >/dev/null 2>&1; then
  aws eks update-addon --cluster-name "${cluster_name}" --region "${region}" \
    --addon-name aws-ebs-csi-driver --addon-version "${ebs_version}" \
    --resolve-conflicts OVERWRITE >/dev/null
else
  aws eks create-addon --cluster-name "${cluster_name}" --region "${region}" \
    --addon-name aws-ebs-csi-driver --addon-version "${ebs_version}" \
    --resolve-conflicts OVERWRITE >/dev/null
fi
aws eks wait addon-active --cluster-name "${cluster_name}" --region "${region}" \
  --addon-name aws-ebs-csi-driver
kubectl --context "${context}" apply \
  -f "${repo_root}/kubernetes-manifests/aws/ebs-gp3-storageclass.yaml"

if ! aws iam get-policy --policy-arn "${lbc_policy_arn}" >/dev/null 2>&1; then
  curl -fsSL "https://raw.githubusercontent.com/kubernetes-sigs/aws-load-balancer-controller/v${lbc_chart_version}/docs/install/iam_policy.json" | \
    aws iam create-policy --policy-name "${lbc_policy}" \
      --policy-document file:///dev/stdin >/dev/null
fi
ensure_role "${lbc_role}" "EKS Pod Identity role for AWS Load Balancer Controller"
aws iam attach-role-policy --role-name "${lbc_role}" --policy-arn "${lbc_policy_arn}"
ensure_association kube-system aws-load-balancer-controller "${lbc_role_arn}"

helm repo add eks https://aws.github.io/eks-charts --force-update
helm repo update eks
helm upgrade --install aws-load-balancer-controller eks/aws-load-balancer-controller \
  --version "${lbc_chart_version}" --namespace kube-system \
  --kube-context "${context}" \
  --set clusterName="${cluster_name}" --set region="${region}" --set vpcId="${vpc_id}" \
  --set serviceAccount.create=true --set serviceAccount.name=aws-load-balancer-controller \
  --wait --timeout 10m
kubectl --context "${context}" --namespace kube-system rollout status \
  deployment/aws-load-balancer-controller --timeout=5m

echo "EKS storage and NLB prerequisites are ready on ${cluster_name@Q}."
