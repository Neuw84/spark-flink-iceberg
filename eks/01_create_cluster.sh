#!/usr/bin/env bash
# Create the EKS cluster (security baseline in cluster.yaml).
#
# PREREQS — edit cluster.yaml and replace the placeholders:
#   <YOUR_ACCOUNT_ID>, <YOUR_KMS_KEY_ID>   -> a dedicated KMS CMK for secrets encryption
#   <YOUR_VPC_ID> + the 4 subnet IDs       -> an existing VPC's subnets, OR delete the
#                                             whole `vpc:` block to let eksctl make a VPC.
#
# Create a dedicated KMS key first (prints the key id to paste into cluster.yaml):
#   source eks/00_profile.env
#   aws kms create-key --description "sfi-iceberg-bench secrets enc" \
#     --query 'KeyMetadata.KeyId' --output text
#
# ~15-20 min for cluster + node group.
set -euo pipefail
cd "$(dirname "$0")"
source ./00_profile.env
_sfi_guard || exit 1

# Dedicated us-east-1 KMS CMK for secrets encryption (create once, reuse).
KMS_KEY_ARN=$(aws kms describe-key --key-id alias/sfi-iceberg-bench-e1 --query 'KeyMetadata.Arn' --output text 2>/dev/null || true)
if [ -z "$KMS_KEY_ARN" ] || [ "$KMS_KEY_ARN" = "None" ]; then
  KID=$(aws kms create-key --region "$AWS_REGION" --description "sfi-iceberg-bench secrets (ephemeral)" \
    --tags TagKey=lifecycle,TagValue=ephemeral-benchmark --query 'KeyMetadata.KeyId' --output text)
  aws kms create-alias --region "$AWS_REGION" --alias-name alias/sfi-iceberg-bench-e1 --target-key-id "$KID"
  KMS_KEY_ARN="arn:aws:kms:${AWS_REGION}:$(_sfi_whoami):key/${KID}"
fi
echo "==> KMS key: $KMS_KEY_ARN"
# Use cluster.local.yaml if present (gitignored, reuses an existing VPC when the
# account is at its VPC/EIP limit); otherwise the standalone repo cluster.yaml.
SRC=cluster.yaml; [ -f cluster.local.yaml ] && SRC=cluster.local.yaml
echo "==> using $SRC"
sed "s#__KMS_KEY_ARN__#${KMS_KEY_ARN}#" "$SRC" > /tmp/sfi_cluster.yaml

echo "==> eksctl create cluster (sfi-iceberg-bench us-east-1, 6× m5.2xlarge, ~15-20 min)"
echo "    BILLABLE (~\$2.40/hr). Your other clusters are untouched."
read -rp "    Proceed? [yes/no] " ok
[ "$ok" = "yes" ] || { echo "aborted"; exit 1; }

eksctl create cluster -f /tmp/sfi_cluster.yaml

echo "==> Point the DEDICATED kubeconfig at the cluster (never ~/.kube/config)"
aws eks update-kubeconfig --name "$CLUSTER" --region "$AWS_REGION" --kubeconfig "$KUBECONFIG"

echo "==> Cluster up. Nodes:"
kubectl get nodes -o wide
