#!/usr/bin/env bash
# Tear down EVERYTHING this benchmark created. Costs stop after this.
# Deletes: engine jobs, operators, Strimzi, the EKS cluster, IRSA roles/policy,
# and (optionally) the S3 warehouse bucket + Glue database.
set -euo pipefail
cd "$(dirname "$0")"
source ./00_profile.env
_sfi_guard || exit 1
[ -f ./.env.eks ] && source ./.env.eks || true

echo "==> Account $(_sfi_whoami)  cluster=$CLUSTER"
read -rp "    Delete the EKS cluster and all benchmark resources? [yes/no] " ok
[ "$ok" = "yes" ] || { echo "aborted"; exit 1; }

echo "==> delete workloads"
kubectl -n bench delete sparkapplication --all --ignore-not-found 2>/dev/null || true
kubectl -n bench delete flinkdeployment --all --ignore-not-found 2>/dev/null || true
kubectl -n bench delete job --all --ignore-not-found 2>/dev/null || true

echo "==> delete cluster (eksctl removes node group, IRSA roles, and the cluster)"
eksctl delete cluster --name "$CLUSTER" --region "$AWS_REGION" --disable-nodegroup-eviction || true
# If a create rolled back, the CFN stack lingers with TerminationProtection on and
# eksctl can't remove it — disable protection + delete the empty stack directly.
for stk in eksctl-${CLUSTER}-cluster eksctl-${CLUSTER}-nodegroup-bench; do
  if aws cloudformation describe-stacks --stack-name "$stk" --region "$AWS_REGION" >/dev/null 2>&1; then
    aws cloudformation update-termination-protection --no-enable-termination-protection \
      --stack-name "$stk" --region "$AWS_REGION" 2>/dev/null || true
    aws cloudformation delete-stack --stack-name "$stk" --region "$AWS_REGION" 2>/dev/null || true
  fi
done

echo "==> IAM policy"
POLICY_ARN=$(aws iam list-policies --scope Local --query "Policies[?PolicyName=='sfi-iceberg-access'].Arn" --output text 2>/dev/null)
[ -n "$POLICY_ARN" ] && [ "$POLICY_ARN" != "None" ] && aws iam delete-policy --policy-arn "$POLICY_ARN" 2>/dev/null || true

read -rp "    Also DELETE the S3 warehouse bucket '${BUCKET:-?}' and Glue db '${GLUE_DB:-?}'? [yes/no] " ok2
if [ "$ok2" = "yes" ] && [ -n "${BUCKET:-}" ]; then
  aws s3 rb "s3://$BUCKET" --force 2>/dev/null || true
  aws glue delete-database --name "${GLUE_DB:-streaming}" 2>/dev/null || true
  echo "    warehouse + Glue db removed"
fi
rm -f kubeconfig .env.eks .registry.env
echo "==> teardown complete"
