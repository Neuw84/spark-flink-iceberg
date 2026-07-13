#!/usr/bin/env bash
# Grant the engine service accounts least-privilege access to the S3 warehouse
# and the Glue database via IRSA. Run after 02_install.sh.
set -euo pipefail
cd "$(dirname "$0")"
source ./00_profile.env
_sfi_guard || exit 1
source ./.env.eks    # BUCKET, GLUE_DB, ACCOUNT
KMS_KEY_ARN=$(aws kms describe-key --key-id alias/sfi-iceberg-bench-e1 --query 'KeyMetadata.Arn' --output text)

# Least-privilege policy: the warehouse bucket + the streaming Glue DB/tables only.
POLICY_NAME=sfi-iceberg-access
POLICY_ARN=$(aws iam list-policies --scope Local \
  --query "Policies[?PolicyName=='$POLICY_NAME'].Arn" --output text 2>/dev/null)
if [ -z "$POLICY_ARN" ] || [ "$POLICY_ARN" = "None" ]; then
  POLICY_ARN=$(aws iam create-policy --policy-name "$POLICY_NAME" --policy-document "{
    \"Version\":\"2012-10-17\",
    \"Statement\":[
      {\"Effect\":\"Allow\",
       \"Action\":[\"s3:GetObject\",\"s3:PutObject\",\"s3:DeleteObject\",\"s3:ListBucket\"],
       \"Resource\":[\"arn:aws:s3:::$BUCKET\",\"arn:aws:s3:::$BUCKET/*\"]},
      {\"Effect\":\"Allow\",
       \"Action\":[\"glue:GetDatabase\",\"glue:GetTable\",\"glue:GetTables\",
                   \"glue:CreateTable\",\"glue:UpdateTable\",\"glue:DeleteTable\",\"glue:BatchDeleteTable\",\"glue:GetPartitions\",
                   \"glue:BatchCreatePartition\",\"glue:CreatePartition\"],
       \"Resource\":[\"arn:aws:glue:$WAREHOUSE_REGION:$ACCOUNT:catalog\",
                     \"arn:aws:glue:$WAREHOUSE_REGION:$ACCOUNT:database/$GLUE_DB\",
                     \"arn:aws:glue:$WAREHOUSE_REGION:$ACCOUNT:table/$GLUE_DB/*\"]},
      {\"Effect\":\"Allow\",
       \"Action\":[\"kms:Decrypt\",\"kms:GenerateDataKey\"],
       \"Resource\":[\"$KMS_KEY_ARN\"]}
    ]}" --query 'Policy.Arn' --output text)
fi
echo "policy: $POLICY_ARN"

# Bind to each engine SA via IRSA (eksctl creates the role + annotates the SA).
for ns_sa in "flink:flink" "spark:spark-operator-spark" "bench:sfi-engine"; do
  ns="${ns_sa%%:*}"; sa="${ns_sa##*:}"
  kubectl create namespace "$ns" 2>/dev/null || true
  eksctl create iamserviceaccount --cluster "$CLUSTER" --region "$AWS_REGION" \
    --namespace "$ns" --name "$sa" --attach-policy-arn "$POLICY_ARN" \
    --approve --override-existing-serviceaccounts
done
echo "==> IRSA configured for flink + spark + bench service accounts"

# Flink native-K8s mode needs the engine SA to manage TaskManager pods in `bench`.
kubectl apply -f - <<'RBAC'
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: { name: sfi-flink-native, namespace: bench }
rules:
  - apiGroups: [""]
    # persistentvolumeclaims: Spark dynamic allocation creates+deletecollection's PVCs
    # for shuffle data; without it the driver 403s on cleanup. pods/configmaps/services
    # cover Flink native-K8s + Spark operator pod management.
    resources: ["pods","configmaps","services","persistentvolumeclaims"]
    verbs: ["create","get","list","watch","update","patch","delete","deletecollection"]
  - apiGroups: ["apps"]
    resources: ["deployments"]
    verbs: ["create","get","list","watch","update","patch","delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: { name: sfi-flink-native, namespace: bench }
subjects: [{ kind: ServiceAccount, name: sfi-engine, namespace: bench }]
roleRef: { kind: Role, name: sfi-flink-native, apiGroup: rbac.authorization.k8s.io }
RBAC
echo "==> Flink native-K8s RBAC granted to sfi-engine"

# Flink's s3-fs-hadoop checkpoint plugin uses the default AWS credential chain
# (node instance role), NOT IRSA. For this ephemeral benchmark, attach the same
# least-privilege S3/Glue/KMS policy to the node role so checkpoints can write.
NODE_ROLE=$(aws eks describe-nodegroup --cluster-name "$CLUSTER" --nodegroup-name bench \
  --region "$AWS_REGION" --query 'nodegroup.nodeRole' --output text | sed 's#.*/##')
aws iam attach-role-policy --role-name "$NODE_ROLE" --policy-arn "$POLICY_ARN" 2>/dev/null || true
echo "==> attached sfi-iceberg-access to node role $NODE_ROLE (Flink checkpoint S3 access)"
