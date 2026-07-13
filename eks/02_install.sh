#!/usr/bin/env bash
# Install the data plane: Strimzi Kafka, the Flink + Spark operators, and create
# the S3 warehouse bucket + Glue database that replace MinIO + the REST catalog.
# Run after 01_create_cluster.sh.
set -euo pipefail
cd "$(dirname "$0")"
source ./00_profile.env
_sfi_guard || exit 1
ACCOUNT=$(_sfi_whoami)
# Everything co-located in AWS_REGION (us-east-1). With cluster + warehouse in the
# same region, the IRSA webhook injects AWS_REGION=us-east-1 which matches the
# bucket → no Iceberg S3FileIO cross-region 301.
WAREHOUSE_REGION="$AWS_REGION"
BUCKET="sfi-iceberg-wh-${ACCOUNT}"
GLUE_DB="streaming"

echo "==> [1/6] S3 warehouse bucket in $WAREHOUSE_REGION (ARCC: PAB + TLS-only): $BUCKET"
# us-east-1 create-bucket must NOT pass LocationConstraint.
aws s3api create-bucket --bucket "$BUCKET" --region "$WAREHOUSE_REGION" 2>/dev/null || true
aws s3api put-bucket-versioning --bucket "$BUCKET" --versioning-configuration Status=Enabled
aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-ownership-controls --bucket "$BUCKET" \
  --ownership-controls 'Rules=[{ObjectOwnership=BucketOwnerEnforced}]'
# SSE-KMS with the dedicated us-east-1 CMK (same region as the bucket now).
KMS_ALIAS="alias/sfi-iceberg-bench-e1"
aws s3api put-bucket-encryption --bucket "$BUCKET" --server-side-encryption-configuration "{
  \"Rules\":[{\"ApplyServerSideEncryptionByDefault\":{\"SSEAlgorithm\":\"aws:kms\",\"KMSMasterKeyID\":\"$KMS_ALIAS\"},\"BucketKeyEnabled\":true}]}"
aws s3api put-bucket-policy --bucket "$BUCKET" --policy "{
  \"Version\":\"2012-10-17\",
  \"Statement\":[{\"Sid\":\"DenyInsecureTransport\",\"Effect\":\"Deny\",\"Principal\":\"*\",
    \"Action\":\"s3:*\",\"Resource\":[\"arn:aws:s3:::$BUCKET\",\"arn:aws:s3:::$BUCKET/*\"],
    \"Condition\":{\"Bool\":{\"aws:SecureTransport\":\"false\"}}}]}"

echo "==> [2/6] Glue Data Catalog database in $WAREHOUSE_REGION: $GLUE_DB"
aws glue create-database --region "$WAREHOUSE_REGION" --database-input "{\"Name\":\"$GLUE_DB\"}" 2>/dev/null || true

echo "==> [3/6] EBS CSI driver + gp3 default StorageClass (EKS 1.31 ships none)"
eksctl create iamserviceaccount --cluster "$CLUSTER" --region "$AWS_REGION" \
  --namespace kube-system --name ebs-csi-controller-sa \
  --attach-policy-arn arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy \
  --approve --role-only --role-name sfi-ebs-csi-role >/dev/null 2>&1 || true
aws eks create-addon --cluster-name "$CLUSTER" --region "$AWS_REGION" --addon-name aws-ebs-csi-driver \
  --service-account-role-arn "arn:aws:iam::${ACCOUNT}:role/sfi-ebs-csi-role" 2>/dev/null || true
for _ in $(seq 1 20); do
  [ "$(aws eks describe-addon --cluster-name "$CLUSTER" --region "$AWS_REGION" --addon-name aws-ebs-csi-driver --query 'addon.status' --output text 2>/dev/null)" = "ACTIVE" ] && break; sleep 10
done
kubectl apply -f - <<'SC'
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: gp3
  annotations: { storageclass.kubernetes.io/is-default-class: "true" }
provisioner: ebs.csi.aws.com
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
parameters: { type: gp3 }
SC

echo "==> [4/6] Strimzi operator + Kafka (KRaft, 3 brokers)"
kubectl create namespace kafka 2>/dev/null || true
kubectl apply -f "https://strimzi.io/install/latest?namespace=kafka" -n kafka
kubectl -n kafka rollout status deploy/strimzi-cluster-operator --timeout=300s
# wait for the Kafka CRD to register before applying the CR (else "no matches for kind")
for _ in $(seq 1 20); do kubectl get crd kafkas.kafka.strimzi.io >/dev/null 2>&1 && break; sleep 5; done
# The manifest carries a __PARTITIONS__ placeholder (the benchmark run script varies
# it per run). Render a sane default here so the initial topic is valid; each run
# resets the topic to its own partition count anyway.
sed "s/__PARTITIONS__/${PARTITIONS:-12}/g" manifests/strimzi/kafka-cluster.yaml | kubectl apply -f -
echo "    waiting for Kafka to be ready (a few minutes)…"
kubectl -n kafka wait kafka/sfi-bench --for=condition=Ready --timeout=600s

echo "==> [5/6] Flink Kubernetes Operator (+ cert-manager prereq)"
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.16.2/cert-manager.yaml
kubectl -n cert-manager rollout status deploy/cert-manager-webhook --timeout=180s
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.10.0/ 2>/dev/null || true
helm repo update >/dev/null
kubectl create namespace flink 2>/dev/null || true
helm upgrade --install flink-operator flink-operator-repo/flink-kubernetes-operator -n flink --wait --timeout 5m

echo "==> [6/6] Spark Operator"
helm repo add spark-operator https://kubeflow.github.io/spark-operator 2>/dev/null || true
helm repo update >/dev/null
kubectl create namespace spark 2>/dev/null || true
kubectl create namespace bench 2>/dev/null || true
helm upgrade --install spark-operator spark-operator/spark-operator \
  -n spark --set "spark.jobNamespaces={bench}" --wait --timeout 5m

# record for later scripts
{ echo "BUCKET=$BUCKET"; echo "GLUE_DB=$GLUE_DB"; echo "ACCOUNT=$ACCOUNT"; echo "WAREHOUSE_REGION=$WAREHOUSE_REGION"; } > .env.eks
echo "==> data plane installed. Wrote eks/.env.eks"
