#!/bin/bash
# ============================================================
#  LocalStack initialization script
#  Runs automatically when LocalStack is ready
# ============================================================
set -euo pipefail

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:4566

BUCKET="pipeline-data"

echo "🚀  Initializing LocalStack resources..."

# ─── Create main S3 bucket ────────────────────────────────────────────────────
echo "📦  Creating S3 bucket: $BUCKET"
aws --endpoint-url="$AWS_ENDPOINT_URL" s3api create-bucket \
    --bucket "$BUCKET" \
    --region us-east-1 2>/dev/null || echo "Bucket already exists"

# ─── Create prefix structure (medallion architecture) ─────────────────────────
for layer in bronze silver gold; do
  aws --endpoint-url="$AWS_ENDPOINT_URL" s3api put-object \
      --bucket "$BUCKET" \
      --key "${layer}/.keep" \
      --body /dev/null 2>/dev/null || true
  echo "  ✅  Created layer: s3://$BUCKET/$layer/"
done

# ─── Create logs bucket ────────────────────────────────────────────────────────
aws --endpoint-url="$AWS_ENDPOINT_URL" s3api create-bucket \
    --bucket "pipeline-logs" \
    --region us-east-1 2>/dev/null || echo "Logs bucket already exists"
echo "  ✅  Created bucket: pipeline-logs"

echo ""
echo "✨  LocalStack initialization complete!"
echo "   Buckets:"
aws --endpoint-url="$AWS_ENDPOINT_URL" s3 ls
