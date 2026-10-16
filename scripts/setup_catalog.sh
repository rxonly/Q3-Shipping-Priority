set -euo pipefail

LK=http://localhost:8181

curl -sf -X POST "$LK/management/v1/bootstrap" \
  -H 'Content-Type: application/json' \
  -d '{"accept-terms-of-use": true}' || echo "bootstrap уже выполнен"

curl -sf -X POST "$LK/management/v1/warehouse" \
  -H 'Content-Type: application/json' \
  -d '{
    "warehouse-name": "lakehouse",
    "storage-profile": {
      "type": "s3",
      "bucket": "lakehouse",
      "endpoint": "http://minio:9000",
      "region": "local-01",
      "path-style-access": true,
      "flavor": "s3-compat",
      "sts-enabled": false
    },
    "storage-credential": {
      "type": "s3",
      "credential-type": "access-key",
      "aws-access-key-id": "minioadmin",
      "aws-secret-access-key": "minioadmin"
    }
  }' && echo "warehouse lakehouse создан" || echo "warehouse уже существует"
