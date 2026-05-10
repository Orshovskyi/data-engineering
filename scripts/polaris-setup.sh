#!/usr/bin/env bash
# Run after: docker compose up -d
# Creates Polaris catalog + RBAC for Trino (lab 5).

set -euo pipefail
POLARIS="${POLARIS_URL:-http://localhost:8181}"

echo "Fetching access token..."
ACCESS_TOKEN="$(curl -sS -X POST "${POLARIS}/api/catalog/v1/oauth/tokens" \
  -d 'grant_type=client_credentials&client_id=root&client_secret=secret&scope=PRINCIPAL_ROLE:ALL' \
  | jq -r '.access_token')"

if [[ -z "${ACCESS_TOKEN}" || "${ACCESS_TOKEN}" == "null" ]]; then
  echo "Failed to obtain token. Is Polaris up on ${POLARIS}?"
  exit 1
fi

echo "Creating catalog polariscatalog (idempotent — may 409 if exists)..."
curl -sS -i -X POST \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  "${POLARIS}/api/management/v1/catalogs" \
  --json '{
  "name": "polariscatalog",
  "type": "INTERNAL",
  "properties": {
    "default-base-location": "s3://warehouse",
    "s3.endpoint": "http://minio:9000",
    "s3.path-style-access": "true",
    "s3.access-key-id": "admin",
    "s3.secret-access-key": "password",
    "s3.region": "dummy-region"
  },
  "storageConfigInfo": {
    "roleArn": "arn:aws:iam::000000000000:role/minio-polaris-role",
    "storageType": "S3",
    "allowedLocations": [
      "s3://warehouse/*"
    ]
  }
}' || true

echo
echo "Catalogs:"
curl -sS -X GET "${POLARIS}/api/management/v1/catalogs" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" | jq

echo "Catalog admin grant (Polaris 1.x uses catalog-roles / principal-roles)..."
curl -sS -X PUT "${POLARIS}/api/management/v1/catalogs/polariscatalog/catalog-roles/catalog_admin/grants" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  --json '{"grant":{"type":"catalog", "privilege":"CATALOG_MANAGE_CONTENT"}}' | jq .

echo "Principal role data_engineer..."
curl -sS -X POST "${POLARIS}/api/management/v1/principal-roles" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  --json '{"principalRole":{"name":"data_engineer"}}' | jq . || true

echo "Link data_engineer -> catalog_admin..."
curl -sS -X PUT "${POLARIS}/api/management/v1/principal-roles/data_engineer/catalog-roles/polariscatalog" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  --json '{"catalogRole":{"name":"catalog_admin"}}' | jq .

echo "Grant data_engineer to root..."
curl -sS -X PUT "${POLARIS}/api/management/v1/principals/root/principal-roles" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  --json '{"principalRole": {"name": "data_engineer"}}' | jq . || true

echo "Root principal roles:"
curl -sS -X GET "${POLARIS}/api/management/v1/principals/root/principal-roles" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" | jq

echo "Done. Connect: docker compose exec -it trino trino --server localhost:8080 --catalog iceberg"
