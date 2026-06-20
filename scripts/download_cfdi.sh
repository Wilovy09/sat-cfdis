#!/usr/bin/env bash
# Download a CFDI XML from S3 by UUID for local inspection.
# Usage: ./scripts/download_cfdi.sh <UUID>
set -euo pipefail

UUID="${1:-}"
if [[ -z "$UUID" ]]; then
  echo "Usage: $0 <UUID>" >&2
  exit 1
fi

UUID_LOWER=$(echo "$UUID" | tr '[:upper:]' '[:lower:]')
BUCKET="pulso-sat-cfdis-390548747493-us-east-2-an"
DB_URL="postgresql://uadquiere_test:nEA295gJ4tbW%21DH6@adquiere-test.crms0e6earhs.us-east-2.rds.amazonaws.com/adquiere_test?sslmode=require"

echo "Looking up CFDI $UUID in DB..."

UUID_UPPER=$(echo "$UUID" | tr '[:lower:]' '[:upper:]')

# Try cfdis table first (already enriched, has discrete columns)
ROW=$(psql "$DB_URL" -t -A -c "
  SELECT rfc_emisor, rfc_receptor, fecha_emision
  FROM pulso.cfdis
  WHERE uuid = '$UUID_UPPER'
  LIMIT 1;
")

if [[ -n "$ROW" ]]; then
  RFC_E=$(echo "$ROW" | cut -d'|' -f1)
  RFC_R=$(echo "$ROW" | cut -d'|' -f2)
  FECHA=$(echo "$ROW" | cut -d'|' -f3)
  YEAR=$(echo "$FECHA"  | cut -d'-' -f1)
  MONTH=$(echo "$FECHA" | cut -d'-' -f2)
  DAY=$(echo "$FECHA"   | cut -d'-' -f3 | cut -dT -f1)
else
  # Fall back to job_invoices metadata JSON
  META=$(psql "$DB_URL" -t -A -c "
    SELECT metadata FROM pulso.job_invoices
    WHERE uuid = '$UUID_UPPER'
    LIMIT 1;
  ")
  if [[ -z "$META" ]]; then
    echo "UUID not found in cfdis or job_invoices" >&2
    exit 1
  fi
  RFC_E=$(echo "$META" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('rfcEmisor') or d.get('RfcEmisor') or d.get('rfc_emisor','UNKNOWN'))" 2>/dev/null)
  RFC_R=$(echo "$META" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('rfcReceptor') or d.get('RfcReceptor') or d.get('rfc_receptor','UNKNOWN'))" 2>/dev/null)
  FECHA=$(echo "$META" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('fecha') or d.get('Fecha') or d.get('fechaEmision') or d.get('FechaEmision',''))" 2>/dev/null)
  YEAR=$(echo "$FECHA"  | cut -d'-' -f1)
  MONTH=$(echo "$FECHA" | cut -d'-' -f2)
  DAY=$(echo "$FECHA"   | cut -d'-' -f3 | cut -dT -f1)
fi

S3_KEY="cfdis/${RFC_E}/${RFC_R}/${YEAR}/${MONTH}/${DAY}/${UUID_LOWER}.xml"
OUT="$(pwd)/${UUID_LOWER}.xml"

echo "RFC emisor:   $RFC_E"
echo "RFC receptor: $RFC_R"
echo "Fecha:        ${YEAR}-${MONTH}-${DAY}"
echo "S3 key:       s3://${BUCKET}/${S3_KEY}"
echo ""

aws s3 cp "s3://${BUCKET}/${S3_KEY}" "$OUT"
echo ""
echo "Saved to: $OUT"
