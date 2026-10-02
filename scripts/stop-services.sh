#!/usr/bin/env bash
set -euo pipefail

systemctl --user stop lakehouse-minio.service lakehouse-nessie.service 2>/dev/null || true
echo "MinIO + Nessie stopped"
