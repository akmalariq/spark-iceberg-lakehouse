#!/usr/bin/env bash
set -euo pipefail

systemctl --user start lakehouse-minio.service 2>/dev/null || systemctl --user enable --now lakehouse-minio.service
systemctl --user start lakehouse-nessie.service 2>/dev/null || systemctl --user enable --now lakehouse-nessie.service

for i in $(seq 1 60); do
    m=$(curl -fsS --max-time 2 http://localhost:9100/minio/health/live 2>/dev/null && echo up || echo down)
    n=$(curl -fsS --max-time 2 -o /dev/null -w "%{http_code}" http://localhost:19120/api/v2/config 2>/dev/null || true)
    if [ "$m" = "up" ] && [ "$n" = "200" ]; then
        echo "MinIO (9100) + Nessie (19120) are up"
        exit 0
    fi
    sleep 2
done

echo "services failed to start" >&2
exit 1
