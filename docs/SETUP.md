# Local setup (no Docker)

All services run as **systemd user units** on WSL/Linux. Requires Java 21+.

## Install

```bash
# Java (Debian/Ubuntu WSL)
sudo apt install -y openjdk-21-jdk

# MinIO server + client
curl -fsSL -o ~/.local/bin/minio https://dl.min.io/server/minio/release/linux-amd64/minio
curl -fsSL -o ~/.local/bin/mc    https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x ~/.local/bin/minio ~/.local/bin/mc

# Nessie standalone server
curl -fsSL -o .tools/nessie.jar \
  https://github.com/projectnessie/nessie/releases/download/nessie-0.108.4/nessie-quarkus-0.108.4-runner.jar

# Spark (already vendored at .tools/spark-3.5.9-bin-hadoop3)
```

## Service units

The repo's `scripts/start-services.sh`/`stop-services.sh` manage these units
(installed to `~/.config/systemd/user/`):

- `lakehouse-minio.service` — `minio server .tools/minio-data --address ":9100" --console-address ":9101"`
- `lakehouse-nessie.service` — `java -Dquarkus.config.locations=... -jar .tools/nessie.jar`
  (config in `.tools/nessie-conf/application.properties`)

### Why the Nessie config override?

Nessie's bundled config hardcodes `quarkus.management.port=9000`, which clashes
with ClickHouse's native TCP port. `.tools/nessie-conf/application.properties`
moves the management interface to `19121`:

```properties
quarkus.http.port=19120
quarkus.management.port=19121
```

### Ports

| Port | Service |
|---|---|
| 8123 / 9000 | ClickHouse (HTTP / native) — from the stock-tracker project |
| 9100 / 9101 | MinIO API / console |
| 19120 / 19121 | Nessie API / management |

### Nessie notes

- Default version store is **IN_MEMORY** — catalog state resets on restart.
  For persistence, set `quarkus.nessie.version.store.type=ROCKSDB` etc. in the
  override file (fine for demos either way; data lives in MinIO).
- Nessie sets `gc.enabled=false` on created tables. Compaction still works;
  `expire_snapshots` first runs `ALTER TABLE ... SET TBLPROPERTIES
  ('gc.enabled'='true')`.
