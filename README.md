# APRelay

An ActivityPub relay server built with Swift.

APRelay relays activities between federated instances, enabling cross-instance content discovery. It supports subscriber management, domain blocking, restricted mode (allowlist), and HTTP Signature verification.

> [!WARNING]
> This project is still in early development and has **not been tested in production environments**. Use at your own risk.

## Features

- Authorized Fetch (signed GET requests) support
- Subscriber management with pending / accepted / rejected states
- Manual accept mode for controlled federation
- Domain blocking with optional reason
- Restricted mode (allowlist)
- Redis-backed activity deduplication
- Background job queue for reliable delivery
- Prometheus metrics export on a dedicated port
- WebFinger & NodeInfo 2.1 discovery
- Multi-language homepage (English, Korean, Japanese) with `Accept-Language` detection
- Dark mode & responsive homepage with status badges
- Version tracking with git-based auto-detection
- Admin REST API & CLI commands

## Supported Activities

- Create
- Announce
- Delete
- Update
- Move
- Add
- Remove
- Undo
- Like
- EmojiReact

## Requirements

| Component | Version |
|-----------|---------|
| Swift | 6.3+ |
| OS | macOS 14+ / Linux |
| Redis (or Valkey) | 7+ |

## Getting Started

### Build

```bash
swift build
```

### Run

```bash
swift run APRelay serve
```

### Test

```bash
swift test
```

#### Using Docker

```bash
docker run --rm -v "$(pwd):/build" -w /build swift:6.3-noble swift test
```

#### Redis Integration Tests

The Redis repository tests are skipped unless `REDIS_TEST_URL` points at a live Redis or Valkey instance. They only touch keys for `*.redis-test.example` domains and one `redis-test.*` setting, but use a dedicated database index anyway:

```bash
REDIS_TEST_URL=redis://127.0.0.1:6379/15 swift test --filter RedisRelayRepositoryTests
```

#### HTML Snapshots

To visually inspect the rendered homepage without running the server, generate self-contained HTML snapshots by setting the `HTML_SNAPSHOT_DIR` environment variable:

```bash
HTML_SNAPSHOT_DIR=html-snapshots swift test --filter HTMLSnapshotTests
open html-snapshots/
```

Snapshots are organized by locale and scenario:

```
html-snapshots/
  en/
    default.html
    restricted-mode.html
    with-subscribers.html
    ...
  ja/
  ko/
```

Each file inlines the CSS so it can be opened directly in a browser. The snapshot directory is automatically `.gitignore`d on first run.

## Docker

### Using Docker Compose (recommended)

```bash
docker compose up -d
```

This starts the relay server and a Valkey (Redis-compatible) instance. The relay is available at `http://localhost:8080` by default.

### Using Docker directly

```bash
docker build -t ap-relay \
  --build-arg SOURCE_COMMIT=$(git rev-parse HEAD) .
docker run -p 8080:8080 \
  -e RELAY_URL=https://relay.example.com \
  -e REDIS_URL=redis://your-redis:6379 \
  -e ADMIN_TOKEN=your-secret-token \
  ap-relay

# With Prometheus metrics on port 9090
docker run -p 8080:8080 -p 9090:9090 \
  -e RELAY_URL=https://relay.example.com \
  -e REDIS_URL=redis://your-redis:6379 \
  -e ADMIN_TOKEN=your-secret-token \
  -e METRICS_BIND=0.0.0.0:9090 \
  ap-relay
```

### Container registry

Pre-built images are available from GitHub Container Registry:

```bash
docker pull ghcr.io/sinoru/swift-ap-relay:latest
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `RELAY_URL` | Public base URL of the relay | `http://127.0.0.1:8080` |
| `REDIS_URL` | Redis connection URL | `redis://localhost:6379` |
| `ADMIN_TOKEN` | Bearer token for Admin API authentication | (empty) |
| `MANUAL_ACCEPT` | Require admin approval for new subscribers | `false` |
| `RESTRICTED_MODE` | Only allow explicitly accepted domains | `false` |
| `RELAY_NAME` | Relay name shown on the homepage and in User-Agent | domain from `RELAY_URL` |
| `RELAY_DESCRIPTION` | HTML description shown on the homepage | (empty) |
| `RELAY_FOOTER` | HTML footer shown on the homepage | (empty) |
| `AP_RELAY_VERSION` | Override the version string (auto-detected from git if unset) | (auto-detected) |
| `SOURCE_COMMIT` | Source commit hash for version display (auto-detected from git if unset) | (auto-detected) |
| `SOURCE_REPOSITORY_URL` | Source repository URL for NodeInfo and homepage links | `https://github.com/sinoru/swift-ap-relay` |
| `SOURCE_REPOSITORY_COMMIT_PATH` | URL path prefix for commit links (e.g. `/tree/`, `/commit/`, `/-/commit/`) | `/tree/` |
| `METRICS_BIND` | Bind address for the Prometheus metrics server (e.g. `0.0.0.0:9090`). Disabled when unset. | (disabled) |
| `INSTANCE_INFO_CHECK_INTERVAL` | Interval in seconds between periodic instance info checks. Also serves as a reachability heartbeat; failed checks back off exponentially up to 30 minutes. | `60` (minimum `60`) |
| `DEFAULT_QUEUE_WORKER_COUNT` | Number of workers for the default job queue | System CPU core count |
| `DELIVERY_QUEUE_WORKER_COUNT` | Number of workers for the delivery job queue | System CPU core count |
| `INSTANCE_INFO_QUEUE_WORKER_COUNT` | Number of workers for the instance info job queue | System CPU core count |
| `LOG_LEVEL` | Logging level (`trace`, `debug`, `info`, `notice`, `warning`, `error`, `critical`) | `notice` (production) / `info` (development) |

### Localized Environment Variables

`RELAY_NAME`, `RELAY_DESCRIPTION`, and `RELAY_FOOTER` support per-locale overrides via suffixed variants:

| Suffix Pattern | Example | Locale |
|----------------|---------|--------|
| _(none)_ | `RELAY_NAME` | Default (any language) |
| `__KO` | `RELAY_NAME__KO` | Korean |
| `__JA` | `RELAY_NAME__JA` | Japanese |
| `__ZH_TW` | `RELAY_NAME__ZH_TW` | Chinese (Taiwan) |

The homepage automatically selects the best match based on the visitor's `Accept-Language` header.

## Admin CLI Commands

All admin commands connect to a running relay server via the Admin API.

```bash
# List subscribers
swift run APRelay admin list-subscribers [--state pending|accepted|rejected]

# Accept / reject a subscriber
swift run APRelay admin accept <domain>
swift run APRelay admin reject <domain>

# Block / unblock a domain
swift run APRelay admin block <domain> [--reason "..."]
swift run APRelay admin unblock <domain>

# List blocked domains
swift run APRelay admin list-blocked-domains
```

**Connection options** (available for all admin subcommands):

| Option | Description |
|--------|-------------|
| `--url <URL>` | Full Admin API URL |
| `--hostname, -H <HOST>` | Admin API hostname |
| `--port, -p <PORT>` | Admin API port |
| `--tls` | Use HTTPS |
| `--unix-socket <PATH>` | Unix domain socket path |
