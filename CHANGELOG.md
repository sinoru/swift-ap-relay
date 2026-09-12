# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- LitePub mutual follow support: relay sends Follow back to instances that follow the relay actor directly, and handles Accept/Reject responses
- Relay support for Move, Add, Remove, and Undo (non-Follow) activity types
- Relay support for Like and EmojiReact activity types
- Periodic instance info fetching for subscriber instances with software name/version, registration status, staff accounts, reachability, and favicon displayed on the homepage
- `INSTANCE_INFO_CHECK_INTERVAL` environment variable to configure instance info check frequency (default: 60 seconds, minimum: 60)
- `DEFAULT_QUEUE_WORKER_COUNT`, `DELIVERY_QUEUE_WORKER_COUNT`, and `INSTANCE_INFO_QUEUE_WORKER_COUNT` environment variables for configurable queue worker counts
- `SOURCE_REPOSITORY_URL` and `SOURCE_REPOSITORY_COMMIT_PATH` environment variables for configurable source repository links

### Changed

- Homepage instance list now displays subscribers in join order instead of alphabetical order
- Prometheus metrics endpoint moved to a dedicated server controlled by `METRICS_BIND` environment variable
- Delivery jobs now run on a dedicated queue separate from the default job queue
- Instance info checks now run on a dedicated `instanceInfo` queue separate from the default job queue, and double as a reachability heartbeat at a default 60-second interval
- Failed instance info checks preserve last-known software/version/staff/favicon metadata while marking the instance unreachable, and back off exponentially (base 60s doubling per failure, capped at 30 minutes) with equal jitter before the next attempt
- Delivery retries now back off exponentially (base 60s doubling per attempt, capped at 30 minutes) with equal jitter instead of retrying immediately
- `DeliveryError.isRetryable` classification removed; retries are uniformly governed by `maxRetryCount` and the new backoff
- Instance info cache TTL is now a fixed 1 hour, decoupled from the check interval
- Japanese locale: use 登録 (registration) instead of 購読 (subscription) for more natural relay terminology

### Fixed

- NodeInfo response now includes required `services` field and uses free-form `metadata` per NodeInfo 2.1 schema
- InstanceInfoFetchJob error logs now include the failing domain name for diagnosis
- Large ActivityPub payloads (e.g. long posts, many mentions) now accepted instead of returning 413 Payload Too Large
- Stale Undo Follow referencing a superseded Follow no longer removes a re-subscribed instance; Undo must come from and be signed by the stored subscriber actor and reference the currently stored Follow (matching id, and actor when present)
- Accept/Reject responses to the relay's outbound Follow are now honored only when signed by the stored subscriber actor
- Follow activities whose actor does not match the verified HTTP signature signer are now ignored, so the stored subscriber identity is always signature-bound
- Undo with a bare-URI object that does not reference the stored Follow is now relayed through the normal broadcast path instead of being silently dropped, so URI-encoded Undo of non-Follow activities (e.g. an un-boost) is no longer lost
- Subscriber writes are now atomic Lua scripts in Redis: the subscriber hash and its state/all index sets can no longer disagree when two replicas (or a Follow and an admin action) write the same domain concurrently, and delete cleans up every state set
- A Follow or admin accept/reject racing an admin block can no longer reinstate the blocked domain; the repository refuses the write atomically (inbox responds 403 and releases the dedup slot, admin API responds 409)
- Signing key bootstrap now uses `HSETNX`, so replicas booting simultaneously against an empty store all adopt the same key pair instead of the last writer overwriting the others
- Blocked and restricted-mode domains are now rejected before an activity reserves a deduplication slot, so once such a domain is unblocked or allowlisted it can re-deliver the same activity id instead of having it silently absorbed as a duplicate until the TTL expires
- Building from a checkout whose path contains spaces no longer fails in the version generator plugin

### Security

- `VerifiedActor.id` is now bound to the origin that actually serves it. When the document fetched from a signature's keyID claims a different `id`, the relay re-fetches that id and requires that the key it advertises verifies the request (Mastodon-style authority confirmation), so a document hosted on an attacker's server can no longer impersonate another domain's actor by declaring its `id`. Standalone Key documents (Misskey `/publickey`, GoToSocial `/main-key`) are followed to their `owner`, and `publicKey.owner` must match the actor id when present. All failures keep the uniform `401 Signature verification failed` response ([#10](https://github.com/sinoru/swift-ap-relay/issues/10))
- Unified HTTP signature verification error responses on `/inbox` to prevent oracle/fingerprinting attacks; all failure modes (missing/malformed Signature, stale Date, Digest mismatch, actor fetch error, invalid signature) now return identical `401 Unauthorized "Signature verification failed"` with server-side warning logs for operator debugging
- Rejected subscriber state is now sticky: a repeat Follow from a rejected domain no longer silently reinstates the subscriber (previously auto-accept mode reset `.rejected` → `.accepted`, making admin rejections effectively one-time deletes). A matching Undo still removes the record (the remote's withdrawal is honored), so a domain can shed the rejected state by unsubscribing and re-following — use the block list for persistent abusers
- HTTP signatures on `/inbox` now require `date` in the signed headers list, and additionally `digest` on POST, following Mastodon's enforcement. Without these, Date freshness and body integrity checks are not cryptographically bound to the signature; Misskey, Pleroma, and Akkoma senders already include both in practice
- A Follow/Undo/Accept/Reject that is ignored because its actor does not match the verified signer now releases its deduplication slot, so a spoofed activity reusing a predicted id can no longer block the genuine actor's later, correctly signed activity until the dedup TTL expires

## [0.0.1] - 2026-04-12

### Added

- ActivityPub relay server with HTTP Signature verification (draft-cavage-http-signatures-06)
- Inbox controller handling Follow, Undo, Create, Announce, Delete, and Update activities
- Pleroma/Akkoma compatibility: accept Follow with relay actor URL as object
- Signed HTTP GET requests for remote actor fetching, enabling compatibility with Mastodon Authorized Fetch (Secure Mode) and Pleroma `authorized_fetch_mode`
- Actor-signer domain validation to prevent activity forgery
- Digest header requirement and conditional Date header validation for POST requests
- Key ID resolution for both fragment-based (`#main-key`) and path-based (`/publickey`) formats
- Shared inbox (`endpoints.sharedInbox`) support for efficient delivery
- Flexible JSON-LD `@context` decoding for mixed arrays (Pleroma format)
- Flexible `actor` field decoding (string or object with `id`)
- Activity deduplication via Redis with atomic SET NX EX and automatic TTL expiration
- Subscriber management with pending/accepted/rejected states and manual accept mode
- Domain blocking and restricted mode (allowlist) support
- Admin REST API with Bearer token authentication
- Admin CLI commands under `APRelay admin`: list-subscribers, accept, reject, block, unblock, list-blocked-domains
- Delivery system with Vapor Queues for reliable activity delivery with exponential backoff retry and smart error classification
- WebFinger, NodeInfo 2.1, and actor endpoint for federation discovery
- RSA-4096 key pair generation and storage
- Prometheus metrics for inbox activities and delivery performance
- Multi-language homepage with English, Korean, and Japanese translations, auto-selected via `Accept-Language` header
- `Localizer` i18n system with JSON translation files and Accept-Language quality factor parsing
- Dark mode support and responsive homepage with status badges, instance grid, and subscriber join dates
- `VersionGeneratorPlugin` build plugin for automatic version and commit detection from git at build time
- `User-Agent` header on outgoing HTTP requests and `Server` response header with relay version info
- Environment variables: `RELAY_URL`, `REDIS_URL`, `ADMIN_TOKEN`, `RELAY_NAME`, `RELAY_DESCRIPTION`, `RELAY_FOOTER`, `MANUAL_ACCEPT`, `RESTRICTED_MODE`, `AP_RELAY_VERSION`, `SOURCE_COMMIT`, `LOG_LEVEL`
- Per-locale environment variable overrides for `RELAY_NAME`, `RELAY_DESCRIPTION`, and `RELAY_FOOTER` (e.g. `RELAY_NAME__KO`)
- GitHub Actions CI/CD: test workflow with swiftly-based Swift toolchain on Ubuntu and macOS, deploy workflow for multi-platform Docker images (amd64/arm64) to GitHub Container Registry
- Docker multi-stage build with jemalloc, static Swift stdlib linking, and non-root user execution
- Docker Compose with Valkey (Redis-compatible) service
- Strict memory safety checking (SE-0458) enabled for all targets
- Comprehensive test suite: inbox integration, signature middleware, admin API, and localization tests

### Security

- Constant-time comparison for admin token authentication via HMAC-based equality
- Unified admin API authentication error responses to prevent configuration state disclosure; all failure cases return identical `401 Unauthorized` with server-side warning logs for operator debugging
