# yukon-collector

The ingest/decode layer for [yukon](https://github.com/LukeDevOps/yukon)'s
OTLP-style push export. Yukon is a Java agent that instruments a running JVM
to find dead code paths — endpoints, methods, and branches that are
reachable but never actually exercised. Each instrumented instance pushes
periodic batches (hit counts, probe metadata) to a collector over HTTP.
This repo is that collector: it decodes the wire payloads and hands them off
for storage. It doesn't store anything itself.

## Why a separate repo

Storage and multi-tenant aggregation (the part that turns raw hit counts
into "this endpoint has been dead for six months across every instance")
is a closed-source backend, kept out of this repo by design. That mirrors
OpenTelemetry's own collector-vs-backend split: the collector is a thin,
reusable, open piece; what a vendor does with the data downstream isn't.
Decoding OTLP-style protobuf and routing it somewhere is generic enough to
be worth open-sourcing on its own, independent of any particular backend.

## How it fits together

```
yukon agent  --POST protobuf-->  yukon-collector  -->  Sink
(JVM, pushes                     (this repo,            (real backend:
 every 30-60s)                    decode only)           storage, aggregation)
```

The agent's `HttpOtlpStyleExporter` posts three payload types, matching
the paths this collector serves. Each one carries the same resource
attributes: service namespace (optional), service name, version,
instance ID, environment, and a run ID. A service is known by its
namespace and name together. The agent makes a fresh random run ID for each process, so an
instance restarted under a pinned instance ID still names a different run.

- `POST /v1/yukon/deltas` — a `DeltaBatch`: resource attributes plus
  per-probe hit counts since the last successful flush. Sent every flush interval even when empty, as a
  liveness heartbeat — an idle instance and a dead one both need to be
  distinguishable from silence. It also carries endpoint hit totals, one
  entry per HTTP endpoint a web framework has matched a request to.
- `POST /v1/yukon/manifest` — a `ProbeManifest`: resource attributes
  plus a map from probe IDs to their source location (class, method,
  line, branch index), sent incrementally so the collector only needs metadata for probes it hasn't already seen.
  It also carries the endpoints a web framework serves and any endpoint
  module that switched itself off after a linkage failure against a
  framework version it does not match.
- `POST /v1/yukon/static-baseline` — a `StaticBaseline`: an opt-in,
  once-per-process static scan of a service instance's classes, sent as
  one or more chunks. Every chunk carries the same instance and
  `scanned_at`, so `(instance, scanned_at)` identifies one scan; a
  backend must hold every chunk (`chunk_index` of `chunk_count`) before
  it can diff the scan against anything.

All three are protobuf over plain HTTP, not gRPC — the agent sends one batch per
flush interval per instance, so gRPC's multiplexing advantage doesn't apply,
and plain HTTP avoids shading grpc-java/Netty into every instrumented JVM.
See the agent's own `CLAUDE.md` ("Transport", "Hit-data recording &
export") for the full reasoning; the schema itself lives in the agent
repo and is published to the Buf Schema Registry as
[`buf.build/lukedevops-oss/yukon`](https://buf.build/lukedevops-oss/yukon).

## Architecture

- `buf.build/gen/go/lukedevops-oss/yukon/protocolbuffers/go` — Go bindings
  for the yukon wire schema, generated remotely by the BSR and pulled in
  as an ordinary Go module dependency. No local `.proto` copy or `protoc`
  step in this repo.
- `ingest.Handler` — decodes the three payload types above and
  hands each to a `Sink`. Rejects malformed bodies, and decoded ones
  whose resource lacks the service name, instance ID or run ID a backend
  needs to attribute them, with `400` before they reach the sink; accepts
  valid ones with `202`. All three payloads are checked the same way,
  since `class_id` and every cumulative total are only meaningful within
  one run of one instance. A service name that is blank, and a service
  name or namespace that is `.` or `..`, are rejected the same way: a
  service is read at a URL path that holds both, and browsers drop dot
  segments even when they are escaped.
- `ingest.Sink` — the seam a real backend implements. The
  collector has no storage of its own, so this interface is the entire
  contract between "decoded a payload" and "did something with it."
  Two implementations live here: `LogSink`, which only logs what it
  receives and stands in for a backend during development, and
  `forward.ForwardingSink` below. `ingest` and `metrics` are importable
  packages, not `internal`, so a backend written in Go can embed the same
  `Handler` and implement `Sink` itself; the rest of this repo stays
  internal.
- `internal/forward` — `ForwardingSink` relays each decoded payload to a
  backend over the same protobuf-over-HTTP shape the collector accepts,
  the way an OTel Collector exporter re-sends OTLP downstream. Payloads
  are queued and sent by background workers, sharded by service
  namespace, service name and instance ID so one instance's failing
  backend calls can't hold up another's. The run ID is left out of the shard key, so a restarted
  instance keeps its shard and its payloads stay in order. Retries use
  time-bounded exponential backoff on `429`/`502`/`503`/`504`. A full
  shard queue refuses the payload with `503` instead of queuing it, so
  the agent sends it again.
- `internal/processor` — sinks that wrap another sink, changing or
  inspecting a decoded payload before passing it on: the role an OTel
  Collector processor plays. Three exist, `Environment`, `Namespace` and
  `Redaction`.
- `internal/auth` — a shared-secret bearer-token check, wrapped around the
  ingest routes as HTTP middleware. Kept separate from `ingest.Handler` so
  the decode layer stays auth-agnostic.
- `internal/ratelimit` — a per-client-IP token bucket, also wrapped around
  the ingest routes as HTTP middleware, checked before auth so a request
  flood is capped regardless of whether it carries a valid token.
- `metrics` — a handful of counters served at `GET /metrics` in
  the Prometheus text format, with no client library dependency.
- `cmd/yukon-collector` — a minimal HTTP server wiring the sink into the
  handler and listening on `:4319` (the agent's default
  `collectorEndpoint`), overridable via `YUKON_COLLECTOR_ADDR`.

## Running it

The collector requires an auth token before it will start (see
[Authentication](#authentication) below), so the minimal way to run it
locally is:

```
YUKON_COLLECTOR_INSECURE_NO_AUTH=1 go run ./cmd/yukon-collector
```

Listens on `:4319` by default; set `YUKON_COLLECTOR_ADDR` to change that.
Point the yukon agent's `collectorEndpoint` at it, or send a payload by
hand. This posts a `DeltaBatch` for service `demo`, instance `i1`, run
`r1`, with no probe deltas (the same shape the agent sends as an idle
heartbeat):

```
printf '\x0a\x0e\x0a\x04demo\x1a\x02i1\x2a\x02r1' | curl -s -o /dev/null -w '%{http_code}\n' \
  -X POST http://localhost:4319/v1/yukon/deltas \
  -H 'Content-Type: application/x-protobuf' --data-binary @-
```

Expect `202`. Without forwarding configured (see
[Forwarding](#forwarding)), the collector logs each payload it receives
and does nothing else with it.

### Logging

Logs are JSON lines on stdout at `info` and above. Set
`YUKON_COLLECTOR_LOG_LEVEL` to `debug`, `info`, `warn`, or `error` to
change that. Without forwarding configured every payload is logged at
`info`, so `warn` is the quiet setting for a busy collector.

### Authentication

Set `YUKON_COLLECTOR_AUTH_TOKEN` to require a matching
`Authorization: Bearer <token>` header on `/v1/yukon/deltas`,
`/v1/yukon/manifest`, and `/v1/yukon/static-baseline`. Point the agent's
exporter at the same token so its requests carry the header.

```
YUKON_COLLECTOR_AUTH_TOKEN=s3cret go run ./cmd/yukon-collector
```

To rotate the token without refusing any agent, list more than one,
separated by commas. A request passes when its token matches any of
them, so old and new agents both get through while the fleet moves
over. Spaces around each token are trimmed, and an empty entry (`a,,b`
or a trailing comma) stops the collector at startup. A token in this
variable cannot contain a comma.

```
YUKON_COLLECTOR_AUTH_TOKEN=old-token,new-token go run ./cmd/yukon-collector
```

A token set in an environment variable shows up in process listings and
container inspection. To keep it out of them, put the tokens in a file,
one per line, and name the file in `YUKON_COLLECTOR_AUTH_TOKEN_FILE`
instead. Blank lines and lines starting with `#` are skipped, so a token
in a file cannot start with `#`. Spaces around each line are trimmed.
This is the form a mounted Kubernetes or Docker secret takes. Setting
both `YUKON_COLLECTOR_AUTH_TOKEN` and
`YUKON_COLLECTOR_AUTH_TOKEN_FILE` stops the collector at startup.

```
# agents on the old key until the fleet has rolled
old-token
new-token
```

The collector reads the file again every 30 seconds, so a token added
to or removed from it takes effect without a restart. Kubernetes updates
a mounted secret in place and sends no signal, which is why this is a
timed re-read. If a re-read fails, or finds no token, the collector
keeps the tokens it last read, logs a warning, and counts it in
`yukon_collector_token_reload_failures_total{file="auth"}`, so a
re-read that finds no token cannot lock every agent out. A re-read can
still catch a file that is half written and apply what it finds until
the next re-read.

How to change the file depends on how it reaches the collector:

- A Kubernetes secret mounted as a volume updates by itself after the
  kubelet next syncs, often a minute or two. A secret mounted with
  `subPath` never updates, so the collector keeps the old tokens.
- A single file bind-mounted into a container (a Compose `secrets:`
  entry with `file:`, or `-v ./tokens:/run/secrets/tokens`) pins the
  file the container saw at start. Edit it in place, or mount its
  directory instead. Renaming a new file over it leaves the container
  reading the old one, with no warning.
- A file the collector reads directly, or one inside a mounted
  directory, is safest to change by writing a new file and renaming it
  over the old one, so a re-read never sees it half written.
- A Docker Swarm secret never changes in place. Rotating one replaces
  the container, which reads the new file at startup.

At startup there is no
earlier value to fall back on: a file that cannot be read or holds no
token stops the collector, even with `YUKON_COLLECTOR_INSECURE_NO_AUTH`
set.

`/healthz` never requires the token, so liveness/readiness probes keep
working unauthenticated.

The collector fails closed: if neither `YUKON_COLLECTOR_AUTH_TOKEN` nor
`YUKON_COLLECTOR_AUTH_TOKEN_FILE` is set, it
refuses to start rather than running unauthenticated. To run without
auth anyway (local development only — never for anything reachable
outside your machine), opt out explicitly:

```
YUKON_COLLECTOR_INSECURE_NO_AUTH=1 go run ./cmd/yukon-collector
```

Or as a container. The same fail-closed rule applies, so the token (or
the explicit opt-out) has to be passed in:

```
docker build -t yukon-collector .
docker run --rm -p 4319:4319 -e YUKON_COLLECTOR_AUTH_TOKEN=s3cret yukon-collector
```

### TLS

The collector serves plain HTTP only, so it must sit behind a
TLS-terminating reverse proxy or load balancer whenever agents reach it
across a network you do not control. Without one, the agent's bearer
token and every payload travel in the clear, including string literals,
which the redaction processor only sees once they reach the collector.
Point the agent's `endpoint` at the proxy's `https` URL, and make sure
agents and anything else outside your control can reach the listener
only through the proxy; that is also the condition for setting
`YUKON_COLLECTOR_CLIENT_IP_HEADER` (see below). Probes and scrapers of
`/healthz` and `/metrics` inside your network can still reach it
directly. Likewise, use an `https` `YUKON_COLLECTOR_FORWARD_URL`
whenever the backend is reached across a network you do not control,
since that hop carries `YUKON_COLLECTOR_FORWARD_AUTH_TOKEN`.

### Rate limiting

`/v1/yukon/deltas`, `/v1/yukon/manifest`, and `/v1/yukon/static-baseline`
are throttled per client IP: 5 requests/second with a burst of 20 by
default, sized around the agent's 30-60s flush interval. Override with
`YUKON_COLLECTOR_RATE_LIMIT_RPS` and `YUKON_COLLECTOR_RATE_LIMIT_BURST`,
or set the rate to `0` to disable it.

```
YUKON_COLLECTOR_RATE_LIMIT_RPS=10 YUKON_COLLECTOR_RATE_LIMIT_BURST=50 go run ./cmd/yukon-collector
```

A throttled request gets `429` with a `Retry-After` header.

The limit is keyed on the connection's remote address. Behind a reverse
proxy or load balancer every agent arrives from the proxy's address and
they all share one bucket, so set `YUKON_COLLECTOR_CLIENT_IP_HEADER` to
the header the proxy writes the real client address into
(`X-Forwarded-For`, `X-Real-IP`, and so on). For a comma-separated list
the last entry is used, since that is the one the nearest proxy wrote.
The header is trusted as given: only set this when the collector cannot
be reached except through that proxy.

```
YUKON_COLLECTOR_CLIENT_IP_HEADER=X-Forwarded-For go run ./cmd/yukon-collector
```

`/healthz` is never throttled, for the same liveness/readiness reason it's
never gated on auth.

`GET /healthz` returns `200` once the server is up, for liveness/readiness
probes. The image has no shell or `curl`, so `yukon-collector healthcheck`
probes it instead: one GET of `/healthz` on the address
`YUKON_COLLECTOR_ADDR` names (loopback when the host is empty or a
wildcard), exiting 0 or 1, and never through a proxy. The image's
`HEALTHCHECK` runs it.

### Metrics

`GET /metrics` serves counters in the Prometheus text format. Like
`/healthz` it is unauthenticated and unthrottled; it exposes only counts.

| Counter | Labels | Meaning |
| --- | --- | --- |
| `yukon_collector_ingest_accepted_total` | `payload` | Decoded, valid, handed to the sink |
| `yukon_collector_ingest_rejected_total` | `payload`, `reason` | Turned away before or by the sink: `content_type`, `too_large`, `read`, `malformed`, `invalid`, `sink` (the sink refused the payload; the response was `503`) |
| `yukon_collector_auth_rejected_total` | | 401 responses |
| `yukon_collector_token_reload_failures_total` | `file` | A token file re-read that failed or found no usable token, so the last good value stayed; `file` is `auth` (`YUKON_COLLECTOR_AUTH_TOKEN_FILE`) or `forward` (`YUKON_COLLECTOR_FORWARD_AUTH_TOKEN_FILE`) |
| `yukon_collector_rate_limited_total` | | 429 responses |
| `yukon_collector_forward_delivered_total` | `payload` | Backend accepted the payload |
| `yukon_collector_forward_retries_total` | `payload` | Attempts made after a retryable failure |
| `yukon_collector_forward_dropped_total` | `payload`, `reason` | Discarded without delivery: `marshal`, `permanent`, `retry_exhausted`, `shutdown_deadline`, `shutdown_attempt_failed` |
| `yukon_collector_forward_refused_total` | `payload`, `reason` | Not taken and answered `503` so the sender sends it again: `queue_full`, `shutting_down` |
| `yukon_collector_environment_mismatch_total` | `payload` | The agent's environment differed from the collector's configured one (see [Environment](#environment)); `payload` is `deltas`, `manifest`, or `static_baseline` |
| `yukon_collector_namespace_mismatch_total` | `payload` | The agent's service namespace differed from the collector's configured one (see [Namespace](#namespace)); `payload` is `deltas`, `manifest`, or `static_baseline` |
| `yukon_collector_redacted_literals_total` | `payload` | String literal parts replaced by the redaction processor (see [Redaction](#redaction)); `payload` is `manifest` or `static_baseline` |

`payload` is `deltas`, `manifest`, or `static_baseline`. The dropped
counter is the one to alert on: every increment is agent data that never
reached the backend. A steadily rising refused counter means the backend
is slower than the fleet, or the queue is too small; no data is lost
while agents keep retrying. A rising token reload failures counter means
a token file is broken and the collector is still on the tokens it last
read, so a rotation has not taken effect.

### Forwarding

By default the collector only logs what it receives. Set
`YUKON_COLLECTOR_FORWARD_URL` to the base URL of a backend and every
decoded payload is relayed there instead, as a `POST` to the same
`/v1/yukon/deltas`, `/v1/yukon/manifest`, and
`/v1/yukon/static-baseline` paths with the same `application/x-protobuf`
body. `YUKON_COLLECTOR_FORWARD_AUTH_TOKEN`, if
set, is sent as a bearer token on those requests. Spaces around its
value are trimmed, and a value of only spaces stops the collector at
startup. It is a separate secret from `YUKON_COLLECTOR_AUTH_TOKEN`: the agent authenticates to the
collector, the collector authenticates to the backend, and the two need
not match.

```
YUKON_COLLECTOR_AUTH_TOKEN=s3cret \
YUKON_COLLECTOR_FORWARD_URL=https://backend.example.com \
YUKON_COLLECTOR_FORWARD_AUTH_TOKEN=backend-secret \
go run ./cmd/yukon-collector
```

To keep the key out of the environment, name a file that holds it in
`YUKON_COLLECTOR_FORWARD_AUTH_TOKEN_FILE` instead. The file follows the
same rules as `YUKON_COLLECTOR_AUTH_TOKEN_FILE`: blank lines and `#`
lines are skipped, and spaces are trimmed. It must hold exactly one key,
since the collector sends one key and the backend is where several keys
can overlap during a rotation. Setting both variables stops the
collector at startup, and so does a file that cannot be read or does
not hold exactly one key. While forwarding is on, the collector reads
the file again every 30 seconds and sends the new key from the next
request on, and the same advice on changing the file applies. A re-read
that fails, or finds no key or more than one, keeps the last good key,
logs a warning, and counts in
`yukon_collector_token_reload_failures_total{file="forward"}`.

Forwarding is asynchronous. The agent gets its `202` as soon as the
payload is decoded and queued; background workers deliver it, retrying
`429`/`502`/`503`/`504` with exponential backoff for up to five minutes.
When the payload's queue is full, or the collector is shutting down, the
collector answers `503` instead and does not take the payload. The agent
treats that like any failed flush: it retries a few times, then keeps its
counts and sends them on its next flush, so nothing is lost. The agent
reports a probe again only when its hit count changes. If the collector
acknowledged a payload and then dropped it, a rarely hit probe could look
dead for the life of that agent instance.

A payload that fails after it is queued is dropped and counted: the
backend answered with a status that is not retryable, or retries ran
past five minutes. On shutdown, every payload the collector still holds
gets one more delivery attempt within the shutdown deadline, whether it
sits in a queue or a worker was waiting to retry it.

The defaults match the OTLP HTTP exporter's and should rarely need
changing. Each can be overridden; durations use Go syntax (`30s`, `5m`).

| Variable | Default | Meaning |
| --- | --- | --- |
| `YUKON_COLLECTOR_FORWARD_SHARDS` | `8` | Independent queue/worker pairs; payloads are sharded by service namespace, service name and instance ID |
| `YUKON_COLLECTOR_FORWARD_QUEUE_SIZE` | `64` | Queued payloads per shard before new ones are refused with `503` |
| `YUKON_COLLECTOR_FORWARD_REQUEST_TIMEOUT` | `10s` | Bound on one delivery attempt |
| `YUKON_COLLECTOR_FORWARD_RETRY_INITIAL_INTERVAL` | `5s` | First retry wait, grown 1.5x each attempt with jitter |
| `YUKON_COLLECTOR_FORWARD_RETRY_MAX_INTERVAL` | `30s` | Cap on the retry wait |
| `YUKON_COLLECTOR_FORWARD_RETRY_MAX_ELAPSED_TIME` | `5m` | Total time to keep retrying one payload before dropping it |

Trying this out locally needs no real backend: a second `yukon-collector`
instance is a valid one, since `ForwardingSink` posts the same paths and
body shape this collector itself accepts. Point one instance's
`YUKON_COLLECTOR_FORWARD_URL` at another's address and its `LogSink` will
print what the first instance relayed. `internal/forward`'s integration
tests do the same thing without a second process, wiring `ForwardingSink`
straight into a second `ingest.Handler` to confirm a payload decoded on
one end survives the relay and decodes identically on the other.

### Environment

The agent sets `environment` on a payload only when its own `environment`
option is set, so a payload can arrive with the field blank. UAT traffic
looks nothing like prod traffic: code that is dead in prod is often
exercised in UAT, and the reverse. A backend has to know which
environment each payload came from to keep the two apart. An operator
usually runs one collector per environment, so the collector can label
every payload that passes through it, with no change to any JVM's config.

Set `YUKON_COLLECTOR_ENVIRONMENT` to the environment name to stamp onto
every delta batch, manifest and static baseline the collector ingests:

```
YUKON_COLLECTOR_ENVIRONMENT=prod go run ./cmd/yukon-collector
```

`YUKON_COLLECTOR_ENVIRONMENT_ACTION` controls what happens when a payload
already names an environment:

| Action | Behaviour |
| --- | --- |
| `insert` (default) | Fills in the environment only when the agent left it blank; an agent's explicit value wins |
| `upsert` | Always writes the collector's value, even over one the agent set; guarantees nothing passing through a prod collector is ever labelled anything else |

When the agent's value differs from the collector's, that is a mismatch:
`yukon_collector_environment_mismatch_total` goes up under either action,
and a `debug` log line names the service, instance and run involved. A
non-zero count means some JVM is configured for a different environment
than the collector it reports to.

Setting `YUKON_COLLECTOR_ENVIRONMENT_ACTION` without also setting
`YUKON_COLLECTOR_ENVIRONMENT` is a misconfiguration and stops the
collector at startup.

### Namespace

A service is known by its namespace and its name, so two teams can each
run a service called `billing` and stay apart. The agent sets
`service_namespace` on a payload only when it finds one, in its own
`serviceNamespace` option or in OpenTelemetry's settings. One
collector often serves one team, and setting that team's namespace once
on the collector is easier than setting it on every agent. See
[ADR 0002](docs/adr/0002-a-namespace-processor-stamps-a-service-namespace.md).

Set `YUKON_COLLECTOR_SERVICE_NAMESPACE` to the namespace to stamp onto
every delta batch, manifest and static baseline the collector ingests:

```
YUKON_COLLECTOR_SERVICE_NAMESPACE=payments go run ./cmd/yukon-collector
```

With no value set, the processor is off. One collector can then serve
every namespace, and each agent's namespace passes through unchanged.

`YUKON_COLLECTOR_SERVICE_NAMESPACE_ACTION` controls what happens when a
payload already names a namespace:

| Action | Behaviour |
| --- | --- |
| `insert` (default) | Fills in the namespace only for an agent that sent none; an agent's explicit value wins |
| `upsert` | Always writes the collector's value, even over one the agent set |

The collector compares the two values after it trims surrounding spaces.
Case counts: `Payments` and `payments` are different namespaces. An agent
value made only of spaces counts as none. When the agent's value differs
from the collector's, `yukon_collector_namespace_mismatch_total` goes up
under either action, and a `debug` log line names the service, instance
and run involved.

Setting `YUKON_COLLECTOR_SERVICE_NAMESPACE_ACTION` without also setting
`YUKON_COLLECTOR_SERVICE_NAMESPACE` is a misconfiguration and stops the
collector at startup.

### Redaction

A branch site's condition and a string `switch`'s case labels can hold
the adopter's own string literals, such as
`System.getenv("ENABLE_LEGACY_DISCOUNT") == "true"`. The collector runs
inside the adopter's network, so it is the last place to hide them before
they leave. The redaction processor does that. See
[ADR 0001](docs/adr/0001-a-redaction-processor-hides-literals-before-they-leave.md).

The agent sends each condition and case label as a list of parts, each
marked as code, string literal or placeholder. The processor looks only
at string literal parts, in manifests and static baselines. A redacted
part keeps its kind, and its text becomes `…`. Code parts, placeholders,
and class, method and file names are never changed, since the pipeline
needs the names. Branch and site keys are not changed either. They still
digest the literal, so someone who holds the keys and the rest of a
condition can test guesses at a weak secret offline.

Two settings turn it on. Both are off by default.

| Variable | Meaning |
| --- | --- |
| `YUKON_COLLECTOR_REDACT_BLOCKED_VALUES` | Regular expressions in Go syntax, one per line, since a comma can appear inside a pattern. A literal that any of them matches anywhere in its text is redacted. Anchor a pattern with `^` and `$` to match the whole literal. Blank lines are ignored. |
| `YUKON_COLLECTOR_REDACT_ALL_LITERALS` | `1` or `true` redacts every string literal. |

```
YUKON_COLLECTOR_REDACT_BLOCKED_VALUES='(?i)password|secret|token
^sk_live_' \
go run ./cmd/yukon-collector
```

A pattern that does not compile stops the collector at startup with the
variable and the pattern's line named. So does a value for
`YUKON_COLLECTOR_REDACT_ALL_LITERALS` that does not parse as a boolean.
`yukon_collector_redacted_literals_total` counts the parts replaced, and a
`debug` log line names the service, instance, run and count for each
payload that had any. Neither ever includes a literal's text.

While either setting is on, the collector also drops every field it does
not know from every payload, delta batches included. Go keeps unknown
fields when it re-encodes a message for forwarding. An agent built
against a newer schema than this collector could send a new field that
carries a literal, and the collector would forward it unseen. The cost is
that a newer agent's new fields are lost until the collector is updated.

Redaction happens only here. An agent that posts straight to a backend,
with no collector in between, sends literals in clear.

## Development

```
go build ./...
go vet ./...
go test ./...
```

The wire schema is owned by the agent repo and published to the
[Buf Schema Registry](https://buf.build/lukedevops-oss/yukon). To pick up a
schema change, bump the Go bindings:

```
go get buf.build/gen/go/lukedevops-oss/yukon/protocolbuffers/go@latest
```

## License

Apache-2.0 — see [LICENSE](LICENSE).
