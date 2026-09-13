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

The agent's `HttpOtlpStyleExporter` posts two payload types, matching the
paths this collector serves:

- `POST /v1/yukon/deltas` — a `DeltaBatch`: resource attributes
  (service name, version, instance ID) plus per-probe hit counts since the
  last successful flush. Sent every flush interval even when empty, as a
  liveness heartbeat — an idle instance and a dead one both need to be
  distinguishable from silence.
- `POST /v1/yukon/manifest` — a `ProbeManifest`: maps probe IDs to their
  source location (class, method, line, branch index), sent incrementally
  so the collector only needs metadata for probes it hasn't already seen.

Both are protobuf over plain HTTP, not gRPC — the agent sends one batch per
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
- `internal/ingest.Handler` — decodes the two payload types above and hands
  each to a `Sink`. Rejects malformed bodies with `400` before they reach
  the sink; accepts valid ones with `202`.
- `internal/ingest.Sink` — the seam a real backend implements. The
  collector has no storage of its own, so this interface is the entire
  contract between "decoded a payload" and "did something with it."
  Two implementations live here: `LogSink`, which only logs what it
  receives and stands in for a backend during development, and
  `forward.ForwardingSink` below.
- `internal/forward` — `ForwardingSink` relays each decoded payload to a
  backend over the same protobuf-over-HTTP shape the collector accepts,
  the way an OTel Collector exporter re-sends OTLP downstream. Payloads
  are queued and sent by background workers, sharded by service identity
  so one instance's failing backend calls can't hold up another's, with
  time-bounded exponential backoff on `429`/`502`/`503`/`504`.
- `internal/auth` — a shared-secret bearer-token check, wrapped around the
  ingest routes as HTTP middleware. Kept separate from `ingest.Handler` so
  the decode layer stays auth-agnostic.
- `internal/ratelimit` — a per-client-IP token bucket, also wrapped around
  the ingest routes as HTTP middleware, checked before auth so a request
  flood is capped regardless of whether it carries a valid token.
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
hand. This posts a `DeltaBatch` for service `demo`, instance `i1`, with
no probe deltas (the same shape the agent sends as an idle heartbeat):

```
printf '\x0a\x0a\x0a\x04demo\x1a\x02i1' | curl -s -o /dev/null -w '%{http_code}\n' \
  -X POST http://localhost:4319/v1/yukon/deltas \
  -H 'Content-Type: application/x-protobuf' --data-binary @-
```

Expect `202`. Without forwarding configured (see
[Forwarding](#forwarding)), the collector logs each payload it receives
and does nothing else with it.

### Authentication

Set `YUKON_COLLECTOR_AUTH_TOKEN` to require a matching
`Authorization: Bearer <token>` header on `/v1/yukon/deltas` and
`/v1/yukon/manifest`. Point the agent's exporter at the same token so its
requests carry the header.

```
YUKON_COLLECTOR_AUTH_TOKEN=s3cret go run ./cmd/yukon-collector
```

`/healthz` never requires the token, so liveness/readiness probes keep
working unauthenticated.

The collector fails closed: if `YUKON_COLLECTOR_AUTH_TOKEN` is unset, it
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

### Rate limiting

`/v1/yukon/deltas` and `/v1/yukon/manifest` are throttled per client IP: 5
requests/second with a burst of 20 by default, sized around the agent's
30-60s flush interval. Override with `YUKON_COLLECTOR_RATE_LIMIT_RPS` and
`YUKON_COLLECTOR_RATE_LIMIT_BURST`, or set the rate to `0` to disable it.

```
YUKON_COLLECTOR_RATE_LIMIT_RPS=10 YUKON_COLLECTOR_RATE_LIMIT_BURST=50 go run ./cmd/yukon-collector
```

`/healthz` is never throttled, for the same liveness/readiness reason it's
never gated on auth.

`GET /healthz` returns `200` once the server is up, for liveness/readiness
probes.

### Forwarding

By default the collector only logs what it receives. Set
`YUKON_COLLECTOR_FORWARD_URL` to the base URL of a backend and every
decoded payload is relayed there instead, as a `POST` to the same
`/v1/yukon/deltas` and `/v1/yukon/manifest` paths with the same
`application/x-protobuf` body. `YUKON_COLLECTOR_FORWARD_AUTH_TOKEN`, if
set, is sent as a bearer token on those requests. It is a separate secret
from `YUKON_COLLECTOR_AUTH_TOKEN`: the agent authenticates to the
collector, the collector authenticates to the backend, and the two need
not match.

```
YUKON_COLLECTOR_AUTH_TOKEN=s3cret \
YUKON_COLLECTOR_FORWARD_URL=https://backend.example.com \
YUKON_COLLECTOR_FORWARD_AUTH_TOKEN=backend-secret \
go run ./cmd/yukon-collector
```

Forwarding is asynchronous. The agent gets its `202` as soon as the
payload is decoded and queued; background workers deliver it, retrying
`429`/`502`/`503`/`504` with exponential backoff for up to five minutes.
Any other failure is logged and the payload dropped, as is a payload that
arrives while its queue is full. On shutdown, queued payloads get one
delivery attempt each within the shutdown deadline.

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
