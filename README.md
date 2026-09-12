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
export") for the full reasoning; `proto/yukon.proto` here is a synced copy
of the agent's schema.

## Architecture

- `internal/proto/yukon` — generated Go bindings for `proto/yukon.proto`
  (`protoc` + `protoc-gen-go`).
- `internal/ingest.Handler` — decodes the two payload types above and hands
  each to a `Sink`. Rejects malformed bodies with `400` before they reach
  the sink; accepts valid ones with `202`.
- `internal/ingest.Sink` — the seam a real backend implements. The
  collector has no storage of its own, so this interface is the entire
  contract between "decoded a payload" and "did something with it."
  `LogSink` is the only implementation here: it just logs what it
  receives, standing in for a real backend during development.
- `cmd/yukon-collector` — a minimal HTTP server wiring a `LogSink` into the
  handler and listening on `:4319` (the agent's default
  `collectorEndpoint`), overridable via `YUKON_COLLECTOR_ADDR`.

## Running it

```
go run ./cmd/yukon-collector
```

Listens on `:4319` by default. Point the yukon agent's
`collectorEndpoint` at it, or send a payload by hand:

```
YUKON_COLLECTOR_ADDR=:4319 go run ./cmd/yukon-collector
```

Or as a container:

```
docker build -t yukon-collector .
docker run --rm -p 4319:4319 yukon-collector
```

`GET /healthz` returns `200` once the server is up, for liveness/readiness
probes.

## Development

```
go build ./...
go vet ./...
go test ./...
```

`proto/yukon.proto` is a manually synced copy of the agent's schema, not a
shared package — there's one consumer relationship (this repo tracks the
agent's schema, not the reverse), so a diff script is enough overhead.
Check for drift, or pull in a change, with:

```
scripts/sync-proto.sh              # diff only
scripts/sync-proto.sh --apply      # copy + regenerate Go bindings
```

It assumes the agent repo is checked out at `../yukon`; override with
`YUKON_AGENT_REPO`.

## License

Apache-2.0 — see [LICENSE](LICENSE).
