---
status: accepted
---

# The project is called Otherlode

Decided on 2026-09-30.

The project was called Yukon. Yukon is already a trade mark in software, held by others. So the name changed before the first release.

## The design

These names matter to this repo and to the people who run the collector:

- The repository is `otherlodehq/otherlode-collector`.
- The Go module path is `github.com/otherlodehq/otherlode-collector`.
- The binary and the container image are both `otherlode-collector`.
- Every setting is an `OTHERLODE_COLLECTOR_*` environment variable.
- Every metric is named `otherlode_collector_*`.
- The collector serves `/v1/otherlode/deltas`, `/v1/otherlode/manifest` and `/v1/otherlode/static-baseline`. It forwards to the same paths on the backend.
- The wire schema comes from `buf.build/otherlode/otherlode`, in the protobuf package `otherlode.v1`. Field numbers, field names and enum values did not change.

## Considered options

- Keep the name Yukon. Rejected for the reason above.
- Accept the old names beside the new ones for a while, as aliases or as a fallback that reads an old setting. Rejected: nothing was released, so no one depends on the old names. Two names for one setting also make a reader learn which one wins.

## Consequences

- The collector has no fallback to the old names. No old setting, metric, path or module path works.
- An agent and a backend must both use the `/v1/otherlode/...` paths. The collector answers an agent that posts to an old path with a `404`. A backend that serves only the old paths answers `404` too, and the collector drops each payload it forwards there as a permanent failure. The agent already has its `202` by then, so the loss shows only in the collector's warning log and in `otherlode_collector_forward_dropped_total{reason="permanent"}`.
- Git history keeps the old name.
