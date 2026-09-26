---
status: accepted
---

# A namespace processor stamps a service namespace

Decided on 2026-09-26 in a grilling session that spanned this repo, the agent and `yukon-server` (agent ADR 0045, server ADR 0038).

A service is known by its namespace and its name. Often one collector serves one team, and setting that team's namespace once on the collector is easier than setting it on every agent. One collector can also serve every namespace and change nothing.

## The design

- A `Namespace` processor, built like the `Environment` processor and configured the same way: `YUKON_COLLECTOR_SERVICE_NAMESPACE` holds the value, and `YUKON_COLLECTOR_SERVICE_NAMESPACE_ACTION` is `insert` (the default) or `upsert`. A blank value turns the processor off, so a collector with no setting passes each agent's namespace through.
- `insert` fills in a payload that names no namespace. `upsert` also replaces one that names a different namespace. A difference is counted in `yukon_collector_namespace_mismatch_total{payload}`, whichever action is set.
- It stamps delta batches, probe manifests and static baselines, as the `Environment` processor does.
- The forwarding shard key becomes namespace, service and instance, since namespace is part of the service's identity.

## Considered options

- Only the agent sets the namespace. Rejected: a team that runs its own collector would have to configure every agent.
- `upsert` as the default. Rejected: OpenTelemetry's resource processor convention is not to rewrite what the sender said, the same reason the `Environment` processor defaults to `insert`.
