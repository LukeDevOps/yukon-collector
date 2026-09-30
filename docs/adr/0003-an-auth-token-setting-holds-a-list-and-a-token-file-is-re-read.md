---
status: accepted
---

# An auth token setting holds a list, and a token file is re-read

Decided on 2026-09-27 in a grilling session on security that spanned this repo and the private backend.

The collector checks one token from its agents and sends one key to the backend. With one accepted token, rotation means a moment when either the old agents or the new ones are refused. A token set in an environment variable also shows in process listings and container inspection, while Kubernetes and Docker hand secrets over as mounted files. These setting names freeze once the collector is published.

## The design

- `OTHERLODE_COLLECTOR_AUTH_TOKEN` takes one token or a comma-separated list. Spaces around each token are trimmed, and an empty entry is an error at startup. A request passes when its bearer token equals any token in the list. Each comparison is constant-time, as today.
- `OTHERLODE_COLLECTOR_AUTH_TOKEN_FILE` names a file with one token per line. Blank lines and lines that start with `#` are skipped. Setting both variables is an error at startup.
- `OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN_FILE` names a file that holds the one key the collector sends to the backend. Setting it together with `OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN` is an error at startup. The forward key is never a list: the backend accepts several keys per tenant, so rotation happens there.
- The collector re-reads both files every 30 seconds. A mounted Kubernetes secret changes in place and sends no signal, so a signal handler alone would miss it. A re-read that fails, or that finds no token, keeps the last good value, logs a warning and counts `otherlode_collector_token_reload_failures_total{file}`. So a re-read that finds no token cannot lock every agent out. A re-read that catches a hand-edited file half written applies what it finds until the next re-read; a Kubernetes secret mounted as a volume changes by an atomic swap, so it never shows that state. A secret mounted as a single file (a Kubernetes `subPath`, a Compose file secret) never sees a replaced file at all; the README says how to change each kind.
- At startup, the fail-closed rule covers both forms. With no token from either variable, the collector refuses to start unless `OTHERLODE_COLLECTOR_INSECURE_NO_AUTH` is set.

## Considered options

- A separate plural variable, `OTHERLODE_COLLECTOR_AUTH_TOKENS`. Rejected: two names for one setting, and a reader must learn which one wins.
- A primary token and a previous token. Rejected: it allows exactly one overlap, and a fleet that rotates slowly can need two.
- Re-reading only on `SIGHUP`. Rejected: Kubernetes sends no signal when it updates a mounted secret.
