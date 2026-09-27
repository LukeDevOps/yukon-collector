# Security policy

## Reporting a vulnerability

Report a vulnerability privately through GitHub's private vulnerability
reporting:

https://github.com/LukeDevOps/yukon-collector/security/advisories/new

Do not open a public issue, pull request or discussion about it.

Please include:

- the affected version, or the commit if you built from source;
- steps to reproduce, with a minimal setup where you can;
- the impact: what an attacker can do, and under which conditions.

## Scope

The collector runs inside your network. It receives payloads from Yukon
agents over HTTP and can forward them to a backend with an API key. These
parts are in scope:

- how it accepts, decodes, authenticates and forwards payloads;
- how it reads its tokens and keys;
- the per-IP rate limiter on the ingest routes;
- the `/healthz` and `/metrics` routes, which need no token.

Problems in the agent belong in the
[yukon](https://github.com/LukeDevOps/yukon) repository. Report problems in
the hosted Yukon service through the link above.

## Supported versions

Nothing is released yet. Fixes land on `master` and go into the next
release. Once releases exist, fixes go into the latest release.

## What to expect

You will get an acknowledgement of your report. After that you will get a
fix, or a reply that explains why no change is needed.
