---
status: accepted
---

# A redaction processor hides literals before they leave the collector

Decided on 2026-09-24 in a grilling session that spanned this repo, the agent and `yukon-server`.

Under agent ADR 0037 a branch site sends its condition, and under agent ADR 0038 a string `when` sends its case labels. Both can hold the adopter's string literals, such as `System.getenv("ENABLE_LEGACY_DISCOUNT") == "true"`. Until then the agent sent names and digests only. The collector runs in the adopter's network and forwards to a backend outside it, so the collector is where data leaves. Redaction lives there as a processor, the same role an OTel Collector's redaction processor plays, beside the existing `Environment` processor.

## The design

- **Literal parts only.** A condition arrives as a list of parts, each either code or a literal, and a switch case label arrives the same way. The processor looks at literal parts and nothing else. It never parses source text, and it never touches class, method or file names. The pipeline needs those names, and they already travel.
- **Two settings, named after OTel's where the idea matches.** `YUKON_COLLECTOR_REDACT_BLOCKED_VALUES` holds regular expressions, one per line, since a comma can appear inside a pattern. A literal part that any of them matches is replaced by `…`. `YUKON_COLLECTOR_REDACT_ALL_LITERALS=1` replaces every literal part. Both are off by default.
- **A count.** The processor counts the parts it replaced, as a metric beside the existing processor metrics, so an operator can see the rule working without reading payloads.
- **Unknown fields are stripped while redaction is on.** Go protobuf keeps fields it does not know when it re-marshals a payload for forwarding. A collector built against older bindings than the agent would forward a newer literal-carrying field untouched. So while either setting is on, the processor removes unknown fields from every message before passing it on. A newer agent's new fields are then lost until the collector is updated. For a privacy control that is the right direction to fail.
- **Not in the agent.** The agent has no redaction setting of its own, so the policy has one home. A deployment that needs redaction runs this collector. An agent that posts straight to a backend sends literals in clear.

## Considered options

- **Matching patterns over the whole condition text.** Rejected: finding a literal inside Kotlin, Java or Scala source means parsing escapes, raw strings and templates. A pattern would also hit identifiers that happen to match. The agent knows which bytes were a string constant, so it marks them.
- **Redaction in the agent as well.** Rejected: two switches for one rule is how a team believes it is covered when it is not.
- **Re-keying branch and site keys with a keyed hash.** Rejected: keys join builds only while they stay a pure function of the bytecode (agent ADR 0031), and a per-collector secret would split one service's history between collectors.

## Consequences

- Branch and site keys still digest the literal. Someone holding the keys and the rest of a redacted condition can test guesses against a weak secret offline. Redaction stops a literal from being shown or stored. It does not protect a guessable secret from a person who has the keys.
- `yukon-server` imports this module's `ingest.Handler` (server ADR 0002), so it can wrap the same processor around its own sink later as defence in depth. That is logged in the server's STATUS.
