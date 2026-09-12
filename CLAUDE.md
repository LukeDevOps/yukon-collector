# yukon-collector

## Comments

Doc comments on exported types and functions, not inline comments. Reach
for an inline `//` comment only when the implementation itself is
genuinely unintuitive — a non-obvious invariant, a workaround forced by a
specific constraint, something a reader would otherwise misread. Don't
narrate what straightforward code already says.

Run the `humanizer` skill over any comment or doc comment before treating
it as finished. In practice this mostly means: no em dashes, no stock
AI-sounding phrasing ("crucial," "seamless," "robust," "leverage," and
similar), and no sentence that only restates what the code already says.

Don't write comments that describe code relative to time — "now,"
"currently," "previously did X," "new in this change." A comment like that
is stale the moment something else changes, and git history already
carries that context. Describe what the code does and why, not when it
got that way.

Write comments and doc comments in simplified technical English: short
sentences, one main clause each, plain common words over Latinate or
jargon alternatives, no idioms. Favor the concrete verb ("this rejects
bad input") over the abstract nominalization ("this handles rejection of
invalid input").
