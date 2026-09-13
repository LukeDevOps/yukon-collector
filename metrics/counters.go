package metrics

// The collector's counters. Label values for "payload" are "deltas",
// "manifest", or "static_baseline"; "reason" values are listed on each
// counter.
var (
	// IngestAccepted counts payloads that decoded, passed validation, and
	// reached the sink.
	IngestAccepted = NewCounter("yukon_collector_ingest_accepted_total",
		"Payloads decoded, validated, and handed to the sink.", "payload")

	// IngestRejected counts requests turned away before or by the sink.
	// Reasons: content_type, too_large, read, malformed, invalid, sink.
	IngestRejected = NewCounter("yukon_collector_ingest_rejected_total",
		"Requests rejected before or by the sink.", "payload", "reason")

	// AuthRejected counts requests refused with 401.
	AuthRejected = NewCounter("yukon_collector_auth_rejected_total",
		"Requests refused for a missing or wrong bearer token.")

	// RateLimited counts requests refused with 429.
	RateLimited = NewCounter("yukon_collector_rate_limited_total",
		"Requests refused because the client's rate limit was exceeded.")

	// ForwardDelivered counts payloads the backend accepted.
	ForwardDelivered = NewCounter("yukon_collector_forward_delivered_total",
		"Payloads the backend accepted.", "payload")

	// ForwardRetries counts delivery attempts made after a first failure.
	ForwardRetries = NewCounter("yukon_collector_forward_retries_total",
		"Delivery attempts made after a retryable failure.", "payload")

	// ForwardDropped counts payloads discarded without delivery. Reasons:
	// marshal, shutting_down, queue_full, permanent, retry_exhausted,
	// shutdown_deadline, shutdown_attempt_failed.
	ForwardDropped = NewCounter("yukon_collector_forward_dropped_total",
		"Payloads discarded without a successful delivery.", "payload", "reason")
)
