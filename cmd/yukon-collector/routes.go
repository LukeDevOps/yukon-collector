package main

import (
	"log/slog"
	"net/http"

	"github.com/LukeDevOps/yukon-collector/internal/auth"
	"github.com/LukeDevOps/yukon-collector/internal/forward"
	"github.com/LukeDevOps/yukon-collector/internal/ingest"
	"github.com/LukeDevOps/yukon-collector/internal/ratelimit"
)

// registerRoutes wires the ingest handler and health check onto mux. When
// authToken is non-empty, the ingest routes require a matching
// "Authorization: Bearer <authToken>" header. When limiter is non-nil, the
// ingest routes are throttled per client IP, checked before auth so a
// flood is capped regardless of whether it carries a valid token.
// /healthz stays open, unauthenticated and unthrottled, for
// liveness/readiness probes.
//
// When forwardURL is non-empty, ingested payloads are relayed to that
// backend via a forward.ForwardingSink, which registerRoutes returns so
// the caller can Shutdown it on graceful shutdown. An empty forwardURL
// falls back to logging payloads instead of forwarding them, so
// local/dev/CI runs keep working with no backend at all; registerRoutes
// then returns a nil *forward.ForwardingSink.
func registerRoutes(mux *http.ServeMux, logger *slog.Logger, authToken string, limiter *ratelimit.Limiter, forwardURL, forwardAuthToken string) *forward.ForwardingSink {
	if logger == nil {
		logger = slog.Default()
	}

	var sink ingest.Sink
	var fwd *forward.ForwardingSink
	if forwardURL != "" {
		fwd = forward.NewForwardingSink(forward.Config{URL: forwardURL, AuthToken: forwardAuthToken, Logger: logger})
		sink = fwd
	} else {
		logger.Warn("YUKON_COLLECTOR_FORWARD_URL not set; ingest payloads are only logged, not forwarded")
		sink = ingest.NewLogSink(logger)
	}
	handler := ingest.NewHandler(sink, logger)

	ingestMux := http.NewServeMux()
	handler.Register(ingestMux)

	var ingestHandler http.Handler = ingestMux
	if authToken != "" {
		ingestHandler = auth.RequireBearerToken(authToken, ingestMux)
	} else {
		logger.Warn("YUKON_COLLECTOR_AUTH_TOKEN not set; ingest endpoints are unauthenticated")
	}
	if limiter != nil {
		ingestHandler = limiter.Middleware(ingestHandler)
	} else {
		logger.Warn("rate limiting disabled; ingest endpoints accept requests unthrottled")
	}
	mux.Handle("/v1/yukon/", ingestHandler)

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	return fwd
}
