package main

import (
	"log/slog"
	"net/http"

	"github.com/LukeDevOps/yukon-collector/internal/auth"
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
func registerRoutes(mux *http.ServeMux, logger *slog.Logger, authToken string, limiter *ratelimit.Limiter) {
	if logger == nil {
		logger = slog.Default()
	}
	sink := ingest.NewLogSink(logger)
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
}
