package main

import (
	"log/slog"
	"net/http"

	"github.com/LukeDevOps/yukon-collector/internal/auth"
	"github.com/LukeDevOps/yukon-collector/internal/forward"
	"github.com/LukeDevOps/yukon-collector/internal/ingest"
	"github.com/LukeDevOps/yukon-collector/internal/metrics"
	"github.com/LukeDevOps/yukon-collector/internal/ratelimit"
)

// registerRoutes wires the ingest handler and health check onto mux. When
// authToken is non-empty, the ingest routes require a matching
// "Authorization: Bearer <authToken>" header. When limiter is non-nil, the
// ingest routes are throttled per client IP, checked before auth so a
// flood is capped regardless of whether it carries a valid token.
// /healthz and /metrics stay open, unauthenticated and unthrottled: the
// first for liveness/readiness probes, the second for a Prometheus
// scraper, which exposes only counts.
//
// When forwardCfg.URL is non-empty, ingested payloads are relayed to that
// backend via a forward.ForwardingSink built from forwardCfg, which
// registerRoutes returns so the caller can Shutdown it on graceful
// shutdown. An empty URL falls back to logging payloads instead of
// forwarding them, so local/dev/CI runs keep working with no backend at
// all; registerRoutes then returns a nil *forward.ForwardingSink. A URL
// that cannot be used is returned as an error.
func registerRoutes(mux *http.ServeMux, logger *slog.Logger, authToken string, limiter *ratelimit.Limiter, forwardCfg forward.Config) (*forward.ForwardingSink, error) {
	if logger == nil {
		logger = slog.Default()
	}

	var sink ingest.Sink
	var fwd *forward.ForwardingSink
	if forwardCfg.URL != "" {
		forwardCfg.Logger = logger
		var err error
		fwd, err = forward.NewForwardingSink(forwardCfg)
		if err != nil {
			return nil, err
		}
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
	mux.Handle("GET /metrics", metrics.Handler())

	return fwd, nil
}
