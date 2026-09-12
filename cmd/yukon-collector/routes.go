package main

import (
	"log/slog"
	"net/http"

	"github.com/LukeDevOps/yukon-collector/internal/auth"
	"github.com/LukeDevOps/yukon-collector/internal/ingest"
)

// registerRoutes wires the ingest handler and health check onto mux. When
// authToken is non-empty, the ingest routes require a matching
// "Authorization: Bearer <authToken>" header; /healthz stays open for
// liveness/readiness probes regardless.
func registerRoutes(mux *http.ServeMux, logger *slog.Logger, authToken string) {
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
	mux.Handle("/v1/yukon/", ingestHandler)

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}
