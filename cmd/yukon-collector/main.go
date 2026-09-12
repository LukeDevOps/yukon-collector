// Command yukon-collector is the ingest/decode layer for the yukon agent's
// OTLP-style push export. It has no storage of its own; a real backend
// implements ingest.Sink and is wired in here in place of LogSink.
package main

import (
	"log/slog"
	"net/http"
	"os"
)

const defaultAddr = ":4319"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	addr := os.Getenv("YUKON_COLLECTOR_ADDR")
	if addr == "" {
		addr = defaultAddr
	}

	mux := http.NewServeMux()
	registerRoutes(mux, logger)

	logger.Info("yukon-collector listening", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
