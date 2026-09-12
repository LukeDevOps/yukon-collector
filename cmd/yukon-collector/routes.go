package main

import (
	"log/slog"
	"net/http"

	"github.com/LukeDevOps/yukon-collector/internal/ingest"
)

func registerRoutes(mux *http.ServeMux, logger *slog.Logger) {
	sink := ingest.NewLogSink(logger)
	handler := ingest.NewHandler(sink, logger)
	handler.Register(mux)
}
