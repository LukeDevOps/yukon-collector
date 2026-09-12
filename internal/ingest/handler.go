// Package ingest decodes the wire payloads sent by the yukon agent and hands
// them to a Sink for storage or forwarding. It has no storage of its own.
package ingest

import (
	"io"
	"log/slog"
	"net/http"

	"google.golang.org/protobuf/proto"

	yukonpb "github.com/LukeDevOps/yukon-collector/internal/proto/yukon"
)

// Sink receives decoded payloads. A real backend implements this to persist
// or forward them; the collector itself stays stateless.
type Sink interface {
	AcceptDeltaBatch(batch *yukonpb.DeltaBatch)
	AcceptManifest(manifest *yukonpb.ProbeManifest)
}

// Handler implements the agent-facing HTTP surface described in the yukon
// agent's "Transport" design: one POST per flush interval, body is a
// serialized protobuf message, no gRPC.
type Handler struct {
	sink   Sink
	logger *slog.Logger
}

func NewHandler(sink Sink, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{sink: sink, logger: logger}
}

// Register wires the handler's routes onto mux, matching the paths
// HttpOtlpStyleExporter posts to: {endpoint}/v1/yukon/deltas and
// {endpoint}/v1/yukon/manifest.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/yukon/deltas", h.handleDeltaBatch)
	mux.HandleFunc("POST /v1/yukon/manifest", h.handleManifest)
}

func (h *Handler) handleDeltaBatch(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(w, r)
	if err != nil {
		return
	}
	var batch yukonpb.DeltaBatch
	if err := proto.Unmarshal(body, &batch); err != nil {
		h.logger.Warn("rejecting malformed delta batch", "error", err)
		http.Error(w, "malformed delta batch", http.StatusBadRequest)
		return
	}
	h.sink.AcceptDeltaBatch(&batch)
	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) handleManifest(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(w, r)
	if err != nil {
		return
	}
	var manifest yukonpb.ProbeManifest
	if err := proto.Unmarshal(body, &manifest); err != nil {
		h.logger.Warn("rejecting malformed manifest", "error", err)
		http.Error(w, "malformed manifest", http.StatusBadRequest)
		return
	}
	h.sink.AcceptManifest(&manifest)
	w.WriteHeader(http.StatusAccepted)
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return nil, err
	}
	return body, nil
}
