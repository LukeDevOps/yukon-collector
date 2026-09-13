// Package ingest decodes the wire payloads sent by the yukon agent and hands
// them to a Sink for storage or forwarding. It has no storage of its own.
package ingest

import (
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"

	"google.golang.org/protobuf/proto"

	yukonpb "buf.build/gen/go/lukedevops-oss/yukon/protocolbuffers/go"
)

// Sink receives decoded payloads. A real backend implements this to persist
// or forward them; the collector itself stays stateless.
type Sink interface {
	AcceptDeltaBatch(batch *yukonpb.DeltaBatch)
	AcceptManifest(manifest *yukonpb.ProbeManifest)
}

// maxBodyBytes caps a single request body. Delta batches and manifests
// are both small payloads. This limit exists to bound memory use from a
// bad or hostile sender, not to fit any expected payload size.
const maxBodyBytes = 4 << 20 // 4 MiB

// contentType is the only media type the ingest routes accept.
const contentType = "application/x-protobuf"

// DeltaBatchPath and ManifestPath are the agent-facing ingest routes. A
// Sink that relays payloads onward (see forward.ForwardingSink) posts to
// the same paths on the backend, so both sides share these constants
// instead of each holding its own copy of the literal.
const (
	DeltaBatchPath = "/v1/yukon/deltas"
	ManifestPath   = "/v1/yukon/manifest"
)

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
	mux.HandleFunc("POST "+DeltaBatchPath, h.handleDeltaBatch)
	mux.HandleFunc("POST "+ManifestPath, h.handleManifest)
}

func (h *Handler) handleDeltaBatch(w http.ResponseWriter, r *http.Request) {
	var batch yukonpb.DeltaBatch
	if !h.decode(w, r, &batch, "delta batch") {
		return
	}
	if err := validateDeltaBatch(&batch); err != nil {
		h.reject(w, "delta batch", err)
		return
	}
	h.sink.AcceptDeltaBatch(&batch)
	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) handleManifest(w http.ResponseWriter, r *http.Request) {
	var manifest yukonpb.ProbeManifest
	if !h.decode(w, r, &manifest, "manifest") {
		return
	}
	if err := validateManifest(&manifest); err != nil {
		h.reject(w, "manifest", err)
		return
	}
	h.sink.AcceptManifest(&manifest)
	w.WriteHeader(http.StatusAccepted)
}

// validateDeltaBatch checks the fields a Sink needs to attribute the
// batch. The agent always sends them, even on an empty heartbeat batch,
// so a batch without them is a broken or foreign sender, not a quiet
// instance.
func validateDeltaBatch(batch *yukonpb.DeltaBatch) error {
	res := batch.GetResource()
	if res == nil {
		return errors.New("missing resource")
	}
	if res.GetServiceName() == "" {
		return errors.New("resource.service_name is empty")
	}
	if res.GetServiceInstanceId() == "" {
		return errors.New("resource.service_instance_id is empty")
	}
	return nil
}

// validateManifest checks that the manifest names the service it
// describes.
func validateManifest(manifest *yukonpb.ProbeManifest) error {
	if manifest.GetServiceName() == "" {
		return errors.New("service_name is empty")
	}
	return nil
}

// reject answers a decoded but unusable payload with 400.
func (h *Handler) reject(w http.ResponseWriter, what string, err error) {
	h.logger.Warn("rejecting invalid "+what, "error", err)
	http.Error(w, "invalid "+what+": "+err.Error(), http.StatusBadRequest)
}

// decode reads r's body into msg. It reports false after writing the
// error response itself: 415 for the wrong media type, 413 for a body
// over maxBodyBytes, 400 for anything that is not valid protobuf. what
// names the payload in log lines and error bodies.
func (h *Handler) decode(w http.ResponseWriter, r *http.Request, msg proto.Message, what string) bool {
	if !checkContentType(w, r) {
		return false
	}
	body, err := readBody(w, r)
	if err != nil {
		return false
	}
	if err := proto.Unmarshal(body, msg); err != nil {
		h.logger.Warn("rejecting malformed "+what, "error", err)
		http.Error(w, "malformed "+what, http.StatusBadRequest)
		return false
	}
	return true
}

// checkContentType accepts application/x-protobuf with any parameters,
// so a client that appends a charset is not turned away.
func checkContentType(w http.ResponseWriter, r *http.Request) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != contentType {
		http.Error(w, "unsupported content type", http.StatusUnsupportedMediaType)
		return false
	}
	return true
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "failed to read body", http.StatusBadRequest)
		}
		return nil, err
	}
	return body, nil
}
