// Package ingest decodes the wire payloads sent by the yukon agent and hands
// them to a Sink for storage or forwarding. It has no storage of its own.
package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"

	"google.golang.org/protobuf/proto"

	yukonpb "buf.build/gen/go/lukedevops-oss/yukon/protocolbuffers/go"

	"github.com/LukeDevOps/yukon-collector/metrics"
)

// Sink receives decoded payloads. A real backend implements this to persist
// or forward them; the collector itself stays stateless.
//
// ctx is the request's context, so middleware such as an auth or tenant
// resolver can attach request scope for the sink to read back. A non-nil
// error means the payload was not taken: the handler answers 503 and the
// sender is expected to retry.
type Sink interface {
	AcceptDeltaBatch(ctx context.Context, batch *yukonpb.DeltaBatch) error
	AcceptManifest(ctx context.Context, manifest *yukonpb.ProbeManifest) error
	AcceptStaticBaseline(ctx context.Context, baseline *yukonpb.StaticBaseline) error
}

// maxBodyBytes caps a single request body. A static baseline chunk can
// hold up to 20000 method entries and reach several MiB. The agent does
// not retry a 413: it stops sending the rest of the scan on the first
// failure, so a false 413 loses the baseline for the life of that
// process. This limit exists to bound memory use from a bad or hostile
// sender, not to fit any expected payload size.
const maxBodyBytes = 16 << 20 // 16 MiB

// contentType is the only media type the ingest routes accept.
const contentType = "application/x-protobuf"

// DeltaBatchPath, ManifestPath, and StaticBaselinePath are the
// agent-facing ingest routes. A Sink that relays payloads onward (see
// forward.ForwardingSink) posts to the same paths on the backend, so
// both sides share these constants instead of each holding its own copy
// of the literal.
const (
	DeltaBatchPath     = "/v1/yukon/deltas"
	ManifestPath       = "/v1/yukon/manifest"
	StaticBaselinePath = "/v1/yukon/static-baseline"
)

// payloadKind names one payload type for logging and metrics: label is
// the metrics label value, name is the text used in log lines and error
// bodies.
type payloadKind struct {
	label string
	name  string
}

var (
	deltaBatchKind     = payloadKind{label: "deltas", name: "delta batch"}
	manifestKind       = payloadKind{label: "manifest", name: "manifest"}
	staticBaselineKind = payloadKind{label: "static_baseline", name: "static baseline"}
)

// PayloadLabel returns the metrics label for the payload type served at
// path: "deltas", "manifest", or "static_baseline".
func PayloadLabel(path string) string {
	switch path {
	case ManifestPath:
		return manifestKind.label
	case StaticBaselinePath:
		return staticBaselineKind.label
	default:
		return deltaBatchKind.label
	}
}

// Handler implements the agent-facing HTTP surface described in the yukon
// agent's "Transport" design: one POST per flush interval, body is a
// serialized protobuf message, no gRPC.
type Handler struct {
	sink   Sink
	logger *slog.Logger
}

// NewHandler returns a Handler that passes decoded payloads to sink and
// logs to logger, or slog.Default() when logger is nil.
func NewHandler(sink Sink, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{sink: sink, logger: logger}
}

// Register wires the handler's routes onto mux, matching the paths
// HttpOtlpStyleExporter posts to: {endpoint}/v1/yukon/deltas,
// {endpoint}/v1/yukon/manifest, and {endpoint}/v1/yukon/static-baseline.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST "+DeltaBatchPath, h.handleDeltaBatch)
	mux.HandleFunc("POST "+ManifestPath, h.handleManifest)
	mux.HandleFunc("POST "+StaticBaselinePath, h.handleStaticBaseline)
}

func (h *Handler) handleDeltaBatch(w http.ResponseWriter, r *http.Request) {
	var batch yukonpb.DeltaBatch
	if !h.decode(w, r, &batch, deltaBatchKind) {
		return
	}
	if err := validateDeltaBatch(&batch); err != nil {
		h.reject(w, deltaBatchKind, err)
		return
	}
	if err := h.sink.AcceptDeltaBatch(r.Context(), &batch); err != nil {
		h.sinkFailed(w, deltaBatchKind, err)
		return
	}
	metrics.IngestAccepted.Inc(deltaBatchKind.label)
	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) handleManifest(w http.ResponseWriter, r *http.Request) {
	var manifest yukonpb.ProbeManifest
	if !h.decode(w, r, &manifest, manifestKind) {
		return
	}
	if err := validateManifest(&manifest); err != nil {
		h.reject(w, manifestKind, err)
		return
	}
	if err := h.sink.AcceptManifest(r.Context(), &manifest); err != nil {
		h.sinkFailed(w, manifestKind, err)
		return
	}
	metrics.IngestAccepted.Inc(manifestKind.label)
	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) handleStaticBaseline(w http.ResponseWriter, r *http.Request) {
	var baseline yukonpb.StaticBaseline
	if !h.decode(w, r, &baseline, staticBaselineKind) {
		return
	}
	if err := validateStaticBaseline(&baseline); err != nil {
		h.reject(w, staticBaselineKind, err)
		return
	}
	if err := h.sink.AcceptStaticBaseline(r.Context(), &baseline); err != nil {
		h.sinkFailed(w, staticBaselineKind, err)
		return
	}
	metrics.IngestAccepted.Inc(staticBaselineKind.label)
	w.WriteHeader(http.StatusAccepted)
}

// validateResource checks the resource every payload carries: the
// service name, instance ID and run ID a Sink needs to attribute the
// payload. class_id and every cumulative total mean something only
// within one run of one instance. The agent makes a fresh run ID per
// process, so an instance restarted under a pinned instance ID still
// names a different run. An empty run_id means the sender did not set
// it.
func validateResource(res *yukonpb.ResourceAttributes) error {
	if res == nil {
		return errors.New("missing resource")
	}
	if res.GetServiceName() == "" {
		return errors.New("resource.service_name is empty")
	}
	if res.GetServiceInstanceId() == "" {
		return errors.New("resource.service_instance_id is empty")
	}
	if res.GetRunId() == "" {
		return errors.New("resource.run_id is empty")
	}
	return nil
}

// validateDeltaBatch checks the resource a Sink needs to attribute the
// batch. The agent always sends it, even on an empty heartbeat batch,
// so a batch without it is a broken or foreign sender, not a quiet
// instance.
func validateDeltaBatch(batch *yukonpb.DeltaBatch) error {
	return validateResource(batch.GetResource())
}

// validateManifest checks that the manifest's resource names the
// service, instance and run it describes. class_id is assigned per
// run in load order, so the same class_id can mean a different class in
// two instances of the same service, or in two runs of one instance. A
// backend that keys on less than all three would misattribute probes.
func validateManifest(manifest *yukonpb.ProbeManifest) error {
	return validateResource(manifest.GetResource())
}

// validateStaticBaseline checks the fields a backend cannot do without:
// the resource and scanned_at name the scan, and the chunk fields say
// whether the scan is complete. A chunk missing any of them can never be
// attributed or diffed.
func validateStaticBaseline(baseline *yukonpb.StaticBaseline) error {
	if err := validateResource(baseline.GetResource()); err != nil {
		return err
	}
	if baseline.GetScannedAt() <= 0 {
		return errors.New("scanned_at is not set")
	}
	if baseline.GetChunkCount() < 1 {
		return errors.New("chunk_count must be at least 1")
	}
	if baseline.GetChunkIndex() < 0 {
		return errors.New("chunk_index is negative")
	}
	if baseline.GetChunkIndex() >= baseline.GetChunkCount() {
		return errors.New("chunk_index is out of range for chunk_count")
	}
	return nil
}

// reject answers a decoded but unusable payload with 400.
func (h *Handler) reject(w http.ResponseWriter, kind payloadKind, err error) {
	h.logger.Warn("rejecting invalid "+kind.name, "error", err)
	metrics.IngestRejected.Inc(kind.label, "invalid")
	http.Error(w, "invalid "+kind.name+": "+err.Error(), http.StatusBadRequest)
}

// sinkFailed answers a valid payload the sink did not take with 503 and
// no body, so the sender retries instead of the data being lost.
func (h *Handler) sinkFailed(w http.ResponseWriter, kind payloadKind, err error) {
	h.logger.Error("sink rejected "+kind.name, "error", err)
	metrics.IngestRejected.Inc(kind.label, "sink")
	w.WriteHeader(http.StatusServiceUnavailable)
}

// decode reads r's body into msg. It reports false after writing the
// error response itself: 415 for the wrong media type, 413 for a body
// over maxBodyBytes, 400 for anything that is not valid protobuf. kind
// names the payload in log lines, error bodies, and metrics labels.
func (h *Handler) decode(w http.ResponseWriter, r *http.Request, msg proto.Message, kind payloadKind) bool {
	if !checkContentType(w, r) {
		metrics.IngestRejected.Inc(kind.label, "content_type")
		return false
	}
	body, reason, err := readBody(w, r)
	if err != nil {
		metrics.IngestRejected.Inc(kind.label, reason)
		return false
	}
	if err := proto.Unmarshal(body, msg); err != nil {
		h.logger.Warn("rejecting malformed "+kind.name, "error", err)
		metrics.IngestRejected.Inc(kind.label, "malformed")
		http.Error(w, "malformed "+kind.name, http.StatusBadRequest)
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

// readBody reads the request body up to maxBodyBytes. On failure it
// writes the error response and returns the metrics reason label.
func readBody(w http.ResponseWriter, r *http.Request) (body []byte, reason string, err error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	body, err = io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return nil, "too_large", err
		}
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return nil, "read", err
	}
	return body, "", nil
}
