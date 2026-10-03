// Package ingest decodes the wire payloads sent by the Otherlode agent and
// hands them to a Sink for storage or forwarding. It has no storage of its
// own.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	otherlodepb "buf.build/gen/go/otherlode/otherlode/protocolbuffers/go/otherlode/v1"

	"github.com/otherlodehq/otherlode-collector/metrics"
)

// Sink receives decoded payloads. A real backend implements this to persist
// or forward them; the collector itself stays stateless.
//
// ctx is the request's context, so middleware such as an auth or tenant
// resolver can attach request scope for the sink to read back. A non-nil
// error means the payload was not taken: the handler answers 503 and the
// sender is expected to retry.
type Sink interface {
	AcceptDeltaBatch(ctx context.Context, batch *otherlodepb.DeltaBatch) error
	AcceptManifest(ctx context.Context, manifest *otherlodepb.ProbeManifest) error
	AcceptStaticBaseline(ctx context.Context, baseline *otherlodepb.StaticBaseline) error
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
	DeltaBatchPath     = "/v1/otherlode/deltas"
	ManifestPath       = "/v1/otherlode/manifest"
	StaticBaselinePath = "/v1/otherlode/static-baseline"
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
// path: "deltas", "manifest", or "static_baseline". Any other path gives
// "deltas".
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

// Handler implements the agent-facing HTTP surface described in the
// Otherlode agent's "Transport" design: one POST per flush interval, body
// is a serialized protobuf message, no gRPC.
type Handler struct {
	sink   Sink
	logger *slog.Logger

	// slots is a counting semaphore for concurrent decodes. It is nil
	// when decodes are not capped.
	slots chan struct{}

	// slotWait is the longest a request waits for a slot.
	slotWait time.Duration
}

// maxSlotWait is how long a request waits for a decode slot. It is shorter
// than the collector's 10 second write timeout, so in most cases the 503
// reaches the client. The write deadline starts when the headers are read,
// so a slow body upload can still use up the rest.
const maxSlotWait = 5 * time.Second

// HandlerOption configures a Handler. See NewHandler.
type HandlerOption func(*Handler)

// WithMaxConcurrentDecodes caps how many requests the Handler decodes
// and passes to the sink at one time. A value of zero or less means no
// cap, which is the default.
//
// Decoding a protobuf can take far more memory than the body: a 16 MiB
// body of empty messages decodes to gigabytes. The decoded message also
// stays alive until the sink returns. The cap bounds decode memory at
// about n times the largest decode. A request reads its body before it
// takes a slot, so bodies waiting for a slot are held as read, up to
// 16 MiB each.
//
// A slow client holds no slot while it sends, and the cap counts only the
// work that needs the memory. A request that finds no free slot waits for
// one. If its context ends first, the Handler writes no response, since
// the client is gone, and counts the request as rejected with reason
// "canceled". If no slot comes free within five seconds, the Handler
// answers 503 with "Retry-After: 1" and counts reason "busy". The wait is
// bounded so the answer usually arrives before the server's write timeout.
func WithMaxConcurrentDecodes(n int) HandlerOption {
	return func(h *Handler) {
		if n > 0 {
			h.slots = make(chan struct{}, n)
		}
	}
}

// NewHandler returns a Handler that passes decoded payloads to sink and
// logs to logger, or slog.Default() when logger is nil.
func NewHandler(sink Sink, logger *slog.Logger, opts ...HandlerOption) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	h := &Handler{sink: sink, logger: logger, slotWait: maxSlotWait}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// Register adds POST routes for DeltaBatchPath, ManifestPath and
// StaticBaselinePath to mux. They match the paths HttpOtlpStyleExporter
// posts to.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST "+DeltaBatchPath, h.handleDeltaBatch)
	mux.HandleFunc("POST "+ManifestPath, h.handleManifest)
	mux.HandleFunc("POST "+StaticBaselinePath, h.handleStaticBaseline)
}

func (h *Handler) handleDeltaBatch(w http.ResponseWriter, r *http.Request) {
	handle(h, w, r, deltaBatchKind, validateDeltaBatch, h.sink.AcceptDeltaBatch)
}

func (h *Handler) handleManifest(w http.ResponseWriter, r *http.Request) {
	handle(h, w, r, manifestKind, validateManifest, h.sink.AcceptManifest)
}

func (h *Handler) handleStaticBaseline(w http.ResponseWriter, r *http.Request) {
	handle(h, w, r, staticBaselineKind, validateStaticBaseline, h.sink.AcceptStaticBaseline)
}

// handle serves one ingest request. It reads the body, takes a decode
// slot, decodes the body into a new T, validates it, passes it to
// accept, and answers 202. It holds the slot until accept returns. T is
// the pointer type of the payload message.
func handle[T proto.Message](h *Handler, w http.ResponseWriter, r *http.Request, kind payloadKind, validate func(T) error, accept func(context.Context, T) error) {
	body, ok := h.readRequest(w, r, kind)
	if !ok {
		return
	}
	if !h.acquire(r.Context(), w, kind) {
		return
	}
	defer h.release()
	msg := newMessage[T]()
	if !h.unmarshal(w, body, msg, kind) {
		return
	}
	if err := validate(msg); err != nil {
		h.reject(w, kind, err)
		return
	}
	if err := accept(r.Context(), msg); err != nil {
		h.sinkFailed(w, kind, err)
		return
	}
	metrics.IngestAccepted.Inc(kind.label)
	w.WriteHeader(http.StatusAccepted)
}

// newMessage returns a new, empty message of the type T points to.
func newMessage[T proto.Message]() T {
	var zero T
	return zero.ProtoReflect().New().Interface().(T)
}

// validateResource checks the identity a Sink needs: a service name that
// is not blank, an instance ID and a run ID. It rejects a service name
// or namespace of "." or ".." (see isDotSegment). class_id and
// cumulative totals mean something only within one run of one instance,
// so the run ID is required. The agent makes a fresh run ID per process,
// so a restarted instance with a pinned instance ID still names a
// different run.
func validateResource(res *otherlodepb.ResourceAttributes) error {
	if res == nil {
		return errors.New("missing resource")
	}
	name := strings.TrimSpace(res.GetServiceName())
	if name == "" {
		return errors.New("resource.service_name is empty")
	}
	if isDotSegment(name) {
		return fmt.Errorf("resource.service_name is %q, which no URL path can name", name)
	}
	if namespace := strings.TrimSpace(res.GetServiceNamespace()); isDotSegment(namespace) {
		return fmt.Errorf("resource.service_namespace is %q, which no URL path can name", namespace)
	}
	if res.GetServiceInstanceId() == "" {
		return errors.New("resource.service_instance_id is empty")
	}
	if res.GetRunId() == "" {
		return errors.New("resource.run_id is empty")
	}
	return nil
}

// isDotSegment reports whether v is "." or "..". A service is read at a
// URL path that holds its namespace and name as segments, and browsers
// remove a dot segment even when it is percent-escaped. So a service
// named that way could never be opened, and a namespace ".." would lead
// to another service. Backends key a service by the trimmed values, so
// callers pass trimmed values.
func isDotSegment(v string) bool {
	return v == "." || v == ".."
}

// validateDeltaBatch checks the resource a Sink needs to attribute the
// batch. The agent always sends it, even on an empty heartbeat batch,
// so a batch without it is a broken or foreign sender, not a quiet
// instance.
func validateDeltaBatch(batch *otherlodepb.DeltaBatch) error {
	return validateResource(batch.GetResource())
}

// validateManifest checks that the manifest's resource names the
// service, instance and run it describes. class_id is assigned per
// run in load order, so the same class_id can mean a different class in
// two instances of the same service, or in two runs of one instance. A
// backend that keys on less than all three would misattribute probes.
func validateManifest(manifest *otherlodepb.ProbeManifest) error {
	return validateResource(manifest.GetResource())
}

// validateStaticBaseline checks the fields a backend cannot do without:
// the resource and scanned_at name the scan, and the chunk fields say
// whether the scan is complete. A chunk missing any of them can never be
// attributed or diffed.
func validateStaticBaseline(baseline *otherlodepb.StaticBaseline) error {
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

// readRequest reads r's body. It reports false after writing the error
// response itself: 415 for the wrong media type or a content encoding
// other than identity, 413 for a body over maxBodyBytes, 400 for a body
// that cannot be read. kind names the payload in metrics labels.
func (h *Handler) readRequest(w http.ResponseWriter, r *http.Request, kind payloadKind) ([]byte, bool) {
	if !checkContentType(w, r) {
		metrics.IngestRejected.Inc(kind.label, "content_type")
		return nil, false
	}
	body, reason, err := readBody(w, r)
	if err != nil {
		metrics.IngestRejected.Inc(kind.label, reason)
		return nil, false
	}
	return body, true
}

// acquire waits for a free decode slot. Without a cap it returns true at
// once. It reports false when it took no slot. If ctx ends first, it
// counts reason "canceled" and writes no response, since the client is
// gone. If slotWait passes first, it counts reason "busy" and answers 503
// with "Retry-After: 1".
func (h *Handler) acquire(ctx context.Context, w http.ResponseWriter, kind payloadKind) bool {
	if h.slots == nil {
		return true
	}
	timer := time.NewTimer(h.slotWait)
	defer timer.Stop()
	select {
	case h.slots <- struct{}{}:
		return true
	case <-ctx.Done():
		metrics.IngestRejected.Inc(kind.label, "canceled")
		return false
	case <-timer.C:
		metrics.IngestRejected.Inc(kind.label, "busy")
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusServiceUnavailable)
		return false
	}
}

// release frees the slot that a successful acquire took.
func (h *Handler) release() {
	if h.slots != nil {
		<-h.slots
	}
}

// unmarshal decodes body into msg. It reports false after writing a 400
// for anything that is not valid protobuf. kind names the payload in log
// lines, error bodies, and metrics labels.
func (h *Handler) unmarshal(w http.ResponseWriter, body []byte, msg proto.Message, kind payloadKind) bool {
	if err := proto.Unmarshal(body, msg); err != nil {
		h.logger.Warn("rejecting malformed "+kind.name, "error", err)
		metrics.IngestRejected.Inc(kind.label, "malformed")
		http.Error(w, "malformed "+kind.name, http.StatusBadRequest)
		return false
	}
	return true
}

// checkContentType accepts application/x-protobuf with any parameters,
// so a client that appends a charset is not turned away. It rejects a
// Content-Encoding other than identity, since the handler does not
// decompress and would misread the body as malformed protobuf. All
// Content-Encoding lines are joined, and every entry must be empty or
// identity.
func checkContentType(w http.ResponseWriter, r *http.Request) bool {
	for _, enc := range strings.Split(strings.Join(r.Header.Values("Content-Encoding"), ","), ",") {
		if enc = strings.TrimSpace(enc); enc != "" && !strings.EqualFold(enc, "identity") {
			http.Error(w, "unsupported content encoding", http.StatusUnsupportedMediaType)
			return false
		}
	}
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
