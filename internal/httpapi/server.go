// Package httpapi exposes the ledger over a JSON REST API described by
// api/openapi.yaml.
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/MahyarNemati/ledger/internal/idempotency"
	"github.com/MahyarNemati/ledger/internal/ledger"
	"github.com/MahyarNemati/ledger/internal/projection"
)

const (
	maxBodyBytes    = 1 << 20
	maxKeyLength    = 255
	defaultPageSize = 100
	maxPageSize     = 1000
)

// Deps are the collaborators the API needs.
type Deps struct {
	Service     *ledger.Service
	Projections projection.Reader
	Idempotency idempotency.Store
	Logger      *slog.Logger
	// Ready reports whether dependencies are reachable.
	Ready func(context.Context) error
}

type server struct {
	Deps
}

// New builds the HTTP handler.
func New(d Deps) http.Handler {
	if d.Logger == nil {
		d.Logger = slog.New(slog.DiscardHandler)
	}
	s := &server{Deps: d}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", s.ready)

	mux.Handle("POST /v1/accounts", s.idempotent("open_account", s.openAccount))
	mux.Handle("GET /v1/accounts/{id}", s.handle(s.getAccount))
	mux.Handle("POST /v1/accounts/{id}/close", s.idempotent("close_account", s.closeAccount))
	mux.Handle("GET /v1/accounts/{id}/balance", s.handle(s.getBalance))
	mux.Handle("GET /v1/accounts/{id}/statement", s.handle(s.getStatement))

	mux.Handle("POST /v1/entries", s.idempotent("post_entry", s.postEntry))
	mux.Handle("GET /v1/entries/{id}", s.handle(s.getEntry))
	mux.Handle("POST /v1/entries/{id}/reverse", s.idempotent("reverse_entry", s.reverseEntry))
	mux.Handle("POST /v1/transfers", s.idempotent("transfer", s.transfer))

	mux.Handle("POST /v1/holds", s.idempotent("place_hold", s.placeHold))
	mux.Handle("GET /v1/holds/{id}", s.handle(s.getHold))
	mux.Handle("POST /v1/holds/{id}/capture", s.idempotent("capture_hold", s.captureHold))
	mux.Handle("POST /v1/holds/{id}/void", s.idempotent("void_hold", s.voidHold))

	return chain(mux, s.recoverer, s.logRequests, requestID)
}

// result is what a handler produces on success.
type result struct {
	status int
	body   any
}

type handlerFunc func(r *http.Request) (result, error)

// request is the parsed input of a mutating call.
type request struct {
	*http.Request
	body []byte
	// id is derived from the idempotency key and used as the ID of whatever
	// the call creates.
	id uuid.UUID
}

type mutationFunc func(req request) (result, error)

func (s *server) handle(fn handlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		res, err := fn(r)
		if err != nil {
			s.writeError(w, r, err)
			return
		}
		writeJSON(w, res.status, res.body)
	})
}

// idempotent requires an Idempotency-Key, replays stored responses and
// records new ones. Client errors are stored like successes so that a retry
// gets the same answer; server errors and contention release the key.
func (s *server) idempotent(scope string, fn mutationFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		if key == "" || len(key) > maxKeyLength {
			s.writeError(w, r, newError(http.StatusBadRequest, codeKeyRequired, fmt.Sprintf("Idempotency-Key header (1-%d characters) is required", maxKeyLength)))
			return
		}
		body, err := readBody(w, r)
		if err != nil {
			s.writeError(w, r, err)
			return
		}
		ctx := r.Context()
		rec, err := s.Idempotency.Begin(ctx, scope, key, idempotency.Hash(r.Method, r.URL.Path, body))
		if err != nil {
			s.writeError(w, r, err)
			return
		}
		if rec != nil {
			w.Header().Set("Idempotent-Replayed", "true")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(rec.StatusCode)
			_, _ = w.Write(rec.Body)
			return
		}

		res, runErr := fn(request{Request: r, body: body, id: idempotency.DeriveID(scope, key)})
		status, payload := res.status, res.body
		if runErr != nil {
			var detail ErrorDetail
			status, detail = classify(runErr)
			detail.RequestID = requestIDFrom(ctx)
			payload = ErrorBody{Error: detail}
			if status >= 500 || detail.Code == "conflict" {
				if err := s.Idempotency.Release(context.WithoutCancel(ctx), scope, key); err != nil {
					s.Logger.ErrorContext(ctx, "release idempotency key", "err", err)
				}
				s.logError(r, status, runErr)
				writeJSON(w, status, payload)
				return
			}
		}
		buf, err := json.Marshal(payload)
		if err != nil {
			s.writeError(w, r, err)
			return
		}
		buf = append(buf, '\n')
		if err := s.Idempotency.Complete(context.WithoutCancel(ctx), scope, key, idempotency.Record{StatusCode: status, Body: buf}); err != nil {
			s.Logger.ErrorContext(ctx, "store idempotent response", "err", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(buf)
	})
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	if r.ContentLength != 0 {
		if ct := r.Header.Get("Content-Type"); ct != "" {
			mt, _, err := mime.ParseMediaType(ct)
			if err != nil || mt != "application/json" {
				return nil, newError(http.StatusUnsupportedMediaType, codeUnsupportedMedia, "Content-Type must be application/json")
			}
		}
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			return nil, newError(http.StatusRequestEntityTooLarge, codeRequestTooLarge, "request body too large")
		}
		return nil, newError(http.StatusBadRequest, codeMalformed, "could not read request body")
	}
	return body, nil
}

// decode parses a JSON body strictly. An empty body decodes to the zero value.
func decode(body []byte, v any) error {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return newError(http.StatusBadRequest, codeMalformed, "invalid JSON body: "+err.Error())
	}
	if dec.More() {
		return newError(http.StatusBadRequest, codeMalformed, "body must contain a single JSON object")
	}
	return nil
}

func pathID(r *http.Request) (uuid.UUID, error) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		return uuid.Nil, newError(http.StatusBadRequest, codeMalformed, "id must be a UUID")
	}
	return id, nil
}

func queryInt(r *http.Request, name string, def, lo, hi int64) (int64, error) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return def, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < lo || v > hi {
		return 0, newError(http.StatusBadRequest, codeMalformed, fmt.Sprintf("%s must be an integer between %d and %d", name, lo, hi))
	}
	return v, nil
}

func (s *server) ready(w http.ResponseWriter, r *http.Request) {
	if s.Ready != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := s.Ready(ctx); err != nil {
			s.Logger.WarnContext(ctx, "readiness check failed", "err", err)
			s.writeError(w, r, newError(http.StatusServiceUnavailable, codeServiceUnavailable, "dependencies unavailable"))
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	status, detail := classify(err)
	detail.RequestID = requestIDFrom(r.Context())
	s.logError(r, status, err)
	writeJSON(w, status, ErrorBody{Error: detail})
}

func (s *server) logError(r *http.Request, status int, err error) {
	if status >= 500 {
		s.Logger.ErrorContext(r.Context(), "request failed", "method", r.Method, "path", r.URL.Path, "status", status, "err", err, "request_id", requestIDFrom(r.Context()))
	}
}

type ctxKey struct{}

func requestIDFrom(ctx context.Context) string {
	v, _ := ctx.Value(ctxKey{}).(string)
	return v
}

func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" || len(id) > 128 {
			id = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, id)))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.Logger.InfoContext(r.Context(), "http request",
			"method", r.Method, "path", r.URL.Path, "status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(), "request_id", requestIDFrom(r.Context()))
	})
}

func (s *server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler {
					panic(v)
				}
				s.Logger.ErrorContext(r.Context(), "panic", "value", v, "stack", string(debug.Stack()))
				s.writeError(w, r, fmt.Errorf("panic: %v", v))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// chain applies middleware so that the last one listed runs first.
func chain(h http.Handler, mw ...func(http.Handler) http.Handler) http.Handler {
	for _, m := range mw {
		h = m(h)
	}
	return h
}
