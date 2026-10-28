package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/MahyarNemati/ledger/internal/domain"
	"github.com/MahyarNemati/ledger/internal/idempotency"
)

// API-only error codes. Domain codes are passed through unchanged.
const (
	codeMalformed          = "malformed_request"
	codeKeyRequired        = "idempotency_key_required"
	codeKeyReused          = "idempotency_key_reused"
	codeRequestInProgress  = "request_in_progress"
	codeUnsupportedMedia   = "unsupported_media_type"
	codeRequestTooLarge    = "request_too_large"
	codeServiceUnavailable = "service_unavailable"
	codeInternal           = string(domain.CodeInternal)
)

// ErrorBody is the envelope for every non-2xx response.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

type ErrorDetail struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id,omitempty"`
}

type apiError struct {
	status  int
	code    string
	message string
}

func (e *apiError) Error() string { return e.code + ": " + e.message }

func newError(status int, code, msg string) error {
	return &apiError{status: status, code: code, message: msg}
}

var domainStatus = map[domain.Code]int{
	domain.CodeValidation:        http.StatusBadRequest,
	domain.CodeNotFound:          http.StatusNotFound,
	domain.CodeAlreadyExists:     http.StatusConflict,
	domain.CodeConflict:          http.StatusConflict,
	domain.CodeAlreadyReversed:   http.StatusConflict,
	domain.CodeHoldNotPending:    http.StatusConflict,
	domain.CodeAccountClosed:     http.StatusConflict,
	domain.CodeAccountNotEmpty:   http.StatusConflict,
	domain.CodeInsufficientFunds: http.StatusUnprocessableEntity,
	domain.CodeUnbalanced:        http.StatusUnprocessableEntity,
	domain.CodeCurrencyMismatch:  http.StatusUnprocessableEntity,
	domain.CodeHoldExpired:       http.StatusUnprocessableEntity,
	domain.CodeNotReversible:     http.StatusUnprocessableEntity,
}

// classify maps any error to an HTTP status and a public error detail.
// Internal errors never leak their message.
func classify(err error) (int, ErrorDetail) {
	var ae *apiError
	if errors.As(err, &ae) {
		return ae.status, ErrorDetail{Code: ae.code, Message: ae.message}
	}
	switch {
	case errors.Is(err, idempotency.ErrMismatch):
		return http.StatusUnprocessableEntity, ErrorDetail{Code: codeKeyReused, Message: err.Error()}
	case errors.Is(err, idempotency.ErrInProgress):
		return http.StatusConflict, ErrorDetail{Code: codeRequestInProgress, Message: err.Error()}
	}
	var de *domain.Error
	if errors.As(err, &de) {
		if status, ok := domainStatus[de.Code]; ok {
			return status, ErrorDetail{Code: string(de.Code), Message: de.Message}
		}
	}
	return http.StatusInternalServerError, ErrorDetail{Code: codeInternal, Message: "internal error"}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
