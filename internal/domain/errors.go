package domain

import (
	"errors"
	"fmt"
)

// Code is a stable, machine-readable error identifier exposed by the API.
type Code string

const (
	CodeValidation        Code = "validation_failed"
	CodeNotFound          Code = "not_found"
	CodeAlreadyExists     Code = "already_exists"
	CodeInsufficientFunds Code = "insufficient_funds"
	CodeAccountClosed     Code = "account_closed"
	CodeAccountNotEmpty   Code = "account_not_empty"
	CodeCurrencyMismatch  Code = "currency_mismatch"
	CodeUnbalanced        Code = "unbalanced_entry"
	CodeAlreadyReversed   Code = "already_reversed"
	CodeNotReversible     Code = "not_reversible"
	CodeHoldNotPending    Code = "hold_not_pending"
	CodeHoldExpired       Code = "hold_expired"
	CodeConflict          Code = "conflict"
	CodeInternal          Code = "internal"
)

// Error is a business rule violation.
type Error struct {
	Code    Code
	Message string
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Message }

// Errorf builds a domain error.
func Errorf(code Code, format string, args ...any) error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// CodeOf extracts the domain code from err, or CodeInternal.
func CodeOf(err error) Code {
	var de *Error
	if errors.As(err, &de) {
		return de.Code
	}
	return CodeInternal
}

// IsCode reports whether err carries the given domain code.
func IsCode(err error, code Code) bool { return err != nil && CodeOf(err) == code }
