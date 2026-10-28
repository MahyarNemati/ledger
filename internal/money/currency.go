// Package money defines currencies and integer minor-unit amounts.
package money

import (
	"fmt"
	"sort"
	"strings"
)

// Currency is an ISO 4217 style currency with a fixed number of minor-unit digits.
type Currency struct {
	Code     string
	Exponent int
}

func (c Currency) String() string { return c.Code }

var registry = map[string]Currency{
	"USD": {Code: "USD", Exponent: 2},
	"EUR": {Code: "EUR", Exponent: 2},
	"GBP": {Code: "GBP", Exponent: 2},
	"CAD": {Code: "CAD", Exponent: 2},
	"CHF": {Code: "CHF", Exponent: 2},
	"JPY": {Code: "JPY", Exponent: 0},
	"KWD": {Code: "KWD", Exponent: 3},
}

// LookupCurrency returns the registered currency for code (case-insensitive).
func LookupCurrency(code string) (Currency, error) {
	c, ok := registry[strings.ToUpper(strings.TrimSpace(code))]
	if !ok {
		return Currency{}, fmt.Errorf("unsupported currency %q", code)
	}
	return c, nil
}

// Currencies lists the supported currency codes in sorted order.
func Currencies() []string {
	out := make([]string, 0, len(registry))
	for code := range registry {
		out = append(out, code)
	}
	sort.Strings(out)
	return out
}
