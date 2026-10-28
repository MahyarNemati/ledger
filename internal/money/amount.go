package money

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ErrOverflow is returned when integer arithmetic would overflow int64.
var ErrOverflow = errors.New("amount overflow")

// Add returns a+b or ErrOverflow.
func Add(a, b int64) (int64, error) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, ErrOverflow
	}
	return a + b, nil
}

// Sub returns a-b or ErrOverflow.
func Sub(a, b int64) (int64, error) {
	if b == math.MinInt64 {
		return 0, ErrOverflow
	}
	return Add(a, -b)
}

// Format renders minor units as a decimal string in the currency's precision.
func Format(minor int64, c Currency) string {
	if c.Exponent == 0 {
		return strconv.FormatInt(minor, 10)
	}
	neg := minor < 0
	u := uint64(minor)
	if neg {
		u = uint64(-(minor + 1)) + 1
	}
	s := strconv.FormatUint(u, 10)
	if len(s) <= c.Exponent {
		s = strings.Repeat("0", c.Exponent-len(s)+1) + s
	}
	cut := len(s) - c.Exponent
	out := s[:cut] + "." + s[cut:]
	if neg {
		out = "-" + out
	}
	return out
}

// Parse converts a decimal string such as "12.30" into minor units. It rejects
// more fractional digits than the currency allows rather than rounding.
func Parse(s string, c Currency) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty amount")
	}
	neg := false
	switch s[0] {
	case '-':
		neg = true
		s = s[1:]
	case '+':
		s = s[1:]
	}
	whole, frac, hasDot := strings.Cut(s, ".")
	if whole == "" || (hasDot && frac == "") {
		return 0, fmt.Errorf("invalid amount %q", s)
	}
	if len(frac) > c.Exponent {
		return 0, fmt.Errorf("amount %q has more than %d decimal places for %s", s, c.Exponent, c.Code)
	}
	frac += strings.Repeat("0", c.Exponent-len(frac))
	for _, r := range whole + frac {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("invalid amount %q", s)
		}
	}
	digits := whole + frac
	if neg {
		digits = "-" + digits
	}
	v, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return 0, ErrOverflow
	}
	return v, nil
}
