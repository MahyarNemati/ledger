package money

import (
	"errors"
	"math"
	"testing"

	"pgregory.net/rapid"
)

func TestFormatAndParse(t *testing.T) {
	usd, _ := LookupCurrency("usd")
	jpy, _ := LookupCurrency("JPY")
	kwd, _ := LookupCurrency("KWD")

	cases := []struct {
		minor int64
		cur   Currency
		want  string
	}{
		{0, usd, "0.00"},
		{5, usd, "0.05"},
		{-5, usd, "-0.05"},
		{123456, usd, "1234.56"},
		{42, jpy, "42"},
		{1, kwd, "0.001"},
		{math.MinInt64, usd, "-92233720368547758.08"},
	}
	for _, tc := range cases {
		if got := Format(tc.minor, tc.cur); got != tc.want {
			t.Errorf("Format(%d, %s) = %q, want %q", tc.minor, tc.cur, got, tc.want)
		}
		back, err := Parse(tc.want, tc.cur)
		if err != nil || back != tc.minor {
			t.Errorf("Parse(%q) = %d, %v; want %d", tc.want, back, err, tc.minor)
		}
	}
}

func TestParseRejects(t *testing.T) {
	usd, _ := LookupCurrency("USD")
	for _, in := range []string{"", "1.234", "abc", "1.", ".5", "1e3", "99999999999999999999"} {
		if _, err := Parse(in, usd); err == nil {
			t.Errorf("Parse(%q) expected error", in)
		}
	}
	if _, err := LookupCurrency("XXX"); err == nil {
		t.Error("expected unsupported currency error")
	}
}

func TestAddOverflow(t *testing.T) {
	if _, err := Add(math.MaxInt64, 1); !errors.Is(err, ErrOverflow) {
		t.Fatalf("expected overflow, got %v", err)
	}
	if _, err := Sub(math.MinInt64, 1); !errors.Is(err, ErrOverflow) {
		t.Fatalf("expected overflow, got %v", err)
	}
	if _, err := Sub(0, math.MinInt64); !errors.Is(err, ErrOverflow) {
		t.Fatalf("expected overflow, got %v", err)
	}
}

func TestFormatParseRoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		code := rapid.SampledFrom(Currencies()).Draw(t, "currency")
		cur, _ := LookupCurrency(code)
		v := rapid.Int64().Draw(t, "minor")
		got, err := Parse(Format(v, cur), cur)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if got != v {
			t.Fatalf("round trip %d -> %q -> %d", v, Format(v, cur), got)
		}
	})
}
