package money

import (
	"errors"
	"math"
	"testing"
)

func TestParse(t *testing.T) {
	cases := []struct {
		input   string
		want    int64
		wantErr error
	}{
		{"101.00", 10100, nil},
		{"101.5", 10150, nil},
		{"101", 10100, nil},
		{"0.01", 1, nil},
		{"0", 0, nil},
		{"007.10", 710, nil},
		{"  5.25 ", 525, nil},
		{"92233720368547758.07", math.MaxInt64, nil},
		{"92233720368547758.08", 0, ErrOverflow},
		{"99999999999999999999", 0, ErrOverflow},
		{"101.505", 0, ErrBadAmount},
		{"-1", 0, ErrBadAmount},
		{"+1", 0, ErrBadAmount},
		{"1e3", 0, ErrBadAmount},
		{"", 0, ErrBadAmount},
		{"   ", 0, ErrBadAmount},
		{"1.", 0, ErrBadAmount},
		{".5", 0, ErrBadAmount},
		{"1.2.3", 0, ErrBadAmount},
		{"1 000", 0, ErrBadAmount},
		{"1,50", 0, ErrBadAmount},
		{"١٢", 0, ErrBadAmount},
	}
	for _, testCase := range cases {
		got, err := Parse(testCase.input, 2)
		if !errors.Is(err, testCase.wantErr) || got != testCase.want {
			t.Errorf("Parse(%q) = %d, %v; want %d, %v", testCase.input, got, err, testCase.want, testCase.wantErr)
		}
	}
}

func TestParseScale(t *testing.T) {
	if got, err := Parse("12", 0); err != nil || got != 12 {
		t.Fatalf("scale 0: got %d, %v", got, err)
	}
	if _, err := Parse("1.5", 0); !errors.Is(err, ErrBadAmount) {
		t.Fatalf("scale 0 must reject fraction, got %v", err)
	}
	if _, err := Parse("1", -1); !errors.Is(err, ErrBadScale) {
		t.Fatalf("negative scale: got %v", err)
	}
	if _, err := Parse("1", 19); !errors.Is(err, ErrBadScale) {
		t.Fatalf("too large scale: got %v", err)
	}
}

func TestFormat(t *testing.T) {
	cases := []struct {
		value int64
		scale int
		want  string
	}{
		{10150, 2, "101.50"},
		{1, 2, "0.01"},
		{0, 2, "0.00"},
		{-250, 2, "-2.50"},
		{-1, 2, "-0.01"},
		{42, 0, "42"},
		{math.MaxInt64, 2, "92233720368547758.07"},
		{math.MinInt64, 2, "-92233720368547758.08"},
	}
	for _, testCase := range cases {
		if got := Format(testCase.value, testCase.scale); got != testCase.want {
			t.Errorf("Format(%d, %d) = %q, want %q", testCase.value, testCase.scale, got, testCase.want)
		}
	}
}

func TestFormatParseRoundTrip(t *testing.T) {
	for _, value := range []int64{0, 1, 9, 10, 99, 100, 12345, math.MaxInt64} {
		text := Format(value, PriceScale)
		parsed, err := Parse(text, PriceScale)
		if err != nil || parsed != value {
			t.Errorf("round trip %d -> %q -> %d, %v", value, text, parsed, err)
		}
	}
}

func TestMul(t *testing.T) {
	cases := []struct {
		a, b    int64
		want    int64
		wantErr error
	}{
		{10100, 3, 30300, nil},
		{0, math.MaxInt64, 0, nil},
		{math.MaxInt64, 0, 0, nil},
		{math.MaxInt64, 1, math.MaxInt64, nil},
		{math.MaxInt64 / 2, 2, math.MaxInt64 - 1, nil},
		{math.MaxInt64/2 + 1, 2, 0, ErrOverflow},
		{math.MaxInt64 / 2, 3, 0, ErrOverflow},
		{-1, 5, 0, ErrBadAmount},
		{5, -1, 0, ErrBadAmount},
	}
	for _, testCase := range cases {
		got, err := Mul(testCase.a, testCase.b)
		if !errors.Is(err, testCase.wantErr) || got != testCase.want {
			t.Errorf("Mul(%d, %d) = %d, %v; want %d, %v", testCase.a, testCase.b, got, err, testCase.want, testCase.wantErr)
		}
	}
}

func TestSplitSymbol(t *testing.T) {
	base, quote, err := SplitSymbol("ACME-USD")
	if err != nil || base != "ACME" || quote != "USD" {
		t.Fatalf("got %s %s %v", base, quote, err)
	}
	for _, bad := range []string{"ACME", "-USD", "ACME-", "A-B-C", ""} {
		if _, _, err := SplitSymbol(bad); !errors.Is(err, ErrBadSymbol) {
			t.Errorf("SplitSymbol(%q) error = %v, want ErrBadSymbol", bad, err)
		}
	}
}
