package money

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const PriceScale = 2

const maxScale = 18

var (
	ErrBadAmount = errors.New("некорректная сумма")
	ErrOverflow  = errors.New("сумма не помещается в int64")
	ErrBadScale  = errors.New("недопустимое число знаков после точки")
	ErrBadSymbol = errors.New("некорректное имя торговой пары")
)

func Parse(text string, scale int) (int64, error) {
	if scale < 0 || scale > maxScale {
		return 0, ErrBadScale
	}

	text = strings.TrimSpace(text)
	if text == "" {
		return 0, ErrBadAmount
	}

	integerPart, fractionPart, hasDot := strings.Cut(text, ".")
	if integerPart == "" {
		return 0, ErrBadAmount
	}
	if hasDot && fractionPart == "" {
		return 0, ErrBadAmount
	}
	if len(fractionPart) > scale {
		return 0, ErrBadAmount
	}
	if !onlyDigits(integerPart) || !onlyDigits(fractionPart) {
		return 0, ErrBadAmount
	}

	missingZeros := strings.Repeat("0", scale-len(fractionPart))
	digits := integerPart + fractionPart + missingZeros

	value, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return 0, ErrOverflow
	}
	return value, nil
}

func onlyDigits(text string) bool {
	for _, char := range text {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func Format(value int64, scale int) string {
	if scale < 0 {
		scale = 0
	}

	negative := value < 0
	var absolute uint64
	if negative {
		absolute = uint64(-(value + 1)) + 1
	} else {
		absolute = uint64(value)
	}

	digits := strconv.FormatUint(absolute, 10)
	if scale > 0 {
		if len(digits) <= scale {
			digits = strings.Repeat("0", scale-len(digits)+1) + digits
		}
		split := len(digits) - scale
		digits = digits[:split] + "." + digits[split:]
	}

	if negative {
		return "-" + digits
	}
	return digits
}

func Mul(a, b int64) (int64, error) {
	if a < 0 || b < 0 {
		return 0, ErrBadAmount
	}
	if a != 0 && b > math.MaxInt64/a {
		return 0, ErrOverflow
	}
	return a * b, nil
}

func SplitSymbol(symbol string) (base, quote string, err error) {
	base, quote, found := strings.Cut(symbol, "-")
	if !found || base == "" || quote == "" || strings.Contains(quote, "-") {
		return "", "", fmt.Errorf("%w: %q, ожидается формат BASE-QUOTE", ErrBadSymbol, symbol)
	}
	return base, quote, nil
}
