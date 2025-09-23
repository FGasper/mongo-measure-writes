package math

import "golang.org/x/exp/constraints"

// RealNumber represents any real (i.e., non-complex) number type.
type RealNumber interface {
	constraints.Integer | constraints.Float
}

// DivideToF64 is syntactic sugar around float64(numerator) / float64(denominator).
func DivideToF64[N RealNumber, D RealNumber](numerator N, denominator D) float64 {
	return float64(numerator) / float64(denominator)
}
