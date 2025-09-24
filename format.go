package main

// This package exposes a number of tools that facilitate consistent
// formatting in log reports.

import (
	"fmt"
	"math"

	mmmath "github.com/FGasper/mongo-measure-writes/math"
	"github.com/dustin/go-humanize"
	"golang.org/x/exp/constraints"
)

const decimalPrecision = 2

// num16Plus is like realNum, but it excludes 8-bit int/uint.
type num16Plus interface {
	constraints.Float |
		~uint | ~uint16 | ~uint32 | ~uint64 |
		~int | ~int16 | ~int32 | ~int64
}

// DataUnit signifies some unit of data.
type DataUnit string

const (
	Bytes DataUnit = "bytes"
	KiB   DataUnit = "KiB"
	MiB   DataUnit = "MiB"
	GiB   DataUnit = "GiB"
	TiB   DataUnit = "TiB"
	PiB   DataUnit = "PiB"

	// Anything larger than the above seems like overkill.
)

var unitSize = map[DataUnit]uint64{
	KiB: humanize.KiByte,
	MiB: humanize.MiByte,
	GiB: humanize.GiByte,
	TiB: humanize.TiByte,
	PiB: humanize.PiByte,
}

// BytesToUnit returns a stringified number that represents `count`
// in the given `unit`. For example, count=1024 and unit=KiB would
// return "1".
func BytesToUnit[T num16Plus](count T, unit DataUnit) string {

	// Ideally go-humanize could do this for us,
	// but as of this writing it can’t.
	// https://github.com/dustin/go-humanize/issues/111

	// We could put Bytes into the unitSize map above and handle it
	// the same as other units, but that would entail int/float
	// conversion and possibly risk little rounding errors. We might
	// as well keep it simple where we can.

	if unit == Bytes {
		return FmtReal(count)
	}

	myUnitSize, exists := unitSize[unit]

	if !exists {
		panic(fmt.Sprintf("Missing unit in unitSize: %s", unit))
	}

	return FmtReal(mmmath.DivideToF64(count, myUnitSize))
}

// FmtReal provides a standard formatting of real numbers, with a consistent
// precision and trailing decimal zeros removed.
func FmtReal[T mmmath.RealNumber](num T) string {
	switch any(num).(type) {
	case float32, float64:
		return fmtFloat(num)
	case uint64, uint:
		// Uints that can’t be int64 need to be formatted as floats.
		if uint64(num) > math.MaxInt64 {
			return fmtFloat(num)
		}

		// Any other uint* type can be an int, which we format below.
	default:
		// Formatted below.
	}

	return humanize.Comma(int64(num))
}

func fmtFloat[T mmmath.RealNumber](num T) string {
	return humanize.Commaf(roundFloat(float64(num), decimalPrecision))
}

func roundFloat(val float64, precision uint) float64 {
	ratio := math.Pow10(int(precision))
	return math.Round(val*ratio) / ratio
}

// FindBestUnit gives the “best” DataUnit for the given `count` of bytes.
//
// You can then give that DataUnit to BytesToUnit() to stringify
// multiple byte counts to the same unit.
func FindBestUnit[T num16Plus](count T) DataUnit {

	// humanize.IBytes() does most of what we want but lacks the
	// flexibility to specify a precision. It’s not complicated to
	// implement here anyway.

	if count < T(humanize.KiByte) {
		return Bytes
	}

	// If the log2 is, e.g., 32.05, we want to use 2^30, i.e., GiB.
	log2 := math.Log2(float64(count))

	// Convert log2 to the next-lowest multiple of 10.
	unitNum := 10 * uint64(math.Floor(log2/10))

	// Now find that power of 2, which we can compare against
	// the values of the unitSize map (above).
	unitNum = 1 << unitNum

	// Just in case, someday, exibytes become relevant …
	var biggestSize uint64
	var biggestUnit DataUnit

	for unit, size := range unitSize {
		if size == unitNum {
			return unit
		}

		if size > biggestSize {
			biggestSize = size
			biggestUnit = unit
		}
	}

	return biggestUnit
}

// FmtBytes is a convenience that combines BytesToUnit with FindBestUnit.
// Use it to format a single count of bytes.
func FmtBytes[T num16Plus](count T) string {
	unit := FindBestUnit(count)
	return BytesToUnit(count, unit) + " " + string(unit)
}
