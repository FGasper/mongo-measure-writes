package resumetoken

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"

	"go.mongodb.org/mongo-driver/v2/bson"
)

type ResumeToken bson.Raw

var zeroTS = bson.Timestamp{}

// New takes a `bson.Raw` and returns a `ResumeToken`.
func New(raw bson.Raw) ResumeToken {
	return ResumeToken(raw)
}

// Raw returns the resume token contents as a `bson.Raw`.
func (rt ResumeToken) Raw() bson.Raw {
	return bson.Raw(rt)
}

// Timestamp returns the clusterTime element from an encoded ResumeToken.
func (rt ResumeToken) Timestamp() (bson.Timestamp, error) {
	if rt == nil {
		return zeroTS, fmt.Errorf("resume token is nil")
	}

	raw := bson.Raw(rt)

	dataVal, lookupErr := raw.LookupErr("_data")
	if lookupErr != nil {
		return zeroTS, fmt.Errorf("seeking _data in resume token (%v): %w", raw, lookupErr)
	}

	dataString, ok := dataVal.StringValueOK()
	if !ok {
		return zeroTS, fmt.Errorf("resume token _data is BSON %s, not %s", dataVal.Type, bson.TypeString)
	}

	clusterTime, err := getTSFromStringResumeToken(dataString)
	if err != nil {
		return zeroTS, err
	}

	return clusterTime, nil
}

const (
	// Type identifier for a resume token’s timestamp
	// (cf. https://github.com/mongodb-js/mongodb-resumetoken-decoder/blob/2d64962d194a5b99bb28ad1da6e7f1e26f6db0b7/src/keystringdecoder.ts#L20)
	rtTimeStampType uint8 = 130
)

// Get clusterTime element from a string ResumeToken,
// clusterTime is the first element in the encoded token structure.
func getTSFromStringResumeToken(dataString string) (bson.Timestamp, error) {
	keyStringBinData, decodeErr := hex.DecodeString(dataString)
	if decodeErr != nil {
		return zeroTS, fmt.Errorf("decoding hex dataString")
	}

	reader := bytes.NewReader(keyStringBinData)
	typeIdent, err := readType(reader)
	if err != nil {
		return zeroTS, err
	}
	if typeIdent != rtTimeStampType {
		return zeroTS, fmt.Errorf(
			"decoding: typeIdent=%v (expected %v)",
			typeIdent,
			rtTimeStampType,
		)
	}

	var data [8]byte
	_, readErr := reader.Read(data[:])
	if readErr != nil {
		return zeroTS, fmt.Errorf("reading timestamp data: %w", readErr)
	}

	t := binary.BigEndian.Uint32(data[:])
	i := binary.BigEndian.Uint32(data[4:])

	return bson.Timestamp{T: t, I: i}, nil
}

func readType(reader io.ByteReader) (uint8, error) {
	var t uint8
	t, err := reader.ReadByte()

	if err != nil {
		return 0, fmt.Errorf("reading type byte: %w", err)
	}

	return t, nil
}
