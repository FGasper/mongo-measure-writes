package main

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func mustExtract[T any](raw bson.Raw, pointer ...string) T {
	val, err := raw.LookupErr(pointer...)
	if err != nil {
		panic(fmt.Sprintf("extracting %#q from %v: %v", pointer, raw, err))
	}

	var ret T
	if err := val.Unmarshal(&ret); err != nil {
		panic(fmt.Sprintf("unmarshaling %v as %T: %v", val, ret, err))
	}

	return ret
}
