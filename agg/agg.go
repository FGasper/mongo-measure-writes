package agg

import "go.mongodb.org/mongo-driver/v2/bson"

func Eq(comparands ...any) bson.D {
	return bson.D{{"$eq", comparands}}
}

func In(needle any, haystack ...any) bson.D {
	return bson.D{{"$in", bson.A{needle, haystack}}}
}

func BSONSize(ref any) bson.D {
	return bson.D{{"$bsonSize", ref}}
}

func Type(ref any) bson.D {
	return bson.D{{"$type", ref}}
}

func Concat(refs ...any) bson.D {
	return bson.D{{"$concat", refs}}
}

// ---------------------------------------------

type Cond struct {
	If, Then, Else any
}

var _ bson.Marshaler = Cond{}

func (c Cond) D() bson.D {
	return bson.D{
		{"$cond", bson.D{
			{"if", c.If},
			{"then", c.Then},
			{"else", c.Else},
		}},
	}
}

func (c Cond) MarshalBSON() ([]byte, error) {
	return bson.Marshal(c.D())
}

// ---------------------------------------------

type Map struct {
	Input, As, In any
}

var _ bson.Marshaler = Map{}

func (m Map) D() bson.D {
	return bson.D{
		{"$map", bson.D{
			{"input", m.Input},
			{"as", m.As},
			{"in", m.In},
		}},
	}
}

func (m Map) MarshalBSON() ([]byte, error) {
	return bson.Marshal(m.D())
}
