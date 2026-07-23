package middleware

import (
	"encoding/json"
	"reflect"
)

// TypeCodec serializes one concrete result type. Register it on a Registry to
// control how that type crosses the idempotency store.
type TypeCodec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(b []byte) (any, error)
}

// JSONTypeCodec is the default TypeCodec: encodes the value as JSON, and
// reconstructs its concrete value-or-pointer type via reflection on decode.
type JSONTypeCodec struct {
	rt      reflect.Type
	pointer bool
}

// NewJSONTypeCodec builds a JSONTypeCodec for sample's type, reconstructing the
// same value-or-pointer form as sample on decode.
func NewJSONTypeCodec(sample any) JSONTypeCodec {
	rt := reflect.TypeOf(sample)
	pointer := false
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
		pointer = true
	}
	return JSONTypeCodec{rt: rt, pointer: pointer}
}

func (c JSONTypeCodec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

func (c JSONTypeCodec) Unmarshal(b []byte) (any, error) {
	ptr := reflect.New(c.rt)
	if err := json.Unmarshal(b, ptr.Interface()); err != nil {
		return nil, err
	}
	if c.pointer {
		return ptr.Interface(), nil
	}
	return ptr.Elem().Interface(), nil
}
