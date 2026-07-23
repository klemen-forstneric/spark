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

// JSONTypeCodec is the default TypeCodec: encodes the value as JSON, tagging
// each payload with the value's pointer-ness so one codec round-trips both a
// type and a pointer to it.
type JSONTypeCodec struct {
	rt reflect.Type
}

// NewJSONTypeCodec builds a JSONTypeCodec for sample's concrete type.
func NewJSONTypeCodec(sample any) JSONTypeCodec {
	rt := reflect.TypeOf(sample)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	return JSONTypeCodec{rt: rt}
}

func (c JSONTypeCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(struct {
		Pointer bool `json:"pointer,omitempty"`
		Value   any  `json:"value"`
	}{Pointer: reflect.TypeOf(v).Kind() == reflect.Ptr, Value: v})
}

func (c JSONTypeCodec) Unmarshal(b []byte) (any, error) {
	var p struct {
		Pointer bool            `json:"pointer,omitempty"`
		Value   json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, err
	}
	ptr := reflect.New(c.rt)
	if err := json.Unmarshal(p.Value, ptr.Interface()); err != nil {
		return nil, err
	}
	if p.Pointer {
		return ptr.Interface(), nil
	}
	return ptr.Elem().Interface(), nil
}
