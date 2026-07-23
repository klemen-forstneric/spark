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
	ptr := reflect.TypeOf(v).Kind() == reflect.Ptr

	payload := struct {
		Pointer bool `json:"p,omitempty"`
		Value   any  `json:"v"`
	}{
		Pointer: ptr,
		Value:   v,
	}

	return json.Marshal(payload)
}

func (c JSONTypeCodec) Unmarshal(b []byte) (any, error) {
	var payload struct {
		Pointer bool            `json:"p"`
		Value   json.RawMessage `json:"v"`
	}

	if err := json.Unmarshal(b, &payload); err != nil {
		return nil, err
	}

	ptr := reflect.New(c.rt)
	if err := json.Unmarshal(payload.Value, ptr.Interface()); err != nil {
		return nil, err
	}
	if payload.Pointer {
		return ptr.Interface(), nil
	}
	return ptr.Elem().Interface(), nil
}
