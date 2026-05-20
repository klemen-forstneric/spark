package middleware

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

// ErrUnknownType is returned by Codec.Unmarshal when the encoded value
// names a type that wasn't registered. Wrap-friendly via errors.Is.
var ErrUnknownType = errors.New("spark/middleware: unknown type")

// Codec serializes and deserializes arbitrary values, preserving the
// concrete Go type across the round-trip. Used by Idempotent (via
// IdempotencyConfig.Codec) so Stores that cross a serialization boundary
// can round-trip the original result type.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte) (any, error)
}

// Registry maps stable type identifiers to their reflect.Type for
// round-trip reconstruction. Build one with NewRegistry, listing every
// concrete result type that may flow through the codec.
type Registry struct {
	types map[string]reflect.Type
}

// NewRegistry constructs a Registry containing the types of the given
// samples. Pointer/value distinction is captured at encode time and
// honored at decode time, so passing &User{} registers User; both User
// and *User can be round-tripped later.
func NewRegistry(samples ...any) *Registry {
	r := &Registry{types: map[string]reflect.Type{}}
	for _, s := range samples {
		if s == nil {
			continue
		}
		rt := reflect.TypeOf(s)
		if rt.Kind() == reflect.Ptr {
			rt = rt.Elem()
		}
		r.types[rt.String()] = rt
	}
	return r
}

// JSONCodec encodes values as JSON wrapped in a type envelope.
type JSONCodec struct {
	registry *Registry
}

// NewJSONCodec returns a JSONCodec backed by the given Registry.
func NewJSONCodec(r *Registry) *JSONCodec {
	return &JSONCodec{registry: r}
}

type codecEnvelope struct {
	Type    string          `json:"type"`
	Pointer bool            `json:"pointer,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Marshal encodes v into a JSON envelope tagged with its Go type. A nil
// value encodes as an empty envelope and round-trips back to nil.
func (c *JSONCodec) Marshal(v any) ([]byte, error) {
	if v == nil {
		return json.Marshal(codecEnvelope{})
	}
	rt := reflect.TypeOf(v)
	pointer := false
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
		pointer = true
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("spark/middleware: codec marshal value: %w", err)
	}
	return json.Marshal(codecEnvelope{
		Type:    rt.String(),
		Pointer: pointer,
		Data:    data,
	})
}

// Unmarshal decodes an envelope produced by Marshal back into the original
// Go value (honouring the pointer-vs-value distinction). Returns
// ErrUnknownType if the encoded type is not present in the Registry.
func (c *JSONCodec) Unmarshal(data []byte) (any, error) {
	var e codecEnvelope
	if err := json.Unmarshal(data, &e); err != nil {
		return nil, fmt.Errorf("spark/middleware: codec unmarshal envelope: %w", err)
	}
	if e.Type == "" {
		return nil, nil
	}
	rt, ok := c.registry.types[e.Type]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownType, e.Type)
	}
	ptr := reflect.New(rt)
	if err := json.Unmarshal(e.Data, ptr.Interface()); err != nil {
		return nil, fmt.Errorf("spark/middleware: codec unmarshal value: %w", err)
	}
	if e.Pointer {
		return ptr.Interface(), nil
	}
	return ptr.Elem().Interface(), nil
}
