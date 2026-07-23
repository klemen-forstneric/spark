package middleware

import (
	"encoding/json"
	"errors"
	"fmt"
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

// JSONCodec encodes values as JSON wrapped in a type envelope.
type JSONCodec struct {
	registry *Registry
}

// NewJSONCodec returns a JSONCodec backed by the given Registry.
func NewJSONCodec(r *Registry) *JSONCodec {
	return &JSONCodec{registry: r}
}

type codecEnvelope struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data,omitempty"`
}

// Marshal encodes v into a JSON envelope tagged with its Go type. A nil
// value encodes as an empty envelope and round-trips back to nil. The payload
// is produced by the Registry's codec for v's type; JSONCodec owns only the
// envelope framing.
func (c *JSONCodec) Marshal(v any) ([]byte, error) {
	if v == nil {
		return json.Marshal(codecEnvelope{})
	}
	name, tc := c.registry.marshaler(v)
	data, err := tc.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("spark/middleware: codec marshal value: %w", err)
	}
	return json.Marshal(codecEnvelope{Type: name, Data: data})
}

// Unmarshal decodes an envelope produced by Marshal back into the original
// Go value. The payload is decoded by the Registry's codec for the tagged
// type; returns ErrUnknownType if that type was never registered.
func (c *JSONCodec) Unmarshal(data []byte) (any, error) {
	var e codecEnvelope
	if err := json.Unmarshal(data, &e); err != nil {
		return nil, fmt.Errorf("spark/middleware: codec unmarshal envelope: %w", err)
	}
	if e.Type == "" {
		return nil, nil
	}
	tc, err := c.registry.unmarshaler(e.Type)
	if err != nil {
		return nil, err
	}
	v, err := tc.Unmarshal(e.Data)
	if err != nil {
		return nil, fmt.Errorf("spark/middleware: codec unmarshal value: %w", err)
	}
	return v, nil
}
