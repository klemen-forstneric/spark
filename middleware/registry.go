package middleware

import (
	"encoding/json"
	"fmt"
	"reflect"
)

// TypeCodec serializes one concrete result type. Register it on a Registry to
// override the default reflection+json path for that type.
type TypeCodec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(b []byte) (any, error)
}

// Registry maps stable type identifiers to their reflect.Type for
// round-trip reconstruction. Build one with NewRegistry, listing every
// concrete result type that may flow through the codec.
type Registry struct {
	types  map[string]reflect.Type
	custom map[string]TypeCodec
}

// NewRegistry constructs a Registry containing the types of the given
// samples. Pointer/value distinction is captured at encode time and
// honored at decode time, so passing &User{} registers User; both User
// and *User can be round-tripped later.
func NewRegistry(samples ...any) *Registry {
	r := &Registry{types: map[string]reflect.Type{}, custom: map[string]TypeCodec{}}
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

// Register overrides serialization for sample's concrete type with c. The
// pointer/value distinction is stripped: registering &T{} or T{} both key on T.
func (r *Registry) Register(sample any, c TypeCodec) {
	rt := reflect.TypeOf(sample)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	r.custom[rt.String()] = c
}

// marshaler resolves the codec for encoding v — a registered TypeCodec, or the
// default json codec bound to v's concrete type — and reports the type name and
// pointer-ness for the envelope. Never fails: any type can be encoded.
func (r *Registry) marshaler(v any) (typeName string, pointer bool, tc TypeCodec) {
	rt := reflect.TypeOf(v)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
		pointer = true
	}
	typeName = rt.String()
	if c, ok := r.custom[typeName]; ok {
		return typeName, pointer, c
	}
	return typeName, pointer, jsonTypeCodec{rt: rt, pointer: pointer}
}

// unmarshaler resolves the codec for decoding a payload tagged typeName — a
// registered TypeCodec, or the default json codec bound to the registered
// sample type. ErrUnknownType if the type was never registered (its concrete
// type is unknown, so it cannot be reconstructed).
func (r *Registry) unmarshaler(typeName string, pointer bool) (TypeCodec, error) {
	if c, ok := r.custom[typeName]; ok {
		return c, nil
	}
	if rt, ok := r.types[typeName]; ok {
		return jsonTypeCodec{rt: rt, pointer: pointer}, nil
	}
	return nil, fmt.Errorf("%w: %s", ErrUnknownType, typeName)
}

// jsonTypeCodec is the default TypeCodec: json for the payload, reconstructing
// the concrete value-or-pointer type via reflection on decode.
type jsonTypeCodec struct {
	rt      reflect.Type
	pointer bool
}

func (c jsonTypeCodec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

func (c jsonTypeCodec) Unmarshal(b []byte) (any, error) {
	ptr := reflect.New(c.rt)
	if err := json.Unmarshal(b, ptr.Interface()); err != nil {
		return nil, err
	}
	if c.pointer {
		return ptr.Interface(), nil
	}
	return ptr.Elem().Interface(), nil
}
