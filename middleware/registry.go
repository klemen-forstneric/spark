package middleware

import (
	"fmt"
	"reflect"
)

// Registry maps a concrete type name to the TypeCodec that (de)serializes it.
type Registry struct {
	codecs map[string]TypeCodec
}

// NewRegistry registers each sample with a default JSONTypeCodec. It is sugar
// for Register(s, NewJSONTypeCodec(s)). The sample's form is the reconstructed
// form: pass &User{} to round-trip as *User, User{} to round-trip as a value.
func NewRegistry(samples ...any) *Registry {
	r := &Registry{codecs: map[string]TypeCodec{}}
	for _, s := range samples {
		if s == nil {
			continue
		}
		r.Register(s, NewJSONTypeCodec(s))
	}
	return r
}

// Register maps sample's concrete type to c. The pointer/value distinction is
// stripped from the key: &T{} and T{} both key on T.
func (r *Registry) Register(sample any, c TypeCodec) {
	r.codecs[derefName(sample)] = c
}

// marshaler resolves the codec for encoding v and its type name for the
// envelope. Unregistered types fall back to a default JSONTypeCodec, so any
// value can be encoded (though only registered types decode).
func (r *Registry) marshaler(v any) (typeName string, tc TypeCodec) {
	typeName = derefName(v)
	if c, ok := r.codecs[typeName]; ok {
		return typeName, c
	}
	return typeName, NewJSONTypeCodec(v)
}

// unmarshaler resolves the codec for the type tagged in an envelope.
// ErrUnknownType if the type was never registered.
func (r *Registry) unmarshaler(typeName string) (TypeCodec, error) {
	if c, ok := r.codecs[typeName]; ok {
		return c, nil
	}
	return nil, fmt.Errorf("%w: %s", ErrUnknownType, typeName)
}

func derefName(v any) string {
	rt := reflect.TypeOf(v)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	return rt.String()
}
