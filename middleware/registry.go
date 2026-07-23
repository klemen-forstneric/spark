package middleware

import "reflect"

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
