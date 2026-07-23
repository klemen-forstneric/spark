package middleware_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/klemen-forstneric/spark/middleware"
)

type codecUser struct {
	ID    string
	Email string
}

// stubEntity is a value the reflection codec cannot round-trip (identity in an
// unexported field), exercising the custom TypeCodec path.
type stubEntity struct{ id string }

// stubCodec serializes stubEntity via its unexported field.
type stubCodec struct{}

func (stubCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(map[string]string{"id": v.(*stubEntity).id})
}
func (stubCodec) Unmarshal(b []byte) (any, error) {
	var m map[string]string
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &stubEntity{id: m["id"]}, nil
}

type plainResult struct {
	Name string `json:"name"`
}

func TestJSONCodecRoundTripPointer(t *testing.T) {
	c := middleware.NewJSONCodec(middleware.NewRegistry(&codecUser{}))

	original := &codecUser{ID: "u1", Email: "a@b.c"}
	data, err := c.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	decoded, err := c.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	got, ok := decoded.(*codecUser)
	if !ok {
		t.Fatalf("decoded is %T, want *codecUser", decoded)
	}
	if got.ID != original.ID || got.Email != original.Email {
		t.Fatalf("got %+v, want %+v", got, original)
	}
}

func TestJSONCodecRoundTripValue(t *testing.T) {
	c := middleware.NewJSONCodec(middleware.NewRegistry(codecUser{}))

	original := codecUser{ID: "u1", Email: "a@b.c"}
	data, err := c.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	decoded, err := c.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	got, ok := decoded.(codecUser)
	if !ok {
		t.Fatalf("decoded is %T, want codecUser", decoded)
	}
	if got != original {
		t.Fatalf("got %+v, want %+v", got, original)
	}
}

func TestJSONCodecNilRoundTrip(t *testing.T) {
	c := middleware.NewJSONCodec(middleware.NewRegistry())
	data, err := c.Marshal(nil)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := c.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got != nil {
		t.Fatalf("got %v, want nil", got)
	}
}

func TestJSONCodecUnknownType(t *testing.T) {
	c := middleware.NewJSONCodec(middleware.NewRegistry())
	data, err := c.Marshal(&codecUser{ID: "u1"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	_, err = c.Unmarshal(data)
	if !errors.Is(err, middleware.ErrUnknownType) {
		t.Fatalf("expected ErrUnknownType, got %v", err)
	}
}

func TestJSONCodecCustomTypeRoundTrip(t *testing.T) {
	reg := middleware.NewRegistry()
	reg.Register(&stubEntity{}, stubCodec{})
	c := middleware.NewJSONCodec(reg)

	data, err := c.Marshal(&stubEntity{id: "e1"})
	require.NoError(t, err)

	got, err := c.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, &stubEntity{id: "e1"}, got)
}

func TestJSONCodecFallbackForUnregisteredType(t *testing.T) {
	reg := middleware.NewRegistry(plainResult{})
	c := middleware.NewJSONCodec(reg)

	data, err := c.Marshal(plainResult{Name: "x"})
	require.NoError(t, err)

	got, err := c.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, plainResult{Name: "x"}, got)
}
