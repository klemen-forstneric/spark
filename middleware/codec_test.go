package middleware_test

import (
	"errors"
	"testing"

	"github.com/klemen-forstneric/spark/middleware"
)

type codecUser struct {
	ID    string
	Email string
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
