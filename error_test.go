package ddbstreams

import (
	"io"
	"testing"
)

func TestWrappedError(t *testing.T) {
	t.Run("root", func(t *testing.T) {
		msg := "hello world"
		err := wrapErr(nil, msg)
		if got, want := err.Error(), msg; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("wrapped", func(t *testing.T) {
		msg := "hello world"
		err := wrapErr(io.EOF, msg)
		if got, want := err.Error(), "EOF: "+msg; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("cause", func(t *testing.T) {
		type causer interface {
			Cause() error
		}

		want := io.EOF
		err := wrapErr(want, "blah")
		v, ok := err.(causer)
		if !ok {
			t.Fatalf("expected true; got false")
		}
		if got, want := v.Cause(), want; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})
}
