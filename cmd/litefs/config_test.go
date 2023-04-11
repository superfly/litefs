package main_test

import (
	"bytes"
	"strings"
	"testing"

	main "github.com/superfly/litefs/cmd/litefs"
	"gopkg.in/yaml.v3"
)

func TestConfig(t *testing.T) {
	t.Run("EmptyExec", func(t *testing.T) {
		var config main.Config
		dec := yaml.NewDecoder(strings.NewReader("exec:\n"))
		dec.KnownFields(true)
		if err := dec.Decode(&config); err != nil {
			t.Fatal(err)
		}

		if got, want := len(config.Exec), 0; got != want {
			t.Fatalf("len=%v, want %v", got, want)
		}
	})

	t.Run("InlineExec", func(t *testing.T) {
		var config main.Config
		dec := yaml.NewDecoder(strings.NewReader(`exec: "run me"`))
		dec.KnownFields(true)
		if err := dec.Decode(&config); err != nil {
			t.Fatal(err)
		}

		if got, want := config.Exec[0].Cmd, "run me"; got != want {
			t.Fatalf("Cmd=%q, want %q", got, want)
		}
	})

	t.Run("SingleExec", func(t *testing.T) {
		buf, err := testdata.ReadFile("testdata/config/single_exec.yml")
		if err != nil {
			t.Fatal(err)
		}

		var config main.Config
		dec := yaml.NewDecoder(bytes.NewReader(buf))
		dec.KnownFields(true)
		if err := dec.Decode(&config); err != nil {
			t.Fatal(err)
		}

		if got, want := config.Exec[0].Cmd, "run me"; got != want {
			t.Fatalf("Cmd=%q, want %q", got, want)
		}
	})

	t.Run("MultiExec", func(t *testing.T) {
		buf, err := testdata.ReadFile("testdata/config/multi_exec.yml")
		if err != nil {
			t.Fatal(err)
		}

		var config main.Config
		dec := yaml.NewDecoder(bytes.NewReader(buf))
		dec.KnownFields(true)
		if err := dec.Decode(&config); err != nil {
			t.Fatal(err)
		}

		if got, want := config.Exec[0].Cmd, "run me"; got != want {
			t.Fatalf("Cmd=%q, want %q", got, want)
		} else if got, want := config.Exec[0].IfCandidate, true; got != want {
			t.Fatalf("IfCandidate=%v, want %v", got, want)
		}
		if got, want := config.Exec[1].Cmd, "run me too"; got != want {
			t.Fatalf("Cmd=%q, want %q", got, want)
		}
	})
}
