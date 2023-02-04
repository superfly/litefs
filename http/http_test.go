package http_test

import (
	"testing"

	"github.com/superfly/litefs/http"
)

func TestCompileMatch(t *testing.T) {
	for _, tt := range []struct {
		expr    string
		str     string
		matches bool
	}{
		{"/build/*", "/build", false},
		{"/build/*", "/build/", true},
		{"/build/*", "/build/foo", true},
		{"/build/*", "/build/foo/bar", true},
		{"*.png", "/images/pic.png", true},
		{"*foo*", "/foo", true},
		{"*foo*", "foo/bar", true},
		{"*foo*", "/foo/bar", true},
		{"*foo*", "/bar/baz", false},
	} {
		t.Run("", func(t *testing.T) {
			re, err := http.CompileMatch(tt.expr)
			if err != nil {
				t.Fatal(err)
			}
			matched := re.MatchString(tt.str)
			if tt.matches && !matched {
				t.Fatalf("expected %q to match %q, but didn't", tt.expr, tt.str)
			} else if !tt.matches && matched {
				t.Fatalf("expected %q to not match %q, but did", tt.expr, tt.str)
			}
		})
	}
}
