package main_test

import (
	"embed"
	"flag"
	"log"
	"os"
	"testing"

	"github.com/superfly/litefs"
	"golang.org/x/exp/slog"
)

//go:embed testdata
var testdata embed.FS

var (
	fuseDebug = flag.Bool("fuse.debug", false, "enable fuse debugging")
	tracing   = flag.Bool("tracing", false, "enable trace logging")
	funTime   = flag.Duration("funtime", 0, "long-running, functional test time")
)

func init() {
	log.SetFlags(0)
	litefs.LogLevel.Set(slog.LevelDebug)
}

func TestMain(m *testing.M) {
	flag.Parse()
	if *tracing {
		litefs.TraceLog = log.New(os.Stdout, "", litefs.TraceLogFlags)
	}
	os.Exit(m.Run())
}
