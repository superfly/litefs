package main_test

import (
	"flag"
	"log"
	"os"
	"testing"

	"github.com/superfly/litefs"
)

var (
	fuseDebug = flag.Bool("fuse.debug", false, "enable fuse debugging")
	tracing   = flag.Bool("tracing", false, "enable trace logging")
	funTime   = flag.Duration("funtime", 0, "long-running, functional test time")
)

func init() {
	log.SetFlags(0)
}

func TestMain(m *testing.M) {
	flag.Parse()
	if *tracing {
		litefs.TraceLog = log.New(os.Stdout, "", litefs.TraceLogFlags)
	}
	os.Exit(m.Run())
}
