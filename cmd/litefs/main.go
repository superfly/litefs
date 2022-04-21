package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/liteserver/litefs"
)

func main() {
	log.SetFlags(0)

	if err := run(context.Background()); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run(ctx context.Context) (err error) {
	debug := flag.Bool("debug", false, "print debug information")
	rawURL := flag.String("url", "http://localhost:8080", "liteserver url")
	flag.Parse()

	path := flag.Arg(0)
	if path == "" {
		return fmt.Errorf("usage: litefs PATH")
	}

	// Convert path to an absolute path.
	if path, err = filepath.Abs(path); err != nil {
		return fmt.Errorf("abs: %w", err)
	}

	log.Printf("mounting to: %s", path)

	s := litefs.NewServer(path)
	s.Debug = *debug
	s.URL = *rawURL
	if err := s.Open(); err != nil {
		return fmt.Errorf("cannot mount: %w", err)
	}
	defer s.Close()

	s.Wait()

	return s.Close()
}
