package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
)

// Build information.
var (
	Version = ""
	Commit  = ""
)

// DefaultURL refers to the LiteFS API on the local machine.
const DefaultURL = "http://localhost:20202"

func main() {
	log.SetFlags(0)

	if err := run(context.Background(), os.Args[1:]); err == flag.ErrHelp {
		os.Exit(2)
	} else if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	// Extract command name.
	var cmd string
	if len(args) > 0 {
		cmd, args = args[0], args[1:]
	}

	switch cmd {
	case "export":
		c := NewExportCommand()
		if err := c.ParseFlags(ctx, args); err != nil {
			return err
		}
		return c.Run(ctx)

	case "import":
		c := NewImportCommand()
		if err := c.ParseFlags(ctx, args); err != nil {
			return err
		}
		return c.Run(ctx)

	case "mount":
		return runMount(ctx, args)

	case "run":
		c := NewRunCommand()
		if err := c.ParseFlags(ctx, args); err != nil {
			return err
		}
		return c.Run(ctx)

	case "version":
		fmt.Println(VersionString())
		return nil

	default:
		if cmd == "" || cmd == "help" || strings.HasPrefix(cmd, "-") {
			printUsage()
			return flag.ErrHelp
		}
		return fmt.Errorf("litefs %s: unknown command", cmd)
	}
}

func runMount(ctx context.Context, args []string) error {
	signalCh := make(chan os.Signal, 2)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancelCause(ctx)

	// Set HOSTNAME environment variable, if unset by environment.
	// This can be used for variable expansion in the config file.
	if os.Getenv("HOSTNAME") == "" {
		hostname, _ := os.Hostname()
		_ = os.Setenv("HOSTNAME", hostname)
	}

	// Initialize binary and parse CLI flags & config.
	c := NewMountCommand()
	if err := c.ParseFlags(ctx, args); err == flag.ErrHelp {
		os.Exit(2)
	} else if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
		os.Exit(2)
	}

	// Validate configuration.
	if err := c.Validate(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
		os.Exit(2)
	}

	if err := c.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)

		// Only exit the process if enabled in the config. A user want to
		// continue running so that an ephemeral node can be debugged intsead
		// of continually restarting on error.
		if c.Config.ExitOnError {
			_ = c.Close()
			os.Exit(1)
		}

		// Ensure proxy server is closed on error. Otherwise it can be in a
		// state where it is accepting connections but not processing them.
		// See: https://github.com/superfly/litefs/pull/278#issuecomment-1419460935
		if c.ProxyServer != nil {
			log.Printf("closing proxy server on startup error")
			_ = c.ProxyServer.Close()
		}
	}

	fmt.Println("waiting for signal or subprocess to exit")

	// Wait for signal or subcommand exit to stop program.
	var exitCode int
	select {
	case err := <-c.ExecCh():
		cancel(fmt.Errorf("canceled, subprocess exited"))

		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ProcessState.ExitCode()
			fmt.Printf("subprocess exited with error code %d, litefs shutting down\n", exitCode)
		} else if err != nil {
			exitCode = 1
			fmt.Printf("subprocess exited with error, litefs shutting down: %s\n", err)
		} else {
			fmt.Println("subprocess exited successfully, litefs shutting down")
		}

	case sig := <-signalCh:
		if cmd := c.Cmd(); cmd != nil {
			fmt.Println("sending signal to exec process")
			if err := cmd.Process.Signal(sig); err != nil {
				return fmt.Errorf("cannot signal exec process: %w", err)
			}

			fmt.Println("waiting for exec process to close")
			if err := <-c.ExecCh(); err != nil && !strings.HasPrefix(err.Error(), "signal:") {
				return fmt.Errorf("cannot wait for exec process: %w", err)
			}
		}

		cancel(fmt.Errorf("canceled, signal received"))
		fmt.Println("signal received, litefs shutting down")
	}

	if err := c.Close(); err != nil {
		return err
	}

	fmt.Println("litefs shut down complete")
	os.Exit(exitCode)

	return nil
}

func VersionString() string {
	// Print version & commit information, if available.
	if Version != "" {
		return fmt.Sprintf("LiteFS %s, commit=%s", Version, Commit)
	} else if Commit != "" {
		return fmt.Sprintf("LiteFS commit=%s", Commit)
	}
	return "LiteFS development build"
}

// printUsage prints the help screen to STDOUT.
func printUsage() {
	fmt.Println(`
litefs is a distributed file system for replicating SQLite databases.

Usage:

	litefs <command> [arguments]

The commands are:

	export       export a database from a LiteFS cluster to disk
	import       import a SQLite database into a LiteFS cluster
	mount        mount the LiteFS FUSE file system
	run          executes a subcommand for remote writes
	version      prints the version
`[1:])
}
