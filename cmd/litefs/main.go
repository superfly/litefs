package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/superfly/litefs"
	"github.com/superfly/litefs/http"
	"gopkg.in/yaml.v3"
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
	case "import":
		c := NewImportCommand()
		if err := c.ParseFlags(ctx, args); err != nil {
			return err
		}
		return c.Run(ctx)

	case "mount":
		return runMount(ctx, args)

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

	ctx, cancel := context.WithCancel(ctx)

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
	}

	fmt.Println("waiting for signal or subprocess to exit")

	// Wait for signal or subcommand exit to stop program.
	var exitCode int
	select {
	case err := <-c.ExecCh():
		cancel()

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

		cancel()
		fmt.Println("signal received, litefs shutting down")
	}

	if err := c.Close(); err != nil {
		return err
	}

	fmt.Println("litefs shut down complete")
	os.Exit(exitCode)

	return nil
}

// NOTE: Update etc/litefs.yml configuration file after changing the structure below.

// Config represents a configuration for the binary process.
type Config struct {
	Exec         string `yaml:"exec"`
	ExitOnError  bool   `yaml:"exit-on-error"`
	SkipSync     bool   `yaml:"skip-sync"`
	StrictVerify bool   `yaml:"strict-verify"`

	Data    DataConfig    `yaml:"data"`
	FUSE    FUSEConfig    `yaml:"fuse"`
	HTTP    HTTPConfig    `yaml:"http"`
	Proxy   ProxyConfig   `yaml:"proxy"`
	Lease   LeaseConfig   `yaml:"lease"`
	Tracing TracingConfig `yaml:"tracing"`
}

// NewConfig returns a new instance of Config with defaults set.
func NewConfig() Config {
	var config Config
	config.ExitOnError = true

	config.Data.Compress = true
	config.Data.Retention = litefs.DefaultRetention
	config.Data.RetentionMonitorInterval = litefs.DefaultRetentionMonitorInterval

	config.HTTP.Addr = http.DefaultAddr

	config.Lease.Candidate = true
	config.Lease.ReconnectDelay = litefs.DefaultReconnectDelay
	config.Lease.DemoteDelay = litefs.DefaultDemoteDelay

	config.Tracing.MaxSize = DefaultTracingMaxSize
	config.Tracing.MaxCount = DefaultTracingMaxCount
	config.Tracing.Compress = DefaultTracingCompress

	return config
}

// DataConfig represents the configuration for internal LiteFS data. This
// includes database files as well as LTX transaction files.
type DataConfig struct {
	Dir      string `yaml:"dir"`
	Compress bool   `yaml:"compress"`

	Retention                time.Duration `yaml:"retention"`
	RetentionMonitorInterval time.Duration `yaml:"retention-monitor-interval"`
}

// FUSEConfig represents the configuration for the FUSE file system.
type FUSEConfig struct {
	Dir        string `yaml:"dir"`
	AllowOther bool   `yaml:"allow-other"`
	Debug      bool   `yaml:"debug"`
}

// HTTPConfig represents the configuration for the HTTP server.
type HTTPConfig struct {
	Addr string `yaml:"addr"`
}

// ProxyConfig represents the configuration for the HTTP proxy server.
type ProxyConfig struct {
	Addr        string   `yaml:"addr"`
	Target      string   `yaml:"target"`
	DB          string   `yaml:"db"`
	Debug       bool     `yaml:"debug"`
	Passthrough []string `yaml:"passthrough"`
}

// LeaseConfig represents a generic configuration for all lease types.
type LeaseConfig struct {
	// Specifies the type of leasing to use: "consul" or "static"
	Type string `yaml:"type"`

	// The hostname of this node. Used by the application to forward requests.
	Hostname string `yaml:"hostname"`

	// URL for other nodes to access this node's API.
	AdvertiseURL string `yaml:"advertise-url"`

	// Specifies if this node can become primary. Defaults to true.
	//
	// If using a "static" lease, setting this to true makes it the primary.
	// Replicas in a state lease should set this to false.
	Candidate bool `yaml:"candidate"`

	// After disconnect, time before node tries to reconnect to primary or
	// becomes primary itself.
	ReconnectDelay time.Duration `yaml:"reconnect-delay"`

	// Amount of time to wait after a forced demotion before attempting to
	// become primary again.
	DemoteDelay time.Duration `yaml:"demote-delay"`

	// Consul lease settings.
	Consul struct {
		URL       string        `yaml:"url"`
		Key       string        `yaml:"key"`
		TTL       time.Duration `yaml:"ttl"`
		LockDelay time.Duration `yaml:"lock-delay"`
	} `yaml:"consul"`
}

// Tracing configuration defaults.
const (
	DefaultTracingMaxSize  = 64 // MB
	DefaultTracingMaxCount = 8
	DefaultTracingCompress = true
)

// TracingConfig represents the configuration the on-disk trace log.
type TracingConfig struct {
	Path     string `yaml:"path"`
	MaxSize  int    `yaml:"max-size"`
	MaxCount int    `yaml:"max-count"`
	Compress bool   `yaml:"compress"`
}

// UnmarshalConfig unmarshals config from data.
// If expandEnv is true then environment variables are expanded in the config.
func UnmarshalConfig(config *Config, data []byte, expandEnv bool) error {
	// Expand environment variables, if enabled.
	if expandEnv {
		data = []byte(ExpandEnv(string(data)))
	}

	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true) // strict checking
	if err := dec.Decode(&config); err != nil {
		return err
	}
	return nil
}

// ExpandEnv replaces environment variables just like os.ExpandEnv() but also
// allows for equality/inequality binary expressions within the ${} form.
func ExpandEnv(s string) string {
	return os.Expand(s, func(v string) string {
		v = strings.TrimSpace(v)

		if a := expandExprSingleQuote.FindStringSubmatch(v); a != nil {
			if a[2] == "==" {
				return strconv.FormatBool(os.Getenv(a[1]) == a[3])
			}
			return strconv.FormatBool(os.Getenv(a[1]) != a[3])
		}

		if a := expandExprDoubleQuote.FindStringSubmatch(v); a != nil {
			if a[2] == "==" {
				return strconv.FormatBool(os.Getenv(a[1]) == a[3])
			}
			return strconv.FormatBool(os.Getenv(a[1]) != a[3])
		}

		if a := expandExprVar.FindStringSubmatch(v); a != nil {
			if a[2] == "==" {
				return strconv.FormatBool(os.Getenv(a[1]) == os.Getenv(a[3]))
			}
			return strconv.FormatBool(os.Getenv(a[1]) != os.Getenv(a[3]))
		}

		return os.Getenv(v)
	})
}

var (
	expandExprSingleQuote = regexp.MustCompile(`^(\w+)\s*(==|!=)\s*'(.*)'$`)
	expandExprDoubleQuote = regexp.MustCompile(`^(\w+)\s*(==|!=)\s*"(.*)"$`)
	expandExprVar         = regexp.MustCompile(`^(\w+)\s*(==|!=)\s*(\w+)$`)
)

// splitArgs returns the list of args before and after a "--" arg. If the double
// dash is not specified, then args0 is args and args1 is empty.
func splitArgs(args []string) (args0, args1 []string) {
	for i, v := range args {
		if v == "--" {
			return args[:i], args[i+1:]
		}
	}
	return args, nil
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

	import       import a SQLite database into a LiteFS cluster
	mount        mount the LiteFS FUSE file system
	version      prints the version
`[1:])
}
