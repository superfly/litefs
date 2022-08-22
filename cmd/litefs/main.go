// go:build linux
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/mattn/go-shellwords"
	"github.com/superfly/litefs"
	"github.com/superfly/litefs/consul"
	"github.com/superfly/litefs/fuse"
	"github.com/superfly/litefs/http"
	"gopkg.in/yaml.v3"
)

func main() {
	log.SetFlags(0)

	signalCh := make(chan os.Signal, 2)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	// Set HOSTNAME environment variable, if unset by environment.
	// This can be used for variable expansion in the config file.
	if os.Getenv("HOSTNAME") == "" {
		hostname, _ := os.Hostname()
		_ = os.Setenv("HOSTNAME", hostname)
	}

	m := NewMain()
	if err := m.ParseFlags(ctx, os.Args[1:]); err == flag.ErrHelp {
		os.Exit(2)
	} else if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	if err := m.Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		_ = m.Close()
		os.Exit(1)
	}

	// Wait for signal or subcommand exit to stop program.
	select {
	case <-m.execCh:
		cancel()
		fmt.Println("subprocess exited, litefs shutting down")

	case sig := <-signalCh:
		if m.cmd != nil {
			fmt.Println("sending signal to exec process")
			if err := m.cmd.Process.Signal(sig); err != nil {
				fmt.Fprintln(os.Stderr, "cannot signal exec process:", err)
				os.Exit(1)
			}

			fmt.Println("waiting for exec process to close")
			if err := <-m.execCh; err != nil && !strings.HasPrefix(err.Error(), "signal:") {
				fmt.Fprintln(os.Stderr, "cannot wait for exec process:", err)
				os.Exit(1)
			}
		}

		cancel()
		fmt.Println("signal received, litefs shutting down")
	}

	if err := m.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// Main represents the command line program.
type Main struct {
	cmd    *exec.Cmd  // subcommand
	execCh chan error // subcommand error channel

	Config Config

	Store      *litefs.Store
	Leaser     litefs.Leaser
	FileSystem *fuse.FileSystem
	HTTPServer *http.Server

	// Used for generating the advertise URL for testing.
	AdvertiseURLFn func() string
}

// NewMain returns a new instance of Main.
func NewMain() *Main {
	return &Main{
		execCh: make(chan error),
		Config: NewConfig(),
	}
}

// ParseFlags parses the command line flags & config file.
func (m *Main) ParseFlags(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litefs", flag.ContinueOnError)
	configPath := fs.String("config", "", "config file path")
	noExpandEnv := fs.Bool("no-expand-env", false, "do not expand env vars in config")
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() > 0 {
		return fmt.Errorf("too many arguments")
	}

	// Only read from explicit path, if specified. Report any error.
	if *configPath != "" {
		return ReadConfigFile(&m.Config, *configPath, !*noExpandEnv)
	}

	// Attempt to read each config path until we succeed.
	for _, path := range configSearchPaths() {
		if path, err = filepath.Abs(path); err != nil {
			return err
		}

		if err := ReadConfigFile(&m.Config, path, !*noExpandEnv); err == nil {
			fmt.Printf("config file read from %s\n", path)
			return nil
		} else if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("cannot read config file at %s: %s", path, err)
		}
	}
	return fmt.Errorf("config file not found")
}

// configSearchPaths returns paths to search for the config file. It starts with
// the current directory, then home directory, if available. And finally it tries
// to read from the /etc directory.
func configSearchPaths() []string {
	a := []string{"litefs.yml"}
	if u, _ := user.Current(); u != nil && u.HomeDir != "" {
		a = append(a, filepath.Join(u.HomeDir, "litefs.yml"))
	}
	a = append(a, "/etc/litefs.yml")
	return a
}

func (m *Main) Close() (err error) {
	if m.HTTPServer != nil {
		if e := m.HTTPServer.Close(); err == nil {
			err = e
		}
	}

	if m.FileSystem != nil {
		if e := m.FileSystem.Unmount(); err == nil {
			err = e
		}
	}

	if m.Store != nil {
		if e := m.Store.Close(); err == nil {
			err = e
		}
	}

	return err
}

func (m *Main) Run(ctx context.Context) (err error) {
	if m.Config.MountDir == "" {
		return fmt.Errorf("mount path required")
	}

	// Enforce exactly one lease mode.
	if m.Config.Consul != nil && m.Config.Static != nil {
		return fmt.Errorf("cannot specify both 'consul' and 'static' lease modes")
	} else if m.Config.Consul == nil && m.Config.Static == nil {
		return fmt.Errorf("must specify a lease mode ('consul', 'static')")
	}

	// Start listening on HTTP server first so we can determine the URL.
	if err := m.initStore(ctx); err != nil {
		return fmt.Errorf("cannot init store: %w", err)
	} else if err := m.initHTTPServer(ctx); err != nil {
		return fmt.Errorf("cannot init http server: %w", err)
	}

	// Instantiate leaser.
	if m.Config.Consul != nil {
		log.Println("Using Consul to determine primary")
		if err := m.initConsul(ctx); err != nil {
			return fmt.Errorf("cannot init consul: %w", err)
		}
	} else { // static
		log.Printf("Using static primary: is-primary=%v hostname=%s advertise-url=%s", m.Config.Static.Primary, m.Config.Static.Hostname, m.Config.Static.AdvertiseURL)
		m.Leaser = litefs.NewStaticLeaser(m.Config.Static.Primary, m.Config.Static.Hostname, m.Config.Static.AdvertiseURL)
	}

	if err := m.openStore(ctx); err != nil {
		return fmt.Errorf("cannot open store: %w", err)
	}

	if err := m.initFileSystem(ctx); err != nil {
		return fmt.Errorf("cannot init file system: %w", err)
	}
	log.Printf("LiteFS mounted to: %s", m.FileSystem.Path())

	m.HTTPServer.Serve()
	log.Printf("http server listening on: %s", m.HTTPServer.URL())

	// Execute subcommand, if specified in config.
	if err := m.execCmd(ctx); err != nil {
		return fmt.Errorf("cannot exec: %w", err)
	}

	return nil
}

func (m *Main) initConsul(ctx context.Context) (err error) {
	// TEMP: Allow non-localhost addresses.

	// Use hostname from OS, if not specified.
	hostname := m.Config.Consul.Hostname
	if hostname == "" {
		if hostname, err = os.Hostname(); err != nil {
			return err
		}
	}

	// Determine the advertise URL for the LiteFS API.
	// Default to use the hostname and HTTP port. Also allow injection for tests.
	advertiseURL := m.Config.Consul.AdvertiseURL
	if m.AdvertiseURLFn != nil {
		advertiseURL = m.AdvertiseURLFn()
	}
	if advertiseURL == "" && hostname != "" {
		advertiseURL = fmt.Sprintf("http://%s:%d", hostname, m.HTTPServer.Port())
	}

	leaser := consul.NewLeaser(m.Config.Consul.URL, hostname, advertiseURL)
	if v := m.Config.Consul.Key; v != "" {
		leaser.Key = v
	}
	if v := m.Config.Consul.TTL; v > 0 {
		leaser.TTL = v
	}
	if v := m.Config.Consul.LockDelay; v > 0 {
		leaser.LockDelay = v
	}
	if err := leaser.Open(); err != nil {
		return fmt.Errorf("cannot connect to consul: %w", err)
	}
	log.Printf("initializing consul: key=%s url=%s hostname=%s advertise-url=%s", m.Config.Consul.Key, m.Config.Consul.URL, hostname, advertiseURL)

	m.Leaser = leaser
	return nil
}

func (m *Main) initStore(ctx context.Context) error {
	// Load the data directory from the config.
	// Default to use a hidden directory next to the mount, if not specified.
	dataDir := m.Config.DataDir
	if dataDir == "" {
		dir, file := filepath.Split(m.Config.MountDir)
		dataDir = filepath.Join(dir, "."+file)
	}

	m.Store = litefs.NewStore(dataDir, m.Config.Candidate)
	m.Store.RetentionDuration = m.Config.Retention.Duration
	m.Store.RetentionMonitorInterval = m.Config.Retention.MonitorInterval
	m.Store.Client = http.NewClient()
	return nil
}

func (m *Main) openStore(ctx context.Context) error {
	m.Store.Leaser = m.Leaser
	return m.Store.Open()
}

func (m *Main) initFileSystem(ctx context.Context) error {
	// Build the file system to interact with the store.
	fsys := fuse.NewFileSystem(m.Config.MountDir, m.Store)
	fsys.Debug = m.Config.Debug
	if err := fsys.Mount(); err != nil {
		return fmt.Errorf("cannot open file system: %s", err)
	}

	// Attach file system to store so it can invalidate the page cache.
	m.Store.Invalidator = fsys

	m.FileSystem = fsys
	return nil
}

func (m *Main) initHTTPServer(ctx context.Context) error {
	server := http.NewServer(m.Store, m.Config.HTTP.Addr)
	if err := server.Listen(); err != nil {
		return fmt.Errorf("cannot open http server: %w", err)
	}
	m.HTTPServer = server
	return nil
}

func (m *Main) execCmd(ctx context.Context) error {
	// Exit if no subcommand specified.
	if m.Config.Exec == "" {
		return nil
	}

	// TODO: Wait for primary/replica connection.
	time.Sleep(5 * time.Second)

	// Execute subcommand process.
	args, err := shellwords.Parse(m.Config.Exec)
	if err != nil {
		return fmt.Errorf("cannot parse exec command: %w", err)
	}

	log.Printf("starting subprocess: %s %v", args[0], args[1:])

	m.cmd = exec.CommandContext(ctx, args[0], args[1:]...)
	m.cmd.Env = os.Environ()
	m.cmd.Stdout = os.Stdout
	m.cmd.Stderr = os.Stderr
	if err := m.cmd.Start(); err != nil {
		return fmt.Errorf("cannot start exec command: %w", err)
	}
	go func() { m.execCh <- m.cmd.Wait() }()

	return nil
}

// NOTE: Update etc/litefs.yml configuration file after changing the structure below.

// Config represents a configuration for the binary process.
type Config struct {
	MountDir  string `yaml:"mount-dir"`
	DataDir   string `yaml:"data-dir"`
	Exec      string `yaml:"exec"`
	Debug     bool   `yaml:"debug"`
	Candidate bool   `yaml:"candidate"`

	Retention RetentionConfig `yaml:"retention"`
	HTTP      HTTPConfig      `yaml:"http"`
	Consul    *ConsulConfig   `yaml:"consul"`
	Static    *StaticConfig   `yaml:"static"`
}

// NewConfig returns a new instance of Config with defaults set.
func NewConfig() Config {
	var config Config
	config.Candidate = true
	config.Retention.Duration = litefs.DefaultRetentionDuration
	config.Retention.MonitorInterval = litefs.DefaultRetentionMonitorInterval
	config.HTTP.Addr = http.DefaultAddr
	return config
}

// RetentionConfig represents the configuration for LTX file retention.
type RetentionConfig struct {
	Duration        time.Duration `yaml:"duration"`
	MonitorInterval time.Duration `yaml:"monitor-interval"`
}

// HTTPConfig represents the configuration for the HTTP server.
type HTTPConfig struct {
	Addr string `yaml:"addr"`
}

// ConsulConfig represents the configuration for a Consul leaser.
type ConsulConfig struct {
	URL          string        `yaml:"url"`
	Hostname     string        `yaml:"hostname"`
	AdvertiseURL string        `yaml:"advertise-url"`
	Key          string        `yaml:"key"`
	TTL          time.Duration `yaml:"ttl"`
	LockDelay    time.Duration `yaml:"lock-delay"`
}

// StaticConfig represents the configuration for a static leaser.
type StaticConfig struct {
	Primary      bool   `yaml:"primary"`
	Hostname     string `yaml:"hostname"`
	AdvertiseURL string `yaml:"advertise-url"`
}

// ReadConfigFile unmarshals config from filename. If expandEnv is true then
// environment variables are expanded in the config.
func ReadConfigFile(config *Config, filename string, expandEnv bool) error {
	// Read configuration.
	buf, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	// Expand environment variables, if enabled.
	if expandEnv {
		buf = []byte(os.ExpandEnv(string(buf)))
	}

	if err := yaml.Unmarshal(buf, &config); err != nil {
		return err
	}
	return nil
}
