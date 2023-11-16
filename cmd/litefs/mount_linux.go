// go:build linux
package main

import (
	"context"
	"expvar"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/mattn/go-shellwords"
	"github.com/superfly/litefs"
	"github.com/superfly/litefs/consul"
	"github.com/superfly/litefs/fly"
	"github.com/superfly/litefs/fuse"
	"github.com/superfly/litefs/http"
	"github.com/superfly/litefs/internal"
	"github.com/superfly/litefs/lfsc"
	"golang.org/x/exp/slog"
	"gopkg.in/natefinch/lumberjack.v2"
)

// MountCommand represents a command to mount the file system.
type MountCommand struct {
	cmd    *exec.Cmd  // subcommand
	execCh chan error // subcommand error channel

	Config Config

	OS   litefs.OS
	Exit func(int)

	Store       *litefs.Store
	Leaser      litefs.Leaser
	FileSystem  *fuse.FileSystem
	HTTPServer  *http.Server
	ProxyServer *http.ProxyServer

	// Used for generating the advertise URL for testing.
	AdvertiseURLFn func() string

	OnInitStore func()
}

// NewMountCommand returns a new instance of MountCommand.
func NewMountCommand() *MountCommand {
	return &MountCommand{
		execCh: make(chan error),
		Config: NewConfig(),
		OS:     &internal.SystemOS{},
		Exit:   os.Exit,
	}
}

func (c *MountCommand) Cmd() *exec.Cmd     { return c.cmd }
func (c *MountCommand) ExecCh() chan error { return c.execCh }

// ParseFlags parses the command line flags & config file.
func (c *MountCommand) ParseFlags(ctx context.Context, args []string) (err error) {
	// Split the args list if there is a double dash arg included. Arguments
	// after the double dash are used as the "exec" subprocess config option.
	args0, args1 := splitArgs(args)

	fs := flag.NewFlagSet("litefs-mount", flag.ContinueOnError)
	configPath := fs.String("config", "", "config file path")
	noExpandEnv := fs.Bool("no-expand-env", false, "do not expand env vars in config")
	fuseDebug := fs.Bool("fuse.debug", false, "enable FUSE debug logging")
	debug := fs.Bool("debug", false, "enable DEBUG level logging")
	tracing := fs.Bool("tracing", false, "enable trace logging to stdout")
	fs.Usage = func() {
		fmt.Println(`
The mount command will mount a LiteFS directory via FUSE and begin communicating
with the LiteFS cluster. The mount will be accessible once the node becomes the
primary or is able to connect and sync with the primary.

All options are specified in the litefs.yml config file which is searched for in
the present working directory, the current user's home directory, and then
finally at /etc/litefs.yml.

Usage:

	litefs mount [arguments]

Arguments:
`[1:])
		fs.PrintDefaults()
		fmt.Println("")
	}
	if err := fs.Parse(args0); err != nil {
		return err
	} else if fs.NArg() > 0 {
		return fmt.Errorf("too many arguments, specify a '--' to specify an exec command")
	}

	if err := ParseConfigPath(ctx, *configPath, !*noExpandEnv, &c.Config); err != nil {
		return err
	}

	// Override "exec" field if specified on the CLI.
	if args1 != nil {
		c.Config.Exec = ExecConfigSlice{{Cmd: strings.Join(args1, " ")}}
	}

	// Override "debug" field if specified on the CLI.
	if *fuseDebug {
		c.Config.FUSE.Debug = true
	}

	// Override debug logging if the flag is enabled.
	if *debug {
		c.Config.Log.Debug = true
	}

	// Enable trace logging, if specified. The config settings specify a rolling
	// on-disk log whereas the CLI flag specifies output to STDOUT.
	var tw io.Writer
	if c.Config.Tracing.Enabled && c.Config.Tracing.Path != "" {
		log.Printf("trace log enabled: %s", c.Config.Tracing.Path)
		tw = &lumberjack.Logger{
			Filename:   c.Config.Tracing.Path,
			MaxSize:    c.Config.Tracing.MaxSize,
			MaxBackups: c.Config.Tracing.MaxCount,
			Compress:   c.Config.Tracing.Compress,
		}
	}
	if *tracing {
		if tw == nil {
			tw = os.Stdout
		} else {
			tw = io.MultiWriter(os.Stdout, tw)
		}
	}
	if tw != nil {
		litefs.TraceLog.SetOutput(tw)
	}

	return nil
}

// Validate validates the application's configuration.
func (c *MountCommand) Validate(ctx context.Context) (err error) {
	if c.Config.FUSE.Dir == "" {
		return fmt.Errorf("fuse directory required")
	} else if c.Config.Data.Dir == "" {
		return fmt.Errorf("data directory required")
	} else if c.Config.FUSE.Dir == c.Config.Data.Dir {
		return fmt.Errorf("fuse directory and data directory cannot be the same path")
	}

	// Enforce a valid lease mode.
	if !IsValidLeaseType(c.Config.Lease.Type) {
		return fmt.Errorf("invalid lease type, must be either 'consul' or 'static', got: '%v'", c.Config.Lease.Type)
	}

	if c.Config.Lease.Candidate && len(c.Config.Lease.Databases) > 0 {
		return fmt.Errorf("cannot specify a database replication filter on candidate nodes")
	}

	return nil
}

const (
	LeaseTypeConsul = "consul"
	LeaseTypeStatic = "static"
)

// IsValidLeaseType returns true if s is a valid lease type.
func IsValidLeaseType(s string) bool {
	switch s {
	case LeaseTypeConsul, LeaseTypeStatic:
		return true
	default:
		return false
	}
}

func (c *MountCommand) Close() (err error) {
	if c.ProxyServer != nil {
		if e := internal.Close(c.ProxyServer); err == nil {
			err = e
		}
	}

	if c.HTTPServer != nil {
		if e := internal.Close(c.HTTPServer); err == nil {
			err = e
		}
	}

	if c.FileSystem != nil {
		if e := c.FileSystem.Unmount(); err == nil {
			err = e
		}
	}

	if c.Store != nil {
		if e := internal.Close(c.Store); err == nil {
			err = e
		}
	}

	return err
}

func (c *MountCommand) Run(ctx context.Context) (err error) {
	fmt.Println(VersionString())

	if err := c.initLogger(ctx); err != nil {
		return fmt.Errorf("init logger: %w", err)
	}

	// Start listening on HTTP server first so we can determine the URL.
	if err := c.initStore(ctx); err != nil {
		return fmt.Errorf("cannot init store: %w", err)
	} else if err := c.initHTTPServer(ctx); err != nil {
		return fmt.Errorf("cannot init http server: %w", err)
	}

	// Instantiate leaser.
	switch v := c.Config.Lease.Type; v {
	case LeaseTypeConsul:
		log.Println("Using Consul to determine primary")
		if err := c.initConsul(ctx); err != nil {
			return fmt.Errorf("cannot init consul: %w", err)
		}
	case LeaseTypeStatic:
		log.Printf("Using static primary: primary=%v hostname=%s advertise-url=%s",
			c.Config.Lease.Candidate, c.Config.Lease.Hostname, c.Config.Lease.AdvertiseURL)
		c.Leaser = litefs.NewStaticLeaser(c.Config.Lease.Candidate, c.Config.Lease.Hostname, c.Config.Lease.AdvertiseURL)
	default:
		return fmt.Errorf("invalid lease type: %q", v)
	}

	if err := c.openStore(ctx); err != nil {
		return fmt.Errorf("cannot open store: %w", err)
	}

	if err := c.initFileSystem(ctx); err != nil {
		return fmt.Errorf("cannot init file system: %w", err)
	}
	log.Printf("LiteFS mounted to: %s", c.FileSystem.Path())

	c.HTTPServer.Serve()
	log.Printf("http server listening on: %s", c.HTTPServer.URL())

	// Wait until the store either becomes primary or connects to the primary.
	if c.Config.SkipSync {
		log.Printf("skipping cluster sync, starting immediately")
	} else {
		log.Printf("waiting to connect to cluster")
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-c.Store.ReadyCh():
			log.Printf("connected to cluster, ready")
		}

		// Automatically promote the server if requested & it is a candidate.
		if c.Config.Lease.Promote {
			if c.Store.Candidate() {
				log.Printf("node is a candidate, automatically promoting to primary")
				if err := c.promote(ctx); err != nil {
					return fmt.Errorf("promote: %w", err)
				}
			} else {
				log.Printf("node is not a candidate, skipping automatic promotion")
			}
		}
	}

	// Start the proxy server before the subcommand in case we need to hold
	// requests after we promote but before the server is ready.
	if err := c.runProxyServer(ctx); err != nil {
		return fmt.Errorf("cannot run proxy server: %w", err)
	}

	// Execute subcommand, if specified in config.
	// Exit if no subcommand specified.
	if len(c.Config.Exec) > 0 {
		if err := c.execCmds(ctx); err != nil {
			return fmt.Errorf("cannot exec: %w", err)
		}
	}

	return nil
}

func (c *MountCommand) initLogger(ctx context.Context) error {
	// Enable debug logging, if set by the config.
	if c.Config.Log.Debug {
		litefs.LogLevel.Set(slog.LevelDebug)
	}

	opts := slog.HandlerOptions{Level: &litefs.LogLevel}

	if !c.Config.Log.Timestamp {
		opts.ReplaceAttr = removeTime
	}

	var handler slog.Handler
	switch format := c.Config.Log.Format; format {
	case "text":
		handler = slog.NewTextHandler(os.Stderr, &opts)
	case "json":
		handler = slog.NewJSONHandler(os.Stderr, &opts)
	default:
		return fmt.Errorf("invalid log format: %q", format)
	}

	slog.SetDefault(slog.New(handler))
	return nil
}

func (c *MountCommand) initConsul(ctx context.Context) (err error) {
	// TEMP: Allow non-localhost addresses.

	// Use hostname from OS, if not specified.
	hostname := c.Config.Lease.Hostname
	if hostname == "" {
		if hostname, err = os.Hostname(); err != nil {
			return err
		}
	}

	// Determine the advertise URL for the LiteFS API.
	// Default to use the hostname and HTTP port. Also allow injection for tests.
	advertiseURL := c.Config.Lease.AdvertiseURL
	if c.AdvertiseURLFn != nil {
		advertiseURL = c.AdvertiseURLFn()
	}
	if advertiseURL == "" && hostname != "" {
		advertiseURL = fmt.Sprintf("http://%s:%d", hostname, c.HTTPServer.Port())
	}

	leaser := consul.NewLeaser(c.Config.Lease.Consul.URL, c.Config.Lease.Consul.Key, hostname, advertiseURL)
	if v := c.Config.Lease.Consul.TTL; v > 0 {
		leaser.TTL = v
	}
	if v := c.Config.Lease.Consul.LockDelay; v > 0 {
		leaser.LockDelay = v
	}
	if err := leaser.Open(); err != nil {
		return fmt.Errorf("cannot connect to consul: %w", err)
	}
	log.Printf("initializing consul: key=%s url=%s hostname=%s advertise-url=%s",
		c.Config.Lease.Consul.Key, c.Config.Lease.Consul.URL, hostname, advertiseURL)

	c.Leaser = leaser
	return nil
}

func (c *MountCommand) initStore(ctx context.Context) error {
	c.Store = litefs.NewStore(c.Config.Data.Dir, c.Config.Lease.Candidate)
	c.Store.OS = c.OS
	c.Store.Exit = c.Exit
	c.Store.StrictVerify = c.Config.StrictVerify
	c.Store.Compress = c.Config.Data.Compress
	c.Store.Retention = c.Config.Data.Retention
	c.Store.RetentionMonitorInterval = c.Config.Data.RetentionMonitorInterval
	c.Store.ReconnectDelay = c.Config.Lease.ReconnectDelay
	c.Store.DemoteDelay = c.Config.Lease.DemoteDelay
	c.Store.Client = http.NewClient()
	c.Store.DatabaseFilter = c.Config.Lease.Databases
	c.initEnvironment(ctx)

	if c.OnInitStore != nil {
		c.OnInitStore()
	}

	if err := c.initStoreBackupClient(ctx); err != nil {
		return err
	}

	// Initialize as a singleton so we can automatically collect metrics.
	litefs.GlobalStore.Store(c.Store)

	return nil
}

func (c *MountCommand) initEnvironment(ctx context.Context) {
	if fly.Available() {
		c.Store.Environment = fly.NewEnvironment()
	}

	if typ := c.Store.Environment.Type(); typ != "" {
		slog.Info("host environment detected", slog.String("type", typ))
	}
}

func (c *MountCommand) initStoreBackupClient(ctx context.Context) error {
	c.initBackupConfigFromEnv(ctx)

	// Copy common fields.
	c.Store.BackupDelay = c.Config.Backup.Delay
	c.Store.BackupFullSyncInterval = c.Config.Backup.FullSyncInterval

	switch typ := c.Config.Backup.Type; typ {
	case "":
		log.Printf("no backup client configured, skipping")
		return nil

	case "file":
		client := litefs.NewFileBackupClient(c.Config.Backup.Path)
		if err := client.Open(); err != nil {
			return fmt.Errorf("open file backup client: %w", err)
		}
		c.Store.BackupClient = client
		log.Printf("file-based backup client configured: %s", client.URL())

	case "litefs-cloud":
		// Set default endpoint URL, unless already configured.
		if c.Config.Backup.URL == "" {
			c.Config.Backup.URL = "https://litefs.fly.io"
		}

		u, err := url.Parse(c.Config.Backup.URL)
		if err != nil {
			return fmt.Errorf("cannot parse liteserver backup URL: %w", err)
		}

		client := lfsc.NewBackupClient(c.Store, *u)
		client.Cluster = c.Config.Backup.Cluster
		client.AuthToken = c.Config.Backup.AuthToken

		if err := client.Open(); err != nil {
			return fmt.Errorf("open litefs cloud backup client: %w", err)
		}

		c.Store.BackupClient = client
		log.Printf("litefs cloud backup client configured: %s", client.URL())

	default:
		return fmt.Errorf("invalid backup client type: %q", typ)
	}

	return nil
}

func (c *MountCommand) initBackupConfigFromEnv(ctx context.Context) {
	// Automatically configure backup config if environment variable is set.
	token := strings.TrimSpace(os.Getenv("LITEFS_CLOUD_TOKEN"))
	if token == "" {
		return
	}

	// Only set if backup is not configure or if it's configured for LiteFS Cloud.
	if c.Config.Backup.Type != "" && c.Config.Backup.Type != "litefs-cloud" {
		return
	}
	c.Config.Backup.Type = "litefs-cloud"

	// Only set the token if it hasn't been configured.
	if c.Config.Backup.AuthToken == "" {
		c.Config.Backup.AuthToken = token
	}
}

func (c *MountCommand) openStore(ctx context.Context) error {
	c.Store.Leaser = c.Leaser
	if err := c.Store.Open(); err != nil {
		return err
	}

	if clusterID := c.Store.ClusterID(); clusterID != "" {
		log.Printf("using existing cluster id: %q", clusterID)
	}

	// Register expvar variable once so it doesn't panic during tests.
	expvarOnce.Do(func() { expvar.Publish("store", c.Store.Expvar()) })

	return nil
}

func (c *MountCommand) initFileSystem(ctx context.Context) error {
	// Build the file system to interact with the store.
	fsys := fuse.NewFileSystem(c.Config.FUSE.Dir, c.Store)
	fsys.AllowOther = c.Config.FUSE.AllowOther
	fsys.Debug = c.Config.FUSE.Debug
	if err := fsys.Mount(); err != nil {
		return fmt.Errorf("cannot open file system: %s", err)
	}

	// Attach file system to store so it can invalidate the page cache.
	c.Store.Invalidator = fsys

	c.FileSystem = fsys
	return nil
}

func (c *MountCommand) initHTTPServer(ctx context.Context) error {
	server := http.NewServer(c.Store, c.Config.HTTP.Addr)
	server.SnapshotTimeout = c.Config.HTTP.SnapshotTimeout
	if err := server.Listen(); err != nil {
		return fmt.Errorf("cannot open http server: %w", err)
	}
	c.HTTPServer = server
	return nil
}

func (c *MountCommand) runProxyServer(ctx context.Context) error {
	// Skip if there's no target set.
	if c.Config.Proxy.Target == "" {
		log.Printf("no proxy target set, skipping proxy")
		return nil
	}

	// Parse passthrough expressions.
	var passthroughs []*regexp.Regexp
	for _, s := range c.Config.Proxy.Passthrough {
		re, err := http.CompileMatch(s)
		if err != nil {
			return fmt.Errorf("cannot parse proxy passthrough expression: %q", s)
		}
		passthroughs = append(passthroughs, re)
	}

	// Parse always-forward expressions.
	var alwaysForward []*regexp.Regexp
	for _, s := range c.Config.Proxy.AlwaysForward {
		re, err := http.CompileMatch(s)
		if err != nil {
			return fmt.Errorf("cannot parse proxy always-forward expression: %q", s)
		}
		alwaysForward = append(alwaysForward, re)
	}

	server := http.NewProxyServer(c.Store)
	server.Target = c.Config.Proxy.Target
	server.DBName = c.Config.Proxy.DB
	server.Addr = c.Config.Proxy.Addr
	server.MaxLag = c.Config.Proxy.MaxLag
	server.Debug = c.Config.Proxy.Debug
	server.Passthroughs = passthroughs
	server.AlwaysForward = alwaysForward
	server.PrimaryRedirectTimeout = c.Config.Proxy.PrimaryRedirectTimeout
	server.ReadTimeout = c.Config.Proxy.ReadTimeout
	server.ReadHeaderTimeout = c.Config.Proxy.ReadHeaderTimeout
	server.WriteTimeout = c.Config.Proxy.WriteTimeout
	server.IdleTimeout = c.Config.Proxy.IdleTimeout

	if err := server.Listen(); err != nil {
		return err
	}
	c.ProxyServer = server

	c.ProxyServer.Serve()
	log.Printf("proxy server listening on: %s", c.ProxyServer.URL())

	return nil
}

// execCmds sequentially executes the commands in the "exec" config.
// The last command is run asynchronously and will send its exit to the execCh.
func (c *MountCommand) execCmds(ctx context.Context) error {
	for i, config := range c.Config.Exec {
		args, err := shellwords.Parse(config.Cmd)
		if err != nil {
			return fmt.Errorf("cannot parse exec command[%d]: %w", i, err)
		}
		cmd, args := args[0], args[1:]

		// Skip if command should only run on candidate nodes and this is a non-candidate.
		if config.IfCandidate && !c.Store.Candidate() {
			log.Printf("node is not a candidate, skipping command execution: %s %v", cmd, args)
			continue
		}

		// Execute all commands synchronously except for the last one.
		// This is to support migration commands that occur before the app start.
		if i < len(c.Config.Exec)-1 {
			if err := c.execSyncCmd(ctx, cmd, args); err != nil {
				return fmt.Errorf("sync cmd: %w", err)
			}
		} else {
			if err := c.execBackgroundCmd(ctx, cmd, args); err != nil {
				return fmt.Errorf("background cmd: %w", err)
			}
		}
	}

	return nil
}

func (c *MountCommand) execSyncCmd(ctx context.Context, cmd string, args []string) error {
	log.Printf("executing command: %s %v", cmd, args)

	c.cmd = exec.CommandContext(ctx, cmd, args...)
	c.cmd.Env = os.Environ()
	c.cmd.Stdout = os.Stdout
	c.cmd.Stderr = os.Stderr
	if err := c.cmd.Run(); err != nil {
		return fmt.Errorf("cannot run command: %w", err)
	}

	return nil
}

func (c *MountCommand) execBackgroundCmd(ctx context.Context, cmd string, args []string) error {
	log.Printf("starting background subprocess: %s %v", cmd, args)

	c.cmd = exec.CommandContext(ctx, cmd, args...)
	c.cmd.Env = os.Environ()
	c.cmd.Stdout = os.Stdout
	c.cmd.Stderr = os.Stderr
	if err := c.cmd.Start(); err != nil {
		return fmt.Errorf("cannot start exec command: %w", err)
	}
	go func() { c.execCh <- c.cmd.Wait() }()

	return nil
}

// promote issues a lease handoff request to the current primary.
func (c *MountCommand) promote(ctx context.Context) (err error) {
	isPrimary, info := c.Store.PrimaryInfo()
	if isPrimary {
		log.Printf("node is already primary, skipping promotion")
		return nil
	}

	client := http.NewClient()
	if err := client.Handoff(ctx, info.AdvertiseURL, c.Store.ID()); err != nil {
		return fmt.Errorf("handoff: %w", err)
	}

	// Wait for the local node to become primary.
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTicker(10 * time.Second)
	defer timeout.Stop()

	for {
		select {
		case <-timeout.C:
			return fmt.Errorf("timed out waiting for promotion")
		case <-ticker.C:
			if c.Store.IsPrimary() {
				return nil
			}
		}
	}
}

var expvarOnce sync.Once

// removeTime removes the "time" field from slog.
func removeTime(groups []string, a slog.Attr) slog.Attr {
	if a.Key == slog.TimeKey {
		return slog.Attr{}
	}
	return a
}
