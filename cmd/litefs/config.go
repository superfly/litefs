package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/superfly/litefs"
	"github.com/superfly/litefs/http"
	"gopkg.in/yaml.v3"
)

const (
	DefaultDataDir = "/var/lib/litefs"
	DefaultFUSEDir = "/litefs"
)

// NOTE: Update etc/litefs.yml configuration file after changing the structure below.

// Config represents a configuration for the binary process.
type Config struct {
	ExitOnError  bool `yaml:"exit-on-error"`
	SkipSync     bool `yaml:"skip-sync"`
	StrictVerify bool `yaml:"strict-verify"`

	Exec ExecConfigSlice `yaml:"exec"`

	Data    DataConfig    `yaml:"data"`
	FUSE    FUSEConfig    `yaml:"fuse"`
	HTTP    HTTPConfig    `yaml:"http"`
	Proxy   ProxyConfig   `yaml:"proxy"`
	Lease   LeaseConfig   `yaml:"lease"`
	Backup  BackupConfig  `yaml:"backup"`
	Log     LogConfig     `yaml:"log"`
	Tracing TracingConfig `yaml:"tracing"`
}

// NewConfig returns a new instance of Config with defaults set.
func NewConfig() Config {
	var config Config
	config.ExitOnError = true

	config.Data.Dir = DefaultDataDir
	config.Data.Compress = true
	config.Data.Retention = litefs.DefaultRetention
	config.Data.RetentionMonitorInterval = litefs.DefaultRetentionMonitorInterval

	config.FUSE.Dir = DefaultFUSEDir

	config.HTTP.Addr = http.DefaultAddr

	config.Lease.Candidate = true
	config.Lease.ReconnectDelay = litefs.DefaultReconnectDelay
	config.Lease.DemoteDelay = litefs.DefaultDemoteDelay

	config.Backup.Delay = litefs.DefaultBackupDelay
	config.Backup.FullSyncInterval = litefs.DefaultBackupFullSyncInterval

	config.Log.Format = "text"

	config.Proxy.MaxLag = http.DefaultMaxLag
	config.Proxy.PrimaryRedirectTimeout = http.DefaultPrimaryRedirectTimeout
	config.Proxy.ReadTimeout = http.DefaultReadTimeout
	config.Proxy.ReadHeaderTimeout = http.DefaultReadHeaderTimeout
	config.Proxy.WriteTimeout = http.DefaultWriteTimeout
	config.Proxy.IdleTimeout = http.DefaultIdleTimeout

	config.Tracing.Enabled = true
	config.Tracing.MaxSize = DefaultTracingMaxSize
	config.Tracing.MaxCount = DefaultTracingMaxCount
	config.Tracing.Compress = DefaultTracingCompress

	return config
}

// ExecConfigSlice represents a wrapper type for handling YAML marshaling.
type ExecConfigSlice []*ExecConfig

func (a *ExecConfigSlice) UnmarshalYAML(value *yaml.Node) error {
	switch value.Tag {
	case "!!str":
		*a = ExecConfigSlice{{Cmd: value.Value}}
		return nil
	case "!!seq":
		return value.Decode((*[]*ExecConfig)(a))
	default:
		return fmt.Errorf("invalid exec config format")
	}
}

// ExecConfig represents a single exec command.
type ExecConfig struct {
	Cmd         string `yaml:"cmd"`
	IfCandidate bool   `yaml:"if-candidate"`
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
	Addr            string        `yaml:"addr"`
	SnapshotTimeout time.Duration `yaml:"snapshot-timeout"`
}

// ProxyConfig represents the configuration for the HTTP proxy server.
type ProxyConfig struct {
	Addr                   string        `yaml:"addr"`
	Target                 string        `yaml:"target"`
	DB                     string        `yaml:"db"`
	MaxLag                 time.Duration `yaml:"max-lag"`
	Debug                  bool          `yaml:"debug"`
	Passthrough            []string      `yaml:"passthrough"`
	AlwaysForward          []string      `yaml:"always-forward"`
	PrimaryRedirectTimeout time.Duration `yaml:"primary-redirect-timeout"`

	ReadTimeout       time.Duration `yaml:"read-timeout"`
	ReadHeaderTimeout time.Duration `yaml:"read-header-timeout"`
	WriteTimeout      time.Duration `yaml:"write-timeout"`
	IdleTimeout       time.Duration `yaml:"idle-timeout"`
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

	// If true & node is a candidate, it will attempt to promote itself
	// automatically once it connects to the cluster and syncs.
	Promote bool `yaml:"promote"`

	// After disconnect, time before node tries to reconnect to primary or
	// becomes primary itself.
	ReconnectDelay time.Duration `yaml:"reconnect-delay"`

	// Amount of time to wait after a forced demotion before attempting to
	// become primary again.
	DemoteDelay time.Duration `yaml:"demote-delay"`

	// Specifies a subset of databases to replica.
	Databases []string `yaml:"databases"`

	// Consul lease settings.
	Consul struct {
		URL       string        `yaml:"url"`
		Key       string        `yaml:"key"`
		TTL       time.Duration `yaml:"ttl"`
		LockDelay time.Duration `yaml:"lock-delay"`
	} `yaml:"consul"`
}

// BackupConfig represents a config for backup services.
type BackupConfig struct {
	Type             string        `yaml:"type"`
	Path             string        `yaml:"path"`       // "file" only
	URL              string        `yaml:"url"`        // "litefs-cloud" only
	Cluster          string        `yaml:"cluster"`    // "litefs-cloud" only
	AuthToken        string        `yaml:"auth-token"` // "litefs-cloud" only
	Delay            time.Duration `yaml:"-"`
	FullSyncInterval time.Duration `yaml:"-"`
}

// LogConfig represents the configuration for logging.
type LogConfig struct {
	Format    string `yaml:"format"`    // "text", "json"
	Timestamp bool   `yaml:"timestamp"` // include timestamp in log output
	Debug     bool   `yaml:"debug"`     // include debug logging
}

// Tracing configuration defaults.
const (
	DefaultTracingMaxSize  = 64 // MB
	DefaultTracingMaxCount = 8
	DefaultTracingCompress = true
)

// TracingConfig represents the configuration the on-disk trace log.
type TracingConfig struct {
	Enabled  bool   `yaml:"enabled"`
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

// ParseConfigPath parses the configuration file from configPath, if specified.
// Returns ok true if a configuration file was successfully found.
//
// Otherwise searches the standard list of search paths. Returns an error if
// no configuration files could be found.
func ParseConfigPath(ctx context.Context, configPath string, expandEnv bool, config *Config) (err error) {
	// Only read from explicit path, if specified. Report any error.
	if configPath != "" {
		// Read configuration.
		buf, err := os.ReadFile(configPath)
		if err != nil {
			return err
		}
		return UnmarshalConfig(config, buf, expandEnv)
	}

	// Otherwise attempt to read each config path until we succeed.
	for _, path := range configSearchPaths() {
		if path, err = filepath.Abs(path); err != nil {
			return err
		}

		buf, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			continue
		} else if err != nil {
			return fmt.Errorf("cannot read config file at %s: %s", path, err)
		}

		if err := UnmarshalConfig(config, buf, expandEnv); err != nil {
			return fmt.Errorf("cannot unmarshal config file at %s: %s", path, err)
		}

		fmt.Printf("config file read from %s\n", path)
		return nil
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
