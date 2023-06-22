package fly

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"time"

	"golang.org/x/exp/slog"
)

const DefaultTimeout = 2 * time.Second

type Environment struct {
	HTTPClient *http.Client

	Timeout time.Duration
}

func NewEnvironment() *Environment {
	return &Environment{
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return net.Dial("unix", "/.fly/api")
				},
			},
		},
		Timeout: DefaultTimeout,
	}
}

func (e *Environment) Type() string { return "fly.io" }

func (e *Environment) SetPrimaryStatus(ctx context.Context, isPrimary bool) error {
	appName := AppName()
	if appName == "" {
		slog.Info("cannot set primary status on host environment", slog.String("reason", "app name unavailable"))
		return nil
	}

	machineID := MachineID()
	if machineID == "" {
		slog.Info("cannot set primary status on host environment", slog.String("reason", "machine id unavailable"))
		return nil
	}

	role := "replica"
	if isPrimary {
		role = "primary"
	}

	reqBody, err := json.Marshal(postMetadataRequest{
		Value: role,
	})
	if err != nil {
		return fmt.Errorf("marshal metadata request body: %w", err)
	}

	u := url.URL{
		Scheme: "http",
		Host:   "localhost",
		Path:   path.Join("/v1", "apps", appName, "machines", machineID, "metadata", "role"),
	}
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(reqBody))
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, e.Timeout)
	defer cancel()
	req = req.WithContext(ctx)

	resp, err := e.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent:
		return nil
	default:
		return fmt.Errorf("cannot set machine metadata: code=%d", resp.StatusCode)
	}
}

type postMetadataRequest struct {
	Value string `json:"value"`
}

// Available returns true if currently running in a Fly.io environment.
func Available() bool { return AppName() != "" }

// AppName returns the name of the current Fly.io application.
func AppName() string {
	return os.Getenv("FLY_APP_NAME")
}

// MachineID returns the identifier for the current Fly.io machine.
func MachineID() string {
	return os.Getenv("FLY_MACHINE_ID")
}
