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
	"sync/atomic"
	"time"

	"golang.org/x/exp/slog"
)

const DefaultTimeout = 2 * time.Second

type Environment struct {
	setPrimaryStatusCancel atomic.Value

	HTTPClient *http.Client
	Timeout    time.Duration
}

func NewEnvironment() *Environment {
	e := &Environment{
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return net.Dial("unix", "/.fly/api")
				},
			},
		},
		Timeout: DefaultTimeout,
	}
	e.setPrimaryStatusCancel.Store(context.CancelCauseFunc(func(error) {}))

	return e
}

func (e *Environment) Type() string { return "fly.io" }

func (e *Environment) SetPrimaryStatus(ctx context.Context, isPrimary bool) {
	const retryN = 5

	appName := AppName()
	if appName == "" {
		slog.Debug("cannot set environment metadata", slog.String("reason", "app name unavailable"))
		return
	}

	machineID := MachineID()
	if machineID == "" {
		slog.Debug("cannot set environment metadata", slog.String("reason", "machine id unavailable"))
		return
	}

	// Ensure we only have a single in-flight command at a time as the primary
	// status can change while we are retrying. This status is only for
	// informational purposes so it is not critical to correct functioning.
	ctx, cancel := context.WithCancelCause(ctx)
	oldCancel := e.setPrimaryStatusCancel.Swap(cancel).(context.CancelCauseFunc)
	oldCancel(fmt.Errorf("interrupted by new status update"))

	// Continuously retry status update in case the unix socket is unavailable
	// or in case we have exceeded the rate limit. We run this in a goroutine
	// so that we are not blocking the main lease loop.
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		var err error
		for i := 0; ; i++ {
			if err = e.setPrimaryStatus(ctx, isPrimary); err == nil {
				return
			} else if i >= retryN {
				break
			}

			select {
			case <-ctx.Done():
				slog.Debug("cannot set environment metadata",
					slog.String("reason", "context canceled"),
					slog.Any("err", context.Cause(ctx)))
				return
			case <-ticker.C:
			}
		}

		slog.Info("cannot set environment metadata",
			slog.String("reason", "retries exceeded"),
			slog.Any("err", err))
	}()
}

func (e *Environment) setPrimaryStatus(ctx context.Context, isPrimary bool) error {
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
		Path:   path.Join("/v1", "apps", AppName(), "machines", MachineID(), "metadata", "role"),
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
