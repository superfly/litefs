// go:build darwin
package main

import (
	"context"
	"fmt"
	"os/exec"

	"github.com/superfly/litefs"
	"github.com/superfly/litefs/http"
)

// MountCommand represents a command to mount the file system.
type MountCommand struct {
	Config Config
	Store  *litefs.Store

	ProxyServer *http.ProxyServer
}

// NewMountCommand returns a new instance of MountCommand.
func NewMountCommand() *MountCommand {
	return &MountCommand{}
}

// Close closes the command.
func (c *MountCommand) Close() error { return nil }

// ExecCh always returns nil.
func (c *MountCommand) ExecCh() chan error { return nil }

// Cmd always returns nil.
func (c *MountCommand) Cmd() *exec.Cmd { return nil }

// ParseFlags returns an error for non-Linux systems.
func (c *MountCommand) ParseFlags(ctx context.Context, args []string) error {
	return fmt.Errorf("litefs-mount is not available on macOS")
}

func (c *MountCommand) Validate(ctx context.Context) (err error) {
	return fmt.Errorf("litefs-mount is not available on macOS")
}

func (c *MountCommand) Run(ctx context.Context) (err error) {
	return fmt.Errorf("litefs-mount is not available on macOS")
}
