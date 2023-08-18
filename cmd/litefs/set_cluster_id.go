package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/superfly/litefs/http"
)

// SetClusterIDCommand represents a command to override the cluster ID.
type SetClusterIDCommand struct{}

// NewSetClusterIDCommand returns a new instance of SetClusterIDCommand.
func NewSetClusterIDCommand() *SetClusterIDCommand {
	return &SetClusterIDCommand{}
}

// Run executes the command.
func (c *SetClusterIDCommand) Run(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litefs-set-cluster-id", flag.ContinueOnError)
	baseURL := fs.String("url", DefaultURL, "LiteFS API URL")
	fs.Usage = func() {
		fmt.Println(`
This command will override the existing cluster id on the node to a new value.
If configured to use Consul, the value will be updated there as well.

Usage:

	litefs set-cluster-id [arguments] CLUSTERID

Arguments:
`[1:])
		fs.PrintDefaults()
		fmt.Println("")
	}
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() == 0 {
		fs.Usage()
		return flag.ErrHelp
	} else if fs.NArg() > 1 {
		return fmt.Errorf("too many arguments")
	}

	clusterID := fs.Arg(0)

	client := http.NewClient()
	if err := client.SetClusterID(ctx, *baseURL, clusterID); err != nil {
		return err
	}

	fmt.Println("Cluster ID updated.")

	return nil
}
