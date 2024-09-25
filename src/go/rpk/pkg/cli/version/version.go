// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package version

import (
	"fmt"
	"runtime"
	"time"

	"github.com/redpanda-data/common-go/rpadmin"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// Default build-time variables, passed via ldflags.
var (
	version   string // Semver of rpk.
	rev       string // Short git SHA.
	buildTime string // Timestamp of build time, RFC3339.
	hostOs    string // OS that was used to built rpk (go env GOHOSTOS).
	hostArch  string // Arch that was used to built rpk (go env GOHOSTARCH).
)

type rpkVersion struct {
	Version   string `json:"version,omitempty" yaml:"version,omitempty"`
	GitRef    string `json:"git_ref,omitempty" yaml:"git_ref,omitempty"`
	BuildTime string `json:"build_time,omitempty" yaml:"build_time,omitempty"`
	GoVersion string `json:"go_version,omitempty" yaml:"go_version,omitempty"`
	OsArch    string `json:"os_arch,omitempty" yaml:"os_arch,omitempty"`
}

type redpandaVersion struct {
	NodeID  int
	Version string
}
type redpandaVersions []redpandaVersion

func Pretty() string {
	return fmt.Sprintf("%s (rev %s)", version, rev)
}

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Prints the current rpk and Redpanda version",
		Long: `Prints the current rpk and Redpanda version.

This command prints the current rpk version and allows you to list the Redpanda 
version running on each node in your cluster.

To list the Redpanda version of each node in your cluster you may pass the
Admin API hosts via flags, profile, or environment variables.`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			rv := rpkVersion{
				Version:   version,
				GitRef:    rev,
				BuildTime: buildTime,
				GoVersion: runtime.Version(),
				OsArch:    fmt.Sprintf("%s/%s", hostOs, hostArch),
			}
			printRpkVersion(rv)
			var rows redpandaVersions
			printCV := true
			defer func() {
				if printCV {
					printClusterVersions(&rows)
				}
			}()

			p, err := p.LoadVirtualProfile(fs)
			if err != nil {
				zap.L().Sugar().Errorf("unable to load the profile: %v", err)
				return
			}
			// Cloud clusters don't expose their admin API, the rest of the
			// command will always fail. We better exit early.
			if p.FromCloud {
				printCV = false
				return
			}
			cl, err := adminapi.NewClient(
				cmd.Context(),
				fs,
				p,
				rpadmin.ClientTimeout(3*time.Second),
				rpadmin.MaxRetries(2),
			)
			if err != nil {
				zap.L().Sugar().Errorf("unable to create the admin client: %v", err)
				return
			}
			bs, err := cl.Brokers(cmd.Context())
			if err != nil {
				zap.L().Sugar().Errorf("unable to request broker info: %v", err)
				return
			}
			for _, b := range bs {
				if b.IsAlive != nil {
					rows = append(rows, redpandaVersion{b.NodeID, b.Version})
				}
			}
		},
	}
	return cmd
}

func printRpkVersion(rv rpkVersion) {
	fmt.Printf(`Version:     %s
Git ref:     %s
Build date:  %s
OS/Arch:     %s
Go version:  %s
`, rv.Version, rv.GitRef, rv.BuildTime, rv.OsArch, rv.GoVersion)
}

func printClusterVersions(rpv *redpandaVersions) {
	fmt.Println()
	fmt.Println("Redpanda Cluster")
	if len(*rpv) == 0 {
		fmt.Println(`  Unreachable, to debug, use the '-v' flag. To get the broker versions, pass the
  hosts via flags, profile, or environment variables:
    rpk version -X admin.hosts=<host address>`)
		return
	}
	for _, v := range *rpv {
		fmt.Printf("  node-%v  %s\n", v.NodeID, v.Version)
	}
}
