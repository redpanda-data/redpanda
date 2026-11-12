// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package versioncmd

import (
	"fmt"
	"runtime"
	"time"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/version"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
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

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var skipCluster bool
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Prints the current rpk and Redpanda version",
		Long: `Prints the current rpk and Redpanda version.

This command prints the current rpk version and allows you to list the Redpanda
version running on each node in your cluster.

To list the Redpanda version of each node in your cluster you may pass the
Admin API hosts using flags, profile, or environment variables.

To print only the rpk version without contacting a cluster, use the
'--skip-cluster' flag or run 'rpk --version'.`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			rv := rpkVersion{
				Version:   version.Version,
				GitRef:    version.Rev,
				BuildTime: version.BuildTime,
				GoVersion: runtime.Version(),
				OsArch:    fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
			}
			printRpkVersion(rv)
			var rows redpandaVersions
			printCV := true
			defer func() {
				if printCV {
					printClusterVersions(&rows)
				}
			}()

			// When --skip-cluster is set we only report the rpk
			// version and do not reach out to a cluster.
			if skipCluster {
				printCV = false
				return
			}

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
	cmd.Flags().BoolVar(&skipCluster, "skip-cluster", false, "Skip contacting the cluster and only print the rpk version")
	return cmd
}

func printRpkVersion(rv rpkVersion) {
	fmt.Printf(`rpk version: %s
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
    rpk version -X admin.hosts=<host address>

  To get only the rpk version, use 'rpk --version'.`)
		return
	}
	for _, v := range *rpv {
		fmt.Printf("  node-%v  %s\n", v.NodeID, v.Version)
	}
}
