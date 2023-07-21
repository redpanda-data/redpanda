// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cmd

import (
	"fmt"
	"os"

	vectorizedv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/vectorized/v1alpha1"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/i18n"
	"sigs.k8s.io/yaml"

	"github.com/redpanda-data/redpanda/src/go/cluster-to-redpanda-migration/cmd/version"
	"github.com/redpanda-data/redpanda/src/go/cluster-to-redpanda-migration/pkg/migration"
)

var (
	defaultName            = "redpanda-cluster"
	defaultNamespace       = "default"
	DefaultChartRefVersion = "4.0.54"
)

type RootFlags struct {
	ClusterFile string
	ConsoleFile string
	OutputFile  string

	ChartVersion              string
	RedpandaImageTag          string
	RedpandaContainerRegistry string
}

func Execute() error {
	flags := &RootFlags{}

	// RootCmd represents the base command when called without any subcommands
	root := &cobra.Command{
		Use:   "cluster-to-redpanda-migration",
		Short: i18n.T("Redpanda operator manifest migration tool"),
		Long:  "",
		RunE: func(cmd *cobra.Command, args []string) error {
			if flags.ClusterFile == "" && flags.ConsoleFile == "" {
				return fmt.Errorf("must have valid cluster or console resources")
			}

			return DoMigration(flags)
		},
	}

	pf := root.PersistentFlags()
	pf.StringVar(&flags.OutputFile, "output", "redpanda.yaml", "migration output file")
	pf.StringVar(&flags.ClusterFile, "cluster", "", "cluster file")
	pf.StringVar(&flags.ConsoleFile, "console", "", "console file")

	pf.StringVar(&flags.ChartVersion, "chart-version", DefaultChartRefVersion, "chart - version to use for migration")
	pf.StringVar(&flags.RedpandaImageTag, "redpanda-version", "v23.1.10", "Redpanda container tag")
	pf.StringVar(&flags.RedpandaContainerRegistry, "redpanda-containerRegistry", "docker.redpanda.com/redpandadata/redpanda", "container registry to find Redpanda image")

	// add commands
	root.AddCommand(version.NewCommand())

	return root.Execute()
}

func init() {
}

func DoMigration(flags *RootFlags) error {
	cluster, err := getClusterObj(flags.ClusterFile)
	if err != nil {
		return fmt.Errorf("reading Cluster input file: %w", err)
	}

	console, err := getConsoleObj(flags.ConsoleFile)
	if err != nil {
		return fmt.Errorf("reading Console input file: %w", err)
	}

	if cluster == nil {
		defaultName = console.Name
		defaultNamespace = console.Namespace
	}

	rp := migration.CreateMigratedObj(cluster, flags.ChartVersion, defaultName, defaultNamespace)

	if cluster != nil {
		migration.MigrateClusterSpec(cluster, &rp, flags.RedpandaContainerRegistry, flags.RedpandaImageTag)
		migration.MigrateRedpandaConfig(cluster, &rp)
		migration.MigrateConfigurations(cluster.Spec.AdditionalConfiguration, &rp)
	}

	migration.MigrateConsole(console, &rp)

	outputData, err := yaml.Marshal(rp)
	if err != nil {
		return err
	}

	return os.WriteFile(flags.OutputFile, outputData, 0o644)
}

func getClusterObj(file string) (*vectorizedv1alpha1.Cluster, error) {
	if file == "" {
		return nil, nil
	}

	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}

	cluster := &vectorizedv1alpha1.Cluster{}

	err = yaml.Unmarshal(data, cluster)
	if err != nil {
		return nil, err
	}

	return cluster, nil
}

func getConsoleObj(file string) (*vectorizedv1alpha1.Console, error) {
	if file == "" {
		return nil, nil
	}

	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}

	console := &vectorizedv1alpha1.Console{}

	err = yaml.Unmarshal(data, console)
	if err != nil {
		return nil, err
	}

	return console, nil
}
