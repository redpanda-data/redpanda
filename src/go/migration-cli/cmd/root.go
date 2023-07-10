package cmd

import (
	"fmt"
	"os"

	vectorizedv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/vectorized/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/migration-cli/pkg/migration"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/i18n"

	"sigs.k8s.io/yaml"
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

	ChartVersion         string
	RedpandaImageVersion string
	RedpandaRepository   string
}

func Execute() error {
	flags := &RootFlags{}

	// RootCmd represents the base command when called without any subcommands
	root := &cobra.Command{
		Use:   "migration-cli",
		Short: i18n.T("Redpanda Migration Tool"),
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
	pf.StringVar(&flags.RedpandaImageVersion, "redpanda-version", "v23.1.10", "redpanda - version to use for migration")
	pf.StringVar(&flags.RedpandaRepository, "redpanda-repository", "docker.redpanda.com/redpandadata/redpanda", "redpanda - repository to find redpanda image")

	return root.Execute()
}

func init() {
}

func DoMigration(flags *RootFlags) error {
	cluster, err := getClusterObj(flags.ClusterFile)
	if err != nil {
		return err
	}

	console, err := getConsoleObj(flags.ConsoleFile)
	if err != nil {
		return err
	}

	if cluster == nil {
		defaultName = console.Name
		defaultNamespace = console.Namespace
	}

	rp := migration.CreateMigratedObj(cluster, flags.ChartVersion, defaultName, defaultNamespace)

	if cluster != nil {
		migration.MigrateClusterSpec(cluster, &rp, flags.RedpandaRepository, flags.RedpandaImageVersion)
		migration.MigrateRedpandaConfig(cluster, &rp)
		migration.MigrateConfigurations(cluster.Spec.AdditionalConfiguration, &rp)
	}

	migration.MigrateConsole(console, &rp)

	outputData, err := yaml.Marshal(rp)
	if err != nil {
		return err
	}

	return os.WriteFile(flags.OutputFile, outputData, 0o777)
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
