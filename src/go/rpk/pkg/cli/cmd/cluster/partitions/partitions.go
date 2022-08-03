package partitions

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewPartitionsCommand(fs afero.Fs) *cobra.Command {
	var (
		adminURL       string
		adminEnableTLS bool
		adminCertFile  string
		adminKeyFile   string
		adminCAFile    string
	)

	cmd := &cobra.Command{
		Use:   "partitions",
		Args:  cobra.ExactArgs(0),
		Short: "Manage cluster partitions",
	}

	common.AddAdminAPITLSFlags(cmd,
		&adminEnableTLS,
		&adminCertFile,
		&adminKeyFile,
		&adminCAFile,
	)

	cmd.AddCommand(
		NewBalancerStatusCommand(fs),
		NewMovementCancelCommand(fs),
	)

	cmd.PersistentFlags().StringVar(
		&adminURL,
		config.FlagAdminHosts2,
		"",
		"Comma-separated list of admin API addresses (<IP>:<port>)")

	return cmd
}
