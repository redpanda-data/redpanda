package partitions

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewPartitionsCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "partitions",
		Args:  cobra.ExactArgs(0),
		Short: "Manage cluster partitions",
	}
	p.InstallAdminFlags(cmd)
	cmd.AddCommand(
		newBalancerStatusCommand(fs, p),
		newMovementCancelCommand(fs, p),
	)
	return cmd
}
