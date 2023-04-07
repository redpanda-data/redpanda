package license

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewLicenseCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "license",
		Args:  cobra.ExactArgs(0),
		Short: "Manage cluster license",
	}
	p.InstallAdminFlags(cmd)
	cmd.AddCommand(
		newInfoCommand(fs, p),
		newSetCommand(fs, p),
	)
	return cmd
}
