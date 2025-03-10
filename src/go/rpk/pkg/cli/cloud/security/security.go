package security

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cloud/security/secret"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "security",
		Short: "Manage rpk cloud security settings",
		Long: `Manage rpk cloud security settings.

This command allows you to configure and manage security settings for your cloud environment.`,
	}

	cmd.AddCommand(
		secret.NewCommand(fs, p),
	)

	return cmd
}
