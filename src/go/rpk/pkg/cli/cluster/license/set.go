package license

import (
	"fmt"
	"io"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newSetCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var licPath string
	cmd := &cobra.Command{
		Use:   "set",
		Args:  cobra.MaximumNArgs(1),
		Short: "Upload license to the cluster",
		Long: `Upload license to the cluster

You can either provide a path to a file containing the license:

    rpk cluster license set --path /home/organization/redpanda.license

Or inline the license string:

    rpk cluster license set <license string>

If neither are present, rpk will look for the license in the
default location '/etc/redpanda/redpanda.license'.
`,

		Run: func(cmd *cobra.Command, args []string) {
			if licPath != "" && len(args) > 0 {
				out.Die("inline license cannot be passed if flag '--path' is set")
			}
			if licPath == "" && len(args) == 0 {
				fmt.Println("Neither license file nor inline license was provided, checking '/etc/redpanda/redpanda.license'.")
				licPath = "/etc/redpanda/redpanda.license"
			}

			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			var r io.Reader
			if licPath != "" {
				file, err := fs.Open(licPath)
				out.MaybeDie(err, "unable to open %q: %v", licPath, err)
				defer file.Close()
				r = file
			} else {
				r = strings.NewReader(args[0])
			}

			err = cl.SetLicense(cmd.Context(), r)
			out.MaybeDie(err, "unable to set license: %v", err)

			fmt.Println("Successfully uploaded license.")
		},
	}
	cmd.Flags().StringVar(&licPath, "path", "", "Path to the license file")
	return cmd
}
