package license

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newInfoCommand(fs afero.Fs) *cobra.Command {
	var format string
	command := &cobra.Command{
		Use:   "info",
		Args:  cobra.ExactArgs(0),
		Short: "Retrieve license information",
		Long: `Retrieve license information:

    Organization:    Organization the license was generated for.
    Type:            Type of license: free, enterprise, etc.
    Expires:         Number of days the license is valid until or -1 if is expired.
    Version:         License schema version.
`,
		Run: func(cmd *cobra.Command, args []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			info, err := cl.GetLicenseInfo(cmd.Context())
			out.MaybeDie(err, "unable to retrieve license info: %v", err)

			if !info.Loaded {
				if format == "json" {
					out.Die("{}")
				} else {
					out.Die("this cluster is missing a license")
				}
			}

			if info.Properties != (admin.LicenseProperties{}) {
				expired := info.Properties.Expires < 0
				if format == "json" {
					props, err := json.MarshalIndent(struct {
						admin.LicenseProperties
						Expired bool `json:"license_expired,omitempty"`
					}{info.Properties, expired}, "", "  ")
					out.MaybeDie(err, "unable to print license information as json: %v", err)
					fmt.Printf("%s\n", props)
				} else {
					printLicenseInfo(info.Properties, expired)
				}
			} else {
				out.Die("no license loaded")
			}
		},
	}

	command.Flags().StringVar(&format, "format", "text", "Output format (text, json)")
	return command
}

func printLicenseInfo(p admin.LicenseProperties, expired bool) {
	out.Section("LICENSE INFORMATION")
	licenseFormat := `Organization:      %v
Type:              %v
Expires:           %v days
Version:           %v
`
	if expired {
		licenseFormat += `License Expired:   true
`
	}
	fmt.Printf(licenseFormat, p.Organization, p.Type, p.Expires, p.Version)
	if p.Expires < 30 && p.Expires >= 0 {
		fmt.Fprintln(os.Stderr, "warning: your license will expire soon")
		return
	}
}
