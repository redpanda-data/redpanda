package license

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newInfoCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var format string
	cmd := &cobra.Command{
		Use:   "info",
		Args:  cobra.ExactArgs(0),
		Short: "Retrieve license information",
		Long: `Retrieve license information:

    Organization:    Organization the license was generated for.
    Type:            Type of license: free, enterprise, etc.
    Expires:         Expiration date of the license
    Version:         License schema version.
`,
		Run: func(cmd *cobra.Command, args []string) {
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
					tm := time.Unix(info.Properties.Expires, 0).Format("Jan 2 2006")
					props, err := json.MarshalIndent(struct {
						Organization string `json:"organization"`
						Type         string `json:"type"`
						Expires      string `json:"expires"`
						Checksum     string `json:"checksum_sha256,omitempty"`
						Expired      bool   `json:"license_expired,omitempty"`
					}{info.Properties.Organization, info.Properties.Type, tm, info.Properties.Checksum, expired}, "", "  ")
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

	cmd.Flags().StringVar(&format, "format", "text", "Output format (text, json)")
	return cmd
}

func printLicenseInfo(p admin.LicenseProperties, expired bool) {
	out.Section("LICENSE INFORMATION")
	licenseFormat := `Organization:      %v
Type:              %v
Expires:           %v
`
	if expired {
		licenseFormat += `License Expired:   true
`
	}
	tm := time.Unix(p.Expires, 0)
	fmt.Printf(licenseFormat, p.Organization, p.Type, tm.Format("Jan 2 2006"))
	diff := time.Until(tm)
	daysLeft := int(diff.Hours() / 24)
	if daysLeft < 30 && daysLeft >= 0 {
		fmt.Fprintln(os.Stderr, "warning: your license will expire soon")
	}
}
