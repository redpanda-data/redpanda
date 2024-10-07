package license

import (
	"fmt"
	"os"
	"time"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type infoResponse struct {
	Organization string `json:"organization" yaml:"organization"`
	Type         string `json:"type" yaml:"type"`
	Expires      string `json:"expires" yaml:"expires"`
	ExpiresUnix  int64  `json:"expires_unix" yaml:"expires_unix"`
	Checksum     string `json:"checksum_sha256,omitempty" yaml:"checksum_sha256,omitempty"`
	Expired      bool   `json:"license_expired" yaml:"license_expired"`
}

func newInfoCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "info",
		Args:  cobra.ExactArgs(0),
		Short: "Retrieve license information",
		Long: `Retrieve license information:

    Organization:    Organization the license was generated for.
    Type:            Type of license: free, enterprise, etc.
    Expires:         Expiration date of the license
`,
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help(infoResponse{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			info, err := cl.GetLicenseInfo(cmd.Context())
			out.MaybeDie(err, "unable to retrieve license info: %v", err)
			if !info.Loaded {
				out.Die("this cluster is missing a license")
			}
			err = printLicenseInfo(f, info.Properties)
			out.MaybeDieErr(err)
		},
	}
	p.InstallFormatFlag(cmd)
	return cmd
}

func printLicenseInfo(f config.OutFormatter, props rpadmin.LicenseProperties) error {
	ut := time.Unix(props.Expires, 0)
	isExpired := ut.Before(time.Now())
	resp := infoResponse{
		Organization: props.Organization,
		Type:         props.Type,
		Expires:      ut.Format("Jan 2 2006"),
		ExpiresUnix:  props.Expires,
		Checksum:     props.Checksum,
		Expired:      isExpired,
	}
	if isText, _, formatted, err := f.Format(resp); !isText {
		if err != nil {
			return fmt.Errorf("unable to print license info in the required format %q: %v", f.Kind, err)
		}
		fmt.Println(formatted)
		return nil
	}

	out.Section("LICENSE INFORMATION")
	licenseFormat := `Organization:      %v
Type:              %v
Expires:           %v
`
	if isExpired {
		licenseFormat += "License Expired:   true\n"
	}
	fmt.Printf(licenseFormat, resp.Organization, resp.Type, resp.Expires)

	// Warn the user if the License is about to expire (<30 days left).
	diff := time.Until(ut)
	daysLeft := int(diff.Hours() / 24)
	if daysLeft < 30 && daysLeft >= 0 {
		fmt.Fprintln(os.Stderr, "warning: your license will expire soon")
	}
	return nil
}
