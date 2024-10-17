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
	"go.uber.org/zap"
)

type infoResponse struct {
	LicenseStatus      string   `json:"license_status" yaml:"license_status"`
	Organization       string   `json:"organization,omitempty" yaml:"organization,omitempty"`
	Type               string   `json:"type,omitempty" yaml:"type,omitempty"`
	Expires            string   `json:"expires,omitempty" yaml:"expires,omitempty"`
	ExpiresUnix        int64    `json:"expires_unix,omitempty" yaml:"expires_unix,omitempty"`
	Checksum           string   `json:"checksum_sha256,omitempty" yaml:"checksum_sha256,omitempty"`
	Expired            *bool    `json:"license_expired,omitempty" yaml:"license_expired,omitempty"`
	Violation          bool     `json:"license_violation" yaml:"license_violation"`
	EnterpriseFeatures []string `json:"enterprise_features_in_use" yaml:"enterprise_features_in_use"`
}

func newInfoCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "info",
		Aliases: []string{"status"},
		Args:    cobra.ExactArgs(0),
		Short:   "Retrieve license information",
		Long: `Retrieve license information:

    Organization:    Organization the license was generated for.
    Type:            Type of license: free, enterprise, etc.
    Expires:         Expiration date of the license.
    License Status:  Status of the loaded license (valid, expired, not_present).
    Violation:       Whether the cluster is using enterprise features without
                     a valid license.
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

			features, err := cl.GetEnterpriseFeatures(cmd.Context())
			if err != nil {
				zap.L().Sugar().Warnf("unable to retrieve enterprise features: %v; information will be incomplete; is Redpanda up to date?", err)
			}
			err = printLicenseInfo(f, info, &features)
			out.MaybeDieErr(err)
		},
	}
	p.InstallFormatFlag(cmd)
	return cmd
}

func printLicenseInfo(f config.OutFormatter, license rpadmin.License, features *rpadmin.EnterpriseFeaturesResponse) error {
	resp := buildInfoResponse(license, features)
	if isText, _, formatted, err := f.Format(resp); !isText {
		if err != nil {
			return fmt.Errorf("unable to print license info in the required format %q: %v", f.Kind, err)
		}
		fmt.Println(formatted)
		return nil
	}
	printTextLicenseInfo(resp)
	return nil
}

func buildInfoResponse(license rpadmin.License, features *rpadmin.EnterpriseFeaturesResponse) infoResponse {
	var resp infoResponse
	if license.Loaded {
		resp = buildLicenseProperties(license)
	}
	if features != nil {
		resp = buildFeatureViolations(resp, features)
	}
	return resp
}

func buildLicenseProperties(license rpadmin.License) infoResponse {
	ut := time.Unix(license.Properties.Expires, 0)
	isExpired := ut.Before(time.Now())
	return infoResponse{
		Organization: license.Properties.Organization,
		Type:         license.Properties.Type,
		Expires:      ut.Format("Jan 2 2006"),
		ExpiresUnix:  license.Properties.Expires,
		Checksum:     license.Properties.Checksum,
		Expired:      &isExpired,
	}
}

func buildFeatureViolations(resp infoResponse, features *rpadmin.EnterpriseFeaturesResponse) infoResponse {
	resp.Violation = features.Violation
	resp.LicenseStatus = string(features.LicenseStatus)
	resp.EnterpriseFeatures = []string{}
	for _, feat := range features.Features {
		if feat.Enabled {
			resp.EnterpriseFeatures = append(resp.EnterpriseFeatures, feat.Name)
		}
	}
	return resp
}

func printTextLicenseInfo(resp infoResponse) {
	tw := out.NewTable()
	if resp.LicenseStatus != "" {
		tw.Print("License status:", resp.LicenseStatus)
		tw.Print("License violation:", resp.Violation)
	}
	if len(resp.EnterpriseFeatures) > 0 {
		tw.Print("Enterprise features in use:", resp.EnterpriseFeatures)
	}
	if resp.Organization != "" {
		tw.Print("Organization:", resp.Organization)
		tw.Print("Type:", resp.Type)
		tw.Print("Expires:", resp.Expires)
		if *resp.Expired {
			tw.Print("License expired:", *resp.Expired)
		}
		checkLicenseExpiry(resp.ExpiresUnix)
	}
	out.Section("LICENSE INFORMATION")
	tw.Flush()
}

func checkLicenseExpiry(expiresUnix int64) {
	ut := time.Unix(expiresUnix, 0)
	daysLeft := int(time.Until(ut).Hours() / 24)

	if daysLeft < 30 && !ut.Before(time.Now()) {
		fmt.Fprintf(os.Stderr, "WARNING: your license will expire soon.\n\n")
	}
}
