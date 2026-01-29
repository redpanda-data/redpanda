// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package profile

import (
	"fmt"
	"os"

	"github.com/fatih/color"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/authtoken"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type validationStatus string

const (
	statusOK      validationStatus = "ok"
	statusWarning validationStatus = "warning"
	statusError   validationStatus = "error"
)

type validationResult struct {
	check   string
	status  validationStatus
	message string
}

func newValidateCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate [PROFILE_NAME]",
		Short: "Validate configuration and detect common issues",
		Long: `Validate profile configuration and detect common issues.

This command checks the current profile (or a specified profile) for:

  - Cloud settings: Whether from_cloud matches the broker URLs
  - Auth reference: Whether the profile references a valid authentication
  - Auth kind: Whether the authentication type is correctly configured
  - Auth token: Whether the token exists and is not expired
`,
		Example: `
Validate the current profile:
  rpk profile validate

Validate a specific profile:
  rpk profile validate my-profile
`,
		Args: cobra.MaximumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			y, ok := cfg.ActualRpkYaml()
			if !ok {
				out.Die("No rpk.yaml configuration file found")
			}

			// Determine which profile to validate
			var profileName string
			if len(args) > 0 {
				profileName = args[0]
			} else {
				profileName = y.CurrentProfile
			}

			if profileName == "" {
				out.Die("No profile specified and no current profile set")
			}

			profile := y.Profile(profileName)
			if profile == nil {
				out.Die("Profile %q not found", profileName)
			}

			auth := profile.ActualAuth()
			checkers := []profileValidator{
				cloudProfileValidator{profile: profile},
				authReferenceValidator{profile: profile, auth: auth},
				authKindValidator{auth: auth},
				tokenValidator{auth: auth, cfg: cfg},
			}

			var results []validationResult
			for _, checker := range checkers {
				if checker.ShouldRun() {
					results = append(results, checker.Validate())
				}
			}

			// Print results table
			green := color.New(color.FgGreen).SprintFunc()
			yellow := color.New(color.FgYellow).SprintFunc()
			red := color.New(color.FgRed).SprintFunc()

			tw := out.NewTabWriter()
			tw.Print("CHECK", "STATUS", "MESSAGE")

			var anyErr bool
			for _, r := range results {
				var statusStr string
				switch r.status {
				case statusOK:
					statusStr = green("OK")
				case statusWarning:
					statusStr = yellow("WARNING")
				case statusError:
					statusStr = red("ERROR")
					anyErr = true
				}
				tw.Print(r.check, statusStr, r.message)
			}
			tw.Flush()
			if anyErr {
				os.Exit(1)
			}
		},
	}

	return cmd
}

// profileValidator validates a specific aspect of the profile configuration.
type profileValidator interface {
	ShouldRun() bool
	Validate() validationResult
}

type cloudProfileValidator struct {
	profile *config.RpkProfile
}

func (cloudProfileValidator) ShouldRun() bool {
	return true
}

func (c cloudProfileValidator) Validate() validationResult {
	isLikelyCloud := config.IsLikelyCloudCluster(c.profile)
	vr := validationResult{
		check:  "Cloud Settings",
		status: statusOK,
	}
	if c.profile.FromCloud && !isLikelyCloud {
		vr.status = statusWarning
		vr.message = "Detected Cloud Profile but URLs don't look like Redpanda Cloud; consider 'rpk profile edit'"
	}
	if !c.profile.FromCloud {
		if isLikelyCloud {
			vr.status = statusWarning
			vr.message = "URLs look like Redpanda Cloud but from_cloud is set to false; run 'rpk profile set from_cloud=true'"
		} else {
			vr.message = "Not a Redpanda Cloud profile (skipped)"
		}
	}
	return vr
}

type authReferenceValidator struct {
	profile *config.RpkProfile
	auth    *config.RpkCloudAuth
}

func (a authReferenceValidator) ShouldRun() bool {
	return a.profile.FromCloud
}

func (a authReferenceValidator) Validate() validationResult {
	vr := validationResult{
		check:  "Auth Reference",
		status: statusOK,
	}
	// This check is redundant with ShouldRun but kept for defensive clarity
	// since the validation logic depends on FromCloud being true.
	if !a.profile.FromCloud {
		vr.message = "Not a cloud profile"
		return vr
	}
	if a.profile.CloudCluster.AuthOrgID == "" || a.profile.CloudCluster.AuthKind == "" {
		vr.status = statusError
		vr.message = "No auth configured; run 'rpk cloud login' and recreate the profile"
		return vr
	}
	if a.auth == nil {
		vr.status = statusError
		vr.message = fmt.Sprintf("This profile references non-existent auth (org: %s, kind: %s); run 'rpk cloud login' and recreate the profile", a.profile.CloudCluster.AuthOrgID, a.profile.CloudCluster.AuthKind)
		return vr
	}
	return vr
}

type authKindValidator struct {
	auth *config.RpkCloudAuth
}

func (c authKindValidator) ShouldRun() bool {
	return c.auth != nil
}

func (c authKindValidator) Validate() validationResult {
	if c.auth.Kind == config.CloudAuthSSO && c.auth.HasClientCredentials() {
		return validationResult{
			check:   "Auth kind",
			status:  statusWarning,
			message: "Kind is 'sso' but client credentials present; consider fixing auth_kind",
		}
	}
	if c.auth.Kind == config.CloudAuthClientCredentials {
		if c.auth.ClientID == "" && c.auth.ClientSecret == "" {
			return validationResult{
				check:   "Auth kind",
				status:  statusWarning,
				message: "No client credentials stored; next login will require both client ID and secret",
			}
		}
		if c.auth.ClientID != "" && c.auth.ClientSecret == "" {
			return validationResult{
				check:   "Auth kind",
				status:  statusWarning,
				message: "Client Secret not stored; token refresh requires re-auth; use --save flag",
			}
		}
	}
	return validationResult{
		check:  "Auth kind",
		status: statusOK,
	}
}

type tokenValidator struct {
	auth *config.RpkCloudAuth
	cfg  *config.Config
}

func (c tokenValidator) ShouldRun() bool {
	return c.auth != nil
}

func (c tokenValidator) Validate() validationResult {
	vr := validationResult{
		check:  "Auth Token",
		status: statusOK,
	}
	if c.auth.AuthToken == "" {
		vr.status = statusWarning
		vr.message = "No token found; run 'rpk cloud login'"
		return vr
	}
	cl := auth0.NewClient(c.cfg.DevOverrides())
	expired, err := authtoken.ValidateToken(c.auth.AuthToken, cl.Audience(), c.auth.ClientID)
	if err != nil {
		vr.status = statusWarning
		vr.message = fmt.Sprintf("Unable to validate: %v", err)
		return vr
	}
	if expired {
		vr.status = statusError
		vr.message = "Expired; run 'rpk cloud login' to refresh"
		return vr
	}
	return vr
}
