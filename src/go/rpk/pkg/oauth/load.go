package oauth

import (
	"context"
	"fmt"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloudapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

var inTests bool

// LoadFlow loads or creates a config at default path, and validates and
// refreshes or creates an auth token using the given authentication provider.
//
// This returns the actual auth, the virtual auth, and any error. If the virtual
// auth is able to be loaded, it is always returned even if there is an error.
//
// This function is expected to be called at the start of most commands, and it
// saves the token and client ID to the passed cloud config. This returns the
// *actual* currently selected auth.
func LoadFlow(ctx context.Context, fs afero.Fs, cfg *config.Config, cl Client, noUI, forceReload bool, cloudAPIURL string) (authAct, authVir *config.RpkCloudAuth, clearedProfile, isNewAuth bool, err error) {
	// We want to avoid creating a root owned file. If the file exists, we
	// just chmod with rpkos.ReplaceFile and keep old perms even with sudo.
	// If the file does not exist, we will always be creating it to write
	// the token, so we fail if we are running with sudo.
	if _, ok := cfg.ActualRpkYaml(); ok && rpkos.IsRunningSudo() {
		return nil, nil, false, false, fmt.Errorf("detected rpk is running with sudo; please execute this command without sudo to avoid saving the cloud configuration as a root owned file")
	}

	yVir := cfg.VirtualRpkYaml()

	// We try logging into the org specified in the current profile. If
	// the profile is not for a cloud cluster, we log into the default
	// auth.
	//
	// We use the virtual auth to ensure we pick up flags (--secrets).
	pVir := yVir.Profile(yVir.CurrentProfile)
	authVir = pVir.VirtualAuth()
	if authVir == nil {
		authVir = yVir.CurrentAuth() // must be non-nil; we always have a default virtual auth
	}

	var (
		tok        Token
		isNewToken bool
		authKind   string
	)

	if authVir.HasClientCredentials() {
		zap.L().Sugar().Debug("logging in using client credential flow")
		tok, isNewToken, err = ClientCredentialFlow(ctx, cl, authVir, forceReload)
		authKind = config.CloudAuthClientCredentials
	} else {
		zap.L().Sugar().Debug("logging in using OAUTH flow")
		tok, isNewToken, err = DeviceFlow(ctx, cl, authVir, noUI, forceReload)
		authKind = config.CloudAuthSSO
	}
	if err != nil {
		return nil, authVir, false, false, fmt.Errorf("unable to retrieve a cloud token: %w", err)
	}

	yAct, err := cfg.ActualRpkYamlOrEmpty()
	if err != nil {
		return nil, authVir, false, false, fmt.Errorf("unable to load your rpk.yaml file: %v", err)
	}

	var (
		org     cloudapi.Organization
		orgErr  error
		orgOnce bool
		getOrg  = func() (cloudapi.Organization, error) {
			if orgOnce {
				return org, orgErr
			}
			orgOnce = true

			if inTests {
				zap.L().Sugar().Debug("returning fake organization because cloudAPIURL is empty")
				return cloudapi.Organization{NameID: cloudapi.NameID{
					ID:   "no-url-org-id",
					Name: "no-url-org",
				}}, nil
			}

			org, orgErr = cloudapi.NewClient(cloudAPIURL, tok.AccessToken, httpapi.ReqTimeout(10*time.Second)).Organization(ctx)
			if orgErr != nil {
				return org, fmt.Errorf("unable to retrieve the organization for this token: %w", orgErr)
			}
			return org, orgErr
		}
	)

	// If this is a new token and we have a cloud profile, we need to
	// lookup the organization. The SSO flow may have logged into a
	// different org than the current auth and if so, we need to clear
	// the current profile.
	if pAuthAct := yAct.Profile(yAct.CurrentProfile).ActualAuth(); isNewToken && pAuthAct != nil {
		org, orgErr := getOrg()
		if orgErr != nil {
			return nil, authVir, false, false, orgErr
		}
		if org.ID != pAuthAct.OrgID || pAuthAct.Kind != authKind {
			clearedProfile = true
			yAct.CurrentProfile = ""
			yVir.CurrentProfile = ""
		}
	}

	loggedAuth := *authVir

	// If this is a new token, if we had no profile, but we previously did
	// log in, we need to check the same thing. For example, if we
	// previously logged in via client credentials and did not create a
	// profile, and now we're logging in again with new client credentials,
	// we need to clear the current auth so we push new auth below.
	if yAuthAct := yAct.CurrentAuth(); isNewToken && yAuthAct != nil {
		org, orgErr := getOrg()
		if orgErr != nil {
			return nil, authVir, false, false, orgErr
		}
		if org.ID != yAuthAct.OrgID || yAuthAct.Kind != authKind {
			yAct.CurrentCloudAuthOrgID = ""
			yVir.CurrentCloudAuthOrgID = ""
			yAct.CurrentCloudAuthKind = ""
			yVir.CurrentCloudAuthKind = ""
		}
	}

	// We want to update the actual auth.
	//
	// Failure checking:
	//
	// 0 Virtual profile has auth, actual profile does not: this is
	//   not possible. We do not allow creating an auth struct from
	//   flags (only client ID / secret), so any virtual auth must
	//   be from actual auth.
	//
	// 1 Happy path: profile auth == current auth. We just update the
	//   current token.
	//
	// 2 Profile has auth, but it is not the current auth. This should
	//   not happen because we switch auth whenever the user switches
	//   profiles. If we see this, maybe the file was updated manually.
	//   We use the profile auth.
	//
	// 3 Profile has auth and the auth does not exist: maybe the file
	//   was edited manually. ActualAuth returns nil here, so the outdated
	//   auth will be overwritten.
	//
	// 4 Profile has no auth, rpk.yaml has current auth: the user previously
	//   logged in and had a cloud profile, but then switched to a SH cluster.
	//   We log into the rpk.yaml auth.
	//
	// 5 Profile has no auth, rpk.yaml has no auth: either the token is for
	//   a new org and auth was cleared just above, or the user has never
	//   logged in. We create a new auth, lookup the org, and push this
	//   new auth as current auth.

	pAct := yAct.Profile(yAct.CurrentProfile)
	pAuthAct := pAct.ActualAuth()
	yAuthAct := yAct.CurrentAuth()

	if pAuthAct != nil {
		// SUMMARY: We make the current profile's auth the top level
		// yaml's current auth, and ensure we are logging into the
		// profile's auth.
		//
		// This is case 1 or 2, the logic is identical. We start with
		// some case 2 logic: we know the auth actually exists, so we
		// enforce it is the current rpk.yaml auth.
		yAct.MakeAuthCurrent(&pAuthAct)

		// Case 1 and 2: we check some invariants and then ensure the
		// virtual rpk.yaml also has the same current auth.
		pAuthVir := pVir.VirtualAuth()
		if pAuthVir == nil {
			panic("params invariant: virtual profile auth is nil even though actual profile auth is not")
		}
		if !pAuthVir.Equals(pAuthAct) {
			panic("params invariant: internal virtual auth name/org != actual name/org")
		}
		yVir.MakeAuthCurrent(&pAuthVir)

		// Finally, set authAct and authVir so they can be updated below.
		authAct = pAuthAct
		authVir = pAuthVir
	} else {
		// SUMMARY: Profile has no auth, or there is no auth at all in
		// the rpk yaml. We log in, creating a new auth if necessary.
		if pAct != nil {
			// Case 3. This profile refers to deleted auth somehow.
			// We will keep the cloud cluster name details, but the auth
			// name is now useless.
			//
			// We can leave the profile and warn of problems on startup
			// or via some lint command.
			pAct.CloudCluster.AuthOrgID = ""
			pAct.CloudCluster.AuthKind = ""
		}
		if yAuthAct == nil {
			// Case 5.
			yAct.PushNewAuth(config.DefaultRpkCloudAuth())
			yAuthAct = yAct.CurrentAuth()
			isNewAuth = true
		}
		// authVir started as yAuthVir, but could have been cleared above
		// if/when we cleared the yaml auth.
		//
		// If authVir was cleared, we need to reset the client ID and
		// secret to what we used to log in with.
		yAuthVir := yVir.CurrentAuth()
		if yAuthVir == nil {
			yVir.PushNewAuth(config.DefaultRpkCloudAuth())
			yAuthVir = yVir.CurrentAuth()
			if loggedAuth.ClientID != "" {
				yAuthVir.ClientID = loggedAuth.ClientID
			}
			if loggedAuth.ClientSecret != "" {
				yAuthVir.ClientSecret = loggedAuth.ClientSecret
			}
		}
		if !yAuthVir.Equals(yAuthAct) {
			panic("params invariant: virtual auth != actual auth")
		}
		authAct = yAuthAct
		authVir = yAuthVir
	}

	// If this is a new token, we need to lookup the organization. The SSO
	// flow may have logged into a different org than the current auth, and
	// if so, we need to push new auth AND swap away from the current
	// profile. We do this same thing for client credentials because it is
	// possible the user is passing flags to try swapping back to a prior
	// login.
	//
	// If this is a new auth entirely (case 5), we also look up the org.
	if isNewToken || isNewAuth {
		org, orgErr := getOrg()
		if orgErr != nil {
			return nil, authVir, false, false, orgErr
		}

		// If this is new default auth, we actually could have logged
		// into an auth that was already logged into in the past, but
		// is not the current auth. If this is the case, we delete our
		// newly pushed current auth and instead make that old auth the
		// current auth.
		if isNewAuth {
			check := func(y *config.RpkYaml, newAuth **config.RpkCloudAuth) {
				var dropped bool
				for i := range y.CloudAuths {
					a := &y.CloudAuths[i]
					if a.OrgID == org.ID && a.Kind == authKind {
						y.DropAuth(*newAuth)
						dropped = true
						break
					}
				}
				// If we found old auth, we dropped our new auth.
				// Dropping auth shifts the array, so we re-lookup
				// where the auth is.
				if !dropped {
					return
				}
				for i := range y.CloudAuths {
					a := &y.CloudAuths[i]
					if a.OrgID == org.ID && a.Kind == authKind {
						*newAuth = a
						y.MakeAuthCurrent(newAuth)
						return
					}
				}
			}
			check(yAct, &authAct)
			check(yVir, &authVir)
		}
		// We always write the auth name / org / orgID and update the
		// current auth. This is a no-op if check just above did stuff.

		authVir.Kind = authKind
		authAct.Kind = authKind

		authVir.Organization = org.Name
		authAct.Organization = org.Name

		authVir.OrgID = org.ID
		authAct.OrgID = org.ID

		name := fmt.Sprintf("%s-%s %s", authVir.OrgID, authVir.Kind, authVir.Organization)

		authVir.Name = name
		authAct.Name = name

		yVir.CurrentCloudAuthOrgID = org.ID
		yAct.CurrentCloudAuthOrgID = org.ID

		yVir.CurrentCloudAuthKind = authVir.Kind
		yAct.CurrentCloudAuthKind = authVir.Kind // we use the virtual kind here -- clientID is updated below
	}

	// We avoid copying the client secret, but we do keep the client ID.
	authVir.AuthToken = tok.AccessToken
	authAct.AuthToken = tok.AccessToken
	authAct.ClientID = authVir.ClientID

	return authAct, authVir, clearedProfile, isNewAuth, yAct.Write(fs)
}

// PrintSwapMessage prints a message to the user about swapping away from a
// profile due to a new login.
func PrintSwapMessage(priorProfile *config.RpkProfile, newAuth *config.RpkCloudAuth) {
	fmt.Printf("You are now logged into organization %q (%s).", newAuth.Organization, newAuth.OrgID)
	priorAuth := priorProfile.ActualAuth()
	fmt.Printf("rpk swapped away from your prior profile %q because that profile authenticates\nwith organization %q (%s).\n", priorProfile.Name, priorAuth.Organization, priorAuth.OrgID)
}

// MaybePrintSwapMessage prints a message to the user about swapping away from a
// profile due to a new login, if the profile was cleared.
func MaybePrintSwapMessage(clearedProfile bool, priorProfile *config.RpkProfile, newAuth *config.RpkCloudAuth) {
	if clearedProfile {
		PrintSwapMessage(priorProfile, newAuth)
	}
}
