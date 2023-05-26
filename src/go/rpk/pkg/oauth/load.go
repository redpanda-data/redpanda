package oauth

import (
	"context"
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/spf13/afero"
)

// LoadFlow loads or creates a config at default path, and validates and
// refreshes or creates an auth token using the given authentication provider.
//
// This function is expected to be called at the start of most commands, and it
// saves the token and client ID to the passed cloud config.
func LoadFlow(ctx context.Context, fs afero.Fs, cfg *config.Config, cl Client) (token string, err error) {
	// We want to avoid creating a root owned file. If the file exists, we
	// just chmod with rpkos.ReplaceFile and keep old perms even with sudo.
	// If the file does not exist, we will always be creating it to write
	// the token, so we fail if we are running with sudo.
	if _, ok := cfg.ActualRpkYaml(); ok && rpkos.IsRunningSudo() {
		return "", fmt.Errorf("detected rpk is running with sudo; please execute this command without sudo to avoid saving the cloud configuration as a root owned file")
	}

	yVir := cfg.VirtualRpkYaml()
	authVir := yVir.Auth(yVir.CurrentCloudAuth) // must exist

	var resp Token
	if authVir.HasClientCredentials() {
		resp, err = ClientCredentialFlow(ctx, cl, authVir)
	} else {
		resp, err = DeviceFlow(ctx, cl, authVir)
	}
	if err != nil {
		return "", fmt.Errorf("unable to retrieve a cloud token: %w", err)
	}

	// We want to update the actual auth.
	yAct, err := cfg.ActualRpkYamlOrEmpty()
	if err != nil {
		return "", err
	}
	authAct := yAct.Auth(yAct.CurrentCloudAuth)
	if authAct == nil {
		yAct.CurrentCloudAuth = yAct.PushAuth(config.DefaultRpkCloudAuth())
		authAct = yAct.Auth(yAct.CurrentCloudAuth)
	}
	authAct.ClientID = authVir.ClientID
	authAct.AuthToken = resp.AccessToken

	authVir.AuthToken = resp.AccessToken
	return resp.AccessToken, yAct.Write(fs)
}
