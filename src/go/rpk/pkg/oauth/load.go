package oauth

import (
	"context"
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cloud/cloudcfg"
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/spf13/afero"
)

// LoadFlow loads or creates a config at default path, and validates and
// refreshes or creates an auth token using the given authentication provider.
//
// This function is expected to be called at the start of most commands, and it
// saves the token and client ID to the passed cloud config.
func LoadFlow(ctx context.Context, fs afero.Fs, cfg *cloudcfg.Config, cl Client) (token string, err error) {
	// We want to avoid creating a root owned file. If the file exists, we
	// just chmod with rpkos.ReplaceFile and keep old perms even with sudo.
	// If the file does not exist, we will always be creating it to write
	// the token, so we fail if we are running with sudo.
	if !cfg.Exists() && rpkos.IsRunningSudo() {
		return "", fmt.Errorf("detected rpk is running with sudo; please execute this command without sudo to avoid saving the cloud configuration as a root owned file")
	}

	var resp Token
	if cfg.HasClientCredentials() {
		resp, err = ClientCredentialFlow(ctx, cl, cfg)
	} else {
		resp, err = DeviceFlow(ctx, cl, cfg)
	}

	if err != nil {
		return "", fmt.Errorf("unable to retrieve a cloud token: %w", err)
	}
	cfg.AuthToken = resp.AccessToken
	return resp.AccessToken, cfg.SaveIDAndToken(fs)
}
