package mcp

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	fz "io/fs"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	controlplanev1mcp "github.com/redpanda-data/common-go/proto/gen/go/redpanda/api/controlplane/v1/controlplanev1mcp"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/tidwall/sjson"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mcp",
		Short: "Manage Redpanda Cloud MCP server",
	}

	cmd.AddCommand(
		newStdioCommand(fs, p),
		newInstall(fs, p),
	)
	return cmd
}

func newInstall(fs afero.Fs, p *config.Params) *cobra.Command {
	var allowDelete bool
	var mcpClient string
	cmd := &cobra.Command{
		Use:   "install",
		Short: "Install Redpanda Cloud MCP server",
		Long: `Install Redpanda Cloud MCP server.
		
Only Claude Desktop is supported at this time.
Writes an mcpServer entry with name "redpandaCloud" into claude_desktop_config.json in Claude Desktop's config directory.`,
		Args: cobra.NoArgs,
		Run: func(_ *cobra.Command, _ []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "Failed to load rpk config: %w", err)

			if mcpClient != "claude" {
				out.Die("Unsupported client: %s", mcpClient)
			}

			configDir, err := os.UserConfigDir()
			out.MaybeDie(err, "failed to get user configuration directory: %w", err)
			file := filepath.Join(configDir, "Claude", "claude_desktop_config.json")

			fileMode := fz.FileMode(0o600)
			jsonStr := `{}`
			f, err := os.OpenFile(file, os.O_RDONLY, 0o600)
			if err != nil && !os.IsNotExist(err) {
				out.MaybeDie(err, "failed to open Claude Desktop config file: %v", err)
			}
			defer f.Close()

			if err == nil {
				stat, err := f.Stat()
				out.MaybeDie(err, "failed to stat Claude Desktop config file: %v", err)
				fileMode = stat.Mode()

				bytez, err := io.ReadAll(f)
				out.MaybeDie(err, "failed to read content of Claude Desktop config file: %v", err)
				jsonStr = string(bytez)
			}

			jsonStr, err = sjson.Set(jsonStr, "mcpServers.redpandaCloud.command", "rpk")
			out.MaybeDie(err, "failed to patch Claude Desktop config: %v", err)
			var mcpArgs []string

			if cfg.VirtualProfile().CloudEnvironment != "" {
				mcpArgs = append(mcpArgs, "-X")
				mcpArgs = append(mcpArgs, fmt.Sprintf("cloud_environment=%s", cfg.VirtualProfile().CloudEnvironment))
			}
			mcpArgs = append(mcpArgs, []string{
				"--config", filepath.Join(configDir, "rpk", "rpk.yaml"), "cloud", "mcp", "stdio",
			}...)
			if allowDelete {
				mcpArgs = append(mcpArgs, "--allow-delete")
			}
			jsonStr, err = sjson.Set(jsonStr, "mcpServers.redpandaCloud.args", mcpArgs)
			out.MaybeDie(err, "failed to patch Claude Desktop config: %v", err)

			tmpFile := file + ".bak"
			err = os.WriteFile(tmpFile, []byte(jsonStr), fileMode)
			out.MaybeDie(err, "failed to write file: %v", err)
			err = os.Rename(file+".bak", file)
			out.MaybeDie(err, "failed to rename: %v", err)
		},
	}
	cmd.Flags().BoolVarP(&allowDelete, "allow-delete", "", false, "Allow delete RPCs")
	cmd.Flags().StringVar(&mcpClient, "client", "claude", "Name of the MCP client to configure")
	cmd.MarkFlagRequired("client")
	cmd.RegisterFlagCompletionFunc("client", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"claude"}, cobra.ShellCompDirectiveDefault
	})
	return cmd
}

func newStdioCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var allowDelete bool
	cmd := &cobra.Command{
		Use:   "stdio",
		Short: `MCP stdio server. Can be used by MCP clients.`,
		Args:  cobra.NoArgs,
		Run: func(_ *cobra.Command, _ []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "Failed to load config: %w", err)

			// Start out with empty token, and use the maybeReloadToken function to update.
			cl := publicapi.NewCloudClientSet(cfg.DevOverrides().PublicAPIURL, "")
			var m sync.RWMutex
			var tokenOK bool

			maybeReloadToken := func() error {
				cfg, err := p.Load(fs)
				if err != nil {
					return err
				}

				var authVir *config.RpkCloudAuth
				yVir := cfg.VirtualRpkYaml()
				pVir := yVir.Profile(yVir.CurrentProfile)
				authVir = pVir.VirtualAuth()
				if authVir == nil {
					authVir = yVir.CurrentAuth() // must be non-nil; we always have a default virtual auth
				}

				extra := ""
				if e := cfg.VirtualProfile().CloudEnvironment; e != "" {
					extra = fmt.Sprintf(" -X cloud_environment=%s", e)
				}
				authToken := authVir.AuthToken
				acl := auth0.NewClient(cfg.DevOverrides())
				expired, err := oauth.ValidateToken(authToken, acl.Audience(), authVir.ClientID)
				if err != nil {
					m.Lock()
					tokenOK = false
					m.Unlock()
					return fmt.Errorf("failed to validate Cloud token. This may happen, if they never logged in to Redpand Cloud. However, their token is not just expired, we'd not see this error then. Instruct the user to run `rpk%s cloud login` to login to Redpanda Cloud. After running the command, they can ask to retry", extra)
				}
				if expired {
					m.Lock()
					tokenOK = false
					m.Unlock()
					return fmt.Errorf("Redpanda Cloud token is expired. Instruct the user to run `rpk%s cloud login` to obtain a fresh one. Afterwards, they can ask to retry", extra)
				}
				m.RLock()
				ok := tokenOK
				m.RUnlock()
				if !ok {
					cl.UpdateAuthToken(authToken)
					m.Lock()
					tokenOK = true
					m.Unlock()
					fmt.Fprintf(os.Stderr, "Loaded new token\n")
				}
				return nil
			}

			s := server.NewMCPServer(
				"Redpanda Cloud MCP",
				"beta",
				server.WithToolHandlerMiddleware(func(thf server.ToolHandlerFunc) server.ToolHandlerFunc {
					return func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
						if !allowDelete && strings.Contains(strings.ToLower(request.Params.Name), "delete") {
							return nil, fmt.Errorf("deletes are forbidden. Tell the user that they can enable deletes by running rpk cloud mcp install --allow-delete. This will permanently turn on deletes via MCP, which is off by default. Then, they must restart their MCP client. It is important that you tell them to restart their client. To turn it back off, they can run it without the --allow-delete flag")
						}
						if err := maybeReloadToken(); err != nil {
							return nil, err
						}

						return thf(ctx, request)
					}
				}),
			)

			// Controlplane
			controlplanev1mcp.ForwardToConnectRegionServiceClient(s, cl.Region)
			controlplanev1mcp.ForwardToConnectResourceGroupServiceClient(s, cl.ResourceGroup)
			controlplanev1mcp.ForwardToConnectClusterServiceClient(s, cl.Cluster)
			controlplanev1mcp.ForwardToConnectNetworkServiceClient(s, cl.Network)
			controlplanev1mcp.ForwardToConnectServerlessClusterServiceClient(s, cl.Serverless)
			controlplanev1mcp.ForwardToConnectServerlessRegionServiceClient(s, cl.ServerlessRegion)
			controlplanev1mcp.ForwardToConnectOperationServiceClient(s, cl.Operations)
			controlplanev1mcp.ForwardToConnectServerlessRegionServiceClient(s, cl.ServerlessRegion)
			controlplanev1mcp.ForwardToConnectServerlessRegionServiceClient(s, cl.ServerlessRegion)

			if err := server.ServeStdio(s); err != nil {
				fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
			}
		},
	}

	cmd.Flags().BoolVarP(&allowDelete, "allow-delete", "", false, "Allow delete RPCs. Off by default")
	return cmd
}
