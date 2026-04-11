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

	controlplanev1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1"
	dataplanev1alpha3 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1alpha3"
	"connectrpc.com/connect"
	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/redpanda-data/common-go/proto/gen/go/redpanda/api/aigateway/v1/aigatewayv1mcp"
	controlplanev1mcp "github.com/redpanda-data/common-go/proto/gen/go/redpanda/api/controlplane/v1/controlplanev1mcp"
	"github.com/redpanda-data/common-go/proto/gen/go/redpanda/api/dataplane/v1/dataplanev1mcp"
	"github.com/redpanda-data/common-go/proto/gen/go/redpanda/api/dataplane/v1alpha3/dataplanev1alpha3mcp"
	"github.com/redpanda-data/common-go/proto/gen/go/redpanda/api/iam/v1/iamv1mcp"
	"github.com/redpanda-data/protoc-gen-go-mcp/pkg/runtime"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/version"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/authtoken"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/osutil"
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
		newProxyCommand(fs, p),
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
		
Supports Claude Desktop and Claude Code.
Writes an mcpServer entry with name "redpandaCloud" into the appropriate config file.`,
		Args: cobra.NoArgs,
		Run: func(_ *cobra.Command, _ []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "failed to load rpk config: %w", err)

			args := []string{"cloud", "mcp", "stdio"}
			if allowDelete {
				args = append(args, "--allow-delete")
			}
			configFile, err := installMCPConfig(cfg, mcpClient, "redpandaCloud", args)
			out.MaybeDie(err, "failed to install MCP configuration: %v", err)
			fmt.Printf("Successfully installed Redpanda Cloud MCP server to %s.\n", makePathPretty(configFile))
		},
	}
	cmd.Flags().BoolVarP(&allowDelete, "allow-delete", "", false, "Allow delete RPCs")
	cmd.Flags().StringVar(&mcpClient, "client", "claude", "Name of the MCP client to configure")
	cmd.MarkFlagRequired("client")
	cmd.RegisterFlagCompletionFunc("client", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"claude", "claude-code"}, cobra.ShellCompDirectiveDefault
	})
	return cmd
}

// newStdioCommand will be called by an MCP client, and its responses are returned to the LLM, that synthesizes the response message.
func newStdioCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var allowDelete bool
	cmd := &cobra.Command{
		Use:   "stdio",
		Short: `MCP stdio server. Can be used by MCP clients.`,
		Args:  cobra.NoArgs,
		Run: func(_ *cobra.Command, _ []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "failed to load config: %w", err)

			// Start out with empty token, and use the maybeReloadToken function to update.
			cl := publicapi.NewCloudClientSet(cfg.DevOverrides().PublicAPIURL, "")

			// Create dataplane client set with dynamic transport for MCP
			dataplaneClientSet, err := publicapi.NewDataPlaneClientSet("", "")
			out.MaybeDie(err, "failed to create dataplane client set: %v", err)

			// Create AI Gateway client set with dynamic transport for MCP
			aiGatewayClientSet, err := publicapi.NewAIGatewayClientSet("", "")
			out.MaybeDie(err, "failed to create AI gateway client set: %v", err)
			var m sync.RWMutex
			var tokenOK bool

			maybeReloadToken := func() error {
				cfg, err := p.Load(fs)
				if err != nil {
					return err
				}

				var extra string
				if e := cfg.VirtualProfile().CloudEnvironment; e != "" {
					extra = fmt.Sprintf(" -X cloud_environment=%s", e)
				}
				authToken := cfg.VirtualProfile().CurrentAuth().AuthToken
				acl := auth0.NewClient(cfg.DevOverrides())
				expired, err := authtoken.ValidateToken(authToken, acl.Audience(), cfg.VirtualProfile().CurrentAuth().ClientID)
				if err != nil {
					m.Lock()
					tokenOK = false
					m.Unlock()
					return fmt.Errorf("failed to validate Cloud token. This may happen, if they never logged in to Redpand Cloud. However, their token is not just expired, we'd not see this error then. Instruct the user to run `rpk%s cloud login --no-profile` to login to Redpanda Cloud. After running the command, they can ask to retry", extra)
				}
				if expired {
					m.Lock()
					tokenOK = false
					m.Unlock()
					return fmt.Errorf("the Redpanda Cloud token is expired. Instruct the user to run `rpk%s cloud login --no-profile` to obtain a fresh one. Afterwards, they can ask to retry", extra)
				}
				m.RLock()
				ok := tokenOK
				m.RUnlock()
				if !ok {
					cl.UpdateAuthToken(authToken)
					dataplaneClientSet.UpdateAuthToken(authToken)
					aiGatewayClientSet.UpdateAuthToken(authToken)
					m.Lock()
					tokenOK = true
					m.Unlock()
				}
				return nil
			}

			s := server.NewMCPServer(
				"Redpanda Cloud MCP",
				version.Pretty(),
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

			// IAM
			iamv1mcp.ForwardToConnectOrganizationServiceClient(s, cl.Organization)
			iamv1mcp.ForwardToConnectPermissionServiceClient(s, cl.Permission)
			iamv1mcp.ForwardToConnectRoleServiceClient(s, cl.Role)
			iamv1mcp.ForwardToConnectRoleBindingServiceClient(s, cl.RoleBinding)
			iamv1mcp.ForwardToConnectServiceAccountServiceClient(s, cl.ServiceAccount)
			iamv1mcp.ForwardToConnectUserServiceClient(s, cl.IAMUser)
			iamv1mcp.ForwardToConnectUserInviteServiceClient(s, cl.UserInvite)

			// Dataplane
			urlOpt := runtime.WithExtraProperties(runtime.ExtraProperty{
				Name:        "dataplane_api_url",
				Description: "URL to connect to this dataplane. This URL can be found by calling GetCluster or GetServerlessCluster.",
				Required:    true,
				ContextKey:  publicapi.DataplaneAPIURLContextKey{},
			})

			dataplanev1mcp.ForwardToConnectTopicServiceClient(s, dataplaneClientSet.Topic, urlOpt)
			dataplanev1mcp.ForwardToConnectPipelineServiceClient(s, dataplaneClientSet.Pipeline, urlOpt)
			dataplanev1mcp.ForwardToConnectACLServiceClient(s, dataplaneClientSet.ACL, urlOpt)
			dataplanev1mcp.ForwardToConnectCloudStorageServiceClient(s, dataplaneClientSet.CloudStorage, urlOpt)
			dataplanev1mcp.ForwardToConnectQuotaServiceClient(s, dataplaneClientSet.Quota, urlOpt)
			dataplanev1mcp.ForwardToConnectSecretServiceClient(s, dataplaneClientSet.Secret, urlOpt)
			dataplanev1mcp.ForwardToConnectSecurityServiceClient(s, dataplaneClientSet.Security, urlOpt)
			dataplanev1mcp.ForwardToConnectTransformServiceClient(s, dataplaneClientSet.Transform, urlOpt)
			dataplanev1mcp.ForwardToConnectUserServiceClient(s, dataplaneClientSet.User, urlOpt)
			dataplanev1alpha3mcp.ForwardToConnectAIAgentServiceClient(s, dataplaneClientSet.AIAgent, urlOpt)
			dataplanev1alpha3mcp.ForwardToConnectKnowledgeBaseServiceClient(s, dataplaneClientSet.KnowledgeBase, urlOpt)
			dataplanev1alpha3mcp.ForwardToConnectMCPServerServiceClient(s, dataplaneClientSet.MCPServer, urlOpt)

			// AI Gateway
			aiGatewayURLOpt := runtime.WithExtraProperties(runtime.ExtraProperty{
				Name:        "ai_gateway_url",
				Description: "URL to connect to this AI Gateway instance.",
				Required:    true,
				ContextKey:  publicapi.AIGatewayURLContextKey{},
			})

			aigatewayv1mcp.ForwardToConnectAccessControlServiceClient(s, aiGatewayClientSet.AccessControl, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectAccountServiceClient(s, aiGatewayClientSet.Account, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectAnalyticsServiceClient(s, aiGatewayClientSet.Analytics, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectAuditServiceClient(s, aiGatewayClientSet.Audit, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectAuthServiceClient(s, aiGatewayClientSet.Auth, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectBackendPoolServiceClient(s, aiGatewayClientSet.BackendPool, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectConfigServiceClient(s, aiGatewayClientSet.Config, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectGatewayServiceClient(s, aiGatewayClientSet.Gateway, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectGatewayConfigServiceClient(s, aiGatewayClientSet.GatewayConfig, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectGuardrailServiceClient(s, aiGatewayClientSet.Guardrail, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectIAMSettingsServiceClient(s, aiGatewayClientSet.IAMSettings, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectMCPToolsServiceClient(s, aiGatewayClientSet.MCPTools, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectModelPricingServiceClient(s, aiGatewayClientSet.ModelPricing, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectModelProvidersServiceClient(s, aiGatewayClientSet.ModelProviders, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectModelsServiceClient(s, aiGatewayClientSet.Models, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectOAuth2ClientServiceClient(s, aiGatewayClientSet.OAuth2Client, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectOAuth2KeyServiceClient(s, aiGatewayClientSet.OAuth2Key, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectOrganizationServiceClient(s, aiGatewayClientSet.Organization, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectProviderConfigServiceClient(s, aiGatewayClientSet.ProviderConfig, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectRateLimitServiceClient(s, aiGatewayClientSet.RateLimit, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectRoleServiceClient(s, aiGatewayClientSet.Role, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectRoutingServiceClient(s, aiGatewayClientSet.Routing, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectSettingsServiceClient(s, aiGatewayClientSet.Settings, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectSpendLimitServiceClient(s, aiGatewayClientSet.SpendLimit, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectSSOServiceClient(s, aiGatewayClientSet.SSO, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectTeamServiceClient(s, aiGatewayClientSet.Team, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectUserServiceClient(s, aiGatewayClientSet.User, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectVisualMetadataServiceClient(s, aiGatewayClientSet.VisualMetadata, aiGatewayURLOpt)
			aigatewayv1mcp.ForwardToConnectWorkspaceServiceClient(s, aiGatewayClientSet.Workspace, aiGatewayURLOpt)

			if err := server.ServeStdio(s); err != nil {
				fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
			}
		},
	}

	cmd.Flags().BoolVarP(&allowDelete, "allow-delete", "", false, "Allow delete RPCs. Off by default")
	return cmd
}

// newProxyCommand will be called by an MCP client, and its responses are returned to the LLM, that synthesizes the response message.
func newProxyCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		clusterID           string
		serverlessClusterID string
		mcpServerID         string
		install             bool
		mcpClient           string
	)

	cmd := &cobra.Command{
		Use:   "proxy",
		Short: "Proxy requests to a remote Redpanda Cloud MCP server",
		Long: `Proxy MCP requests to a remote Redpanda Cloud MCP server.

This command connects to a specific cluster and MCP server running in that cluster,
then proxies MCP requests from stdio to the remote MCP server over HTTP.

Use --install to configure the MCP client instead of serving stdio.`,
		Args: cobra.NoArgs,
		PreRunE: func(_ *cobra.Command, _ []string) error {
			if install && mcpClient == "" {
				return fmt.Errorf("--client flag is required when using --install")
			}
			if clusterID == "" && serverlessClusterID == "" {
				return fmt.Errorf("must specify either --cluster-id or --serverless-cluster-id")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, _ []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "failed to load config: %w", err)

			authToken := cfg.VirtualProfile().CurrentAuth().AuthToken

			// Start out with empty token, and use the maybeReloadToken function to update.
			cl := publicapi.NewCloudClientSet(cfg.DevOverrides().PublicAPIURL, authToken)

			// Get cluster information and dataplane URL
			var dataplaneURL string
			var actualClusterID string
			if clusterID != "" {
				// Regular cluster
				cluster, err := cl.Cluster.GetCluster(cmd.Context(), connect.NewRequest(&controlplanev1.GetClusterRequest{
					Id: clusterID,
				}))
				out.MaybeDie(err, "failed to get cluster: %v", err)

				dataplaneURL = cluster.Msg.GetCluster().GetDataplaneApi().GetUrl()
				actualClusterID = clusterID
				if dataplaneURL == "" {
					out.Die("cluster %s does not have a dataplane API URL", clusterID)
				}
			} else {
				// Serverless cluster
				serverlessCluster, err := cl.Serverless.GetServerlessCluster(cmd.Context(), connect.NewRequest(&controlplanev1.GetServerlessClusterRequest{
					Id: serverlessClusterID,
				}))
				out.MaybeDie(err, "failed to get serverless cluster: %v", err)

				dataplaneURL = serverlessCluster.Msg.GetServerlessCluster().GetDataplaneApi().GetUrl()
				actualClusterID = serverlessClusterID
				if dataplaneURL == "" {
					out.Die("serverless cluster %s does not have a dataplane API URL", serverlessClusterID)
				}
			}

			// Create a dataplane client set for this specific dataplane
			dataplaneClient, err := publicapi.NewDataPlaneClientSet(dataplaneURL, authToken)
			out.MaybeDie(err, "failed to create dataplane client: %v", err)

			var m sync.RWMutex
			var tokenOK bool

			maybeReloadToken := func() error {
				cfg, err := p.Load(fs)
				if err != nil {
					return err
				}

				extra := ""
				if e := cfg.VirtualProfile().CloudEnvironment; e != "" {
					extra = fmt.Sprintf(" -X cloud_environment=%s", e)
				}

				acl := auth0.NewClient(cfg.DevOverrides())
				expired, err := authtoken.ValidateToken(authToken, acl.Audience(), cfg.VirtualProfile().CurrentAuth().ClientID)
				if err != nil {
					m.Lock()
					tokenOK = false
					m.Unlock()
					return fmt.Errorf("failed to validate Cloud token. This may happen, if they never logged in to Redpand Cloud. However, their token is not just expired, we'd not see this error then. Instruct the user to run `rpk%s cloud login --no-profile` to login to Redpanda Cloud. After running the command, they can ask to retry", extra)
				}
				if expired {
					m.Lock()
					tokenOK = false
					m.Unlock()
					return fmt.Errorf("the Redpanda Cloud token is expired. Instruct the user to run `rpk%s cloud login --no-profile` to obtain a fresh one. Afterwards, they can ask to retry", extra)
				}
				m.RLock()
				ok := tokenOK
				m.RUnlock()
				if !ok {
					m.Lock()
					authToken = cfg.VirtualProfile().CurrentAuth().AuthToken
					tokenOK = true
					m.Unlock()
				}
				return nil
			}

			// Get MCP server information
			mcpServer, err := dataplaneClient.MCPServer.GetMCPServer(cmd.Context(), connect.NewRequest(&dataplanev1alpha3.GetMCPServerRequest{
				Id: mcpServerID,
			}))
			out.MaybeDie(err, "failed to get MCP server: %v", err)

			mcpServerURL := mcpServer.Msg.GetMcpServer().GetUrl()
			if mcpServerURL == "" {
				out.Die("MCP server %s does not have a URL", mcpServerID)
			}

			// Get MCP server display name for configuration
			mcpServerName := mcpServer.Msg.GetMcpServer().GetDisplayName()
			if mcpServerName == "" {
				mcpServerName = mcpServerID // fallback to ID if no display name
			}

			// Handle install mode
			if install {
				args := []string{"cloud", "mcp", "proxy"}
				if clusterID != "" {
					args = append(args, "--cluster-id", clusterID)
				} else {
					args = append(args, "--serverless-cluster-id", serverlessClusterID)
				}
				args = append(args, "--mcp-server-id", mcpServerID)

				configFile, err := installMCPConfig(cfg, mcpClient, mcpServerName, args)
				out.MaybeDie(err, "failed to install MCP configuration: %v", err)
				fmt.Printf("Successfully installed MCP server for '%s' (cluster: %s, server: %s) to %s.\n", mcpServerName, actualClusterID, mcpServerID, makePathPretty(configFile))
				return
			}

			fmt.Fprintf(os.Stderr, "Proxying to MCP server %s at %s\n", mcpServerID, mcpServerURL)

			// Create streamable HTTP client connected to remote server with authentication
			remoteClient, err := client.NewStreamableHttpClient(mcpServerURL,
				transport.WithHTTPHeaderFunc(func(context.Context) map[string]string {
					m.Lock()
					defer m.Unlock()
					return map[string]string{
						"Authorization": fmt.Sprintf("Bearer %s", authToken),
					}
				}),
			)
			out.MaybeDie(err, "failed to create remote MCP client: %v", err)
			defer remoteClient.Close()

			// Initialize the remote client
			fmt.Fprintf(os.Stderr, "Starting remote MCP client\n")
			if err := remoteClient.Start(cmd.Context()); err != nil {
				out.MaybeDie(err, "failed to start remote MCP client: %v", err)
			}

			initRequest := mcp.InitializeRequest{}
			initRequest.Params.ProtocolVersion = mcp.LATEST_PROTOCOL_VERSION
			initRequest.Params.ClientInfo = mcp.Implementation{
				Name:    "rpk",
				Version: version.Pretty(),
			}
			initRequest.Params.Capabilities = mcp.ClientCapabilities{}

			fmt.Fprintf(os.Stderr, "Initializing remote MCP client\n")
			_, err = remoteClient.Initialize(cmd.Context(), initRequest)
			out.MaybeDie(err, "failed to initialize remote MCP client: %v", err)

			// Create local MCP server that will proxy to the remote client
			var clusterType string
			if clusterID != "" {
				clusterType = "cluster"
			} else {
				clusterType = "serverless cluster"
			}
			localServer := server.NewMCPServer(
				fmt.Sprintf("Redpanda Cloud MCP Proxy (%s: %s, server: %s)", clusterType, actualClusterID, mcpServerID),
				version.Pretty(),
				server.WithToolHandlerMiddleware(func(thf server.ToolHandlerFunc) server.ToolHandlerFunc {
					return func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
						if err := maybeReloadToken(); err != nil {
							return nil, err
						}
						return thf(ctx, request)
					}
				}),
			)

			// Add all tools from remote server to local server
			// Currently this is static and will not add/remove tools at runtime.
			if err := addRemoteToolsToServer(cmd.Context(), remoteClient, localServer); err != nil {
				out.MaybeDie(err, "failed to register remote tools: %v", err)
			}

			fmt.Fprintf(os.Stderr, "Proxy server ready, starting stdio server\n")

			if err := server.ServeStdio(localServer); err != nil {
				fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
			}
		},
	}

	cmd.Flags().StringVar(&clusterID, "cluster-id", "", "Cluster ID to connect to")
	cmd.Flags().StringVar(&serverlessClusterID, "serverless-cluster-id", "", "Serverless cluster ID to connect to")
	cmd.Flags().StringVar(&mcpServerID, "mcp-server-id", "", "MCP Server ID to proxy to")
	cmd.Flags().BoolVar(&install, "install", false, "Install MCP proxy configuration instead of serving stdio")
	cmd.Flags().StringVar(&mcpClient, "client", "", "Name of the MCP client to configure (required with --install)")
	cmd.MarkFlagRequired("mcp-server-id")
	cmd.MarkFlagsMutuallyExclusive("cluster-id", "serverless-cluster-id")
	cmd.RegisterFlagCompletionFunc("client", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"claude", "claude-code"}, cobra.ShellCompDirectiveDefault
	})
	return cmd
}

// makePathPretty replaces home directory with ~/.
func makePathPretty(path string) string {
	if homeDir, err := os.UserHomeDir(); err == nil {
		if strings.HasPrefix(path, homeDir) {
			return "~" + strings.TrimPrefix(path, homeDir)
		}
	}
	return path
}

// installMCPConfig installs MCP configuration for the specified client.
func installMCPConfig(cfg *config.Config, mcpClient, serverName string, args []string) (string, error) {
	var file string
	switch mcpClient {
	case "claude":
		configDir, err := os.UserConfigDir()
		if err != nil {
			return "", fmt.Errorf("failed to get user configuration directory: %w", err)
		}
		file = filepath.Join(configDir, "Claude", "claude_desktop_config.json")
	case "claude-code":
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("failed to get user home directory: %w", err)
		}
		file = filepath.Join(homeDir, ".claude.json")
	default:
		return "", fmt.Errorf("unsupported client: %s", mcpClient)
	}

	fileMode := fz.FileMode(0o600)
	jsonStr := `{}`
	f, err := os.OpenFile(file, os.O_RDONLY, 0o600)
	if err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("failed to open config file: %v", err)
	}
	defer f.Close()

	if err == nil {
		stat, err := f.Stat()
		if err != nil {
			return "", fmt.Errorf("failed to stat config file: %v", err)
		}
		fileMode = stat.Mode()

		bytez, err := io.ReadAll(f)
		if err != nil {
			return "", fmt.Errorf("failed to read content of config file: %v", err)
		}
		jsonStr = string(bytez)
	}

	// Set command
	serverKey := fmt.Sprintf("mcpServers.%s.command", serverName)
	jsonStr, err = sjson.Set(jsonStr, serverKey, "rpk")
	if err != nil {
		return "", fmt.Errorf("failed to patch config: %v", err)
	}

	// Build base args with cloud environment if needed
	var mcpArgs []string
	if cfg.VirtualProfile().CloudEnvironment != "" {
		mcpArgs = append(mcpArgs, "-X")
		mcpArgs = append(mcpArgs, fmt.Sprintf("cloud_environment=%s", cfg.VirtualProfile().CloudEnvironment))
	}

	rpkConfigPath, err := config.DefaultRpkYamlPath()
	if err != nil {
		return "", fmt.Errorf("failed to get rpk config path: %w", err)
	}
	mcpArgs = append(mcpArgs, "--config", rpkConfigPath)
	mcpArgs = append(mcpArgs, args...)

	// Set args
	argsKey := fmt.Sprintf("mcpServers.%s.args", serverName)
	jsonStr, err = sjson.Set(jsonStr, argsKey, mcpArgs)
	if err != nil {
		return "", fmt.Errorf("failed to patch config: %v", err)
	}

	err = rpkos.ReplaceFile(afero.NewOsFs(), file, []byte(jsonStr), fileMode)
	if err != nil {
		return "", fmt.Errorf("failed to write file: %v", err)
	}

	return file, nil
}

// addRemoteToolsToServer registers all tools from remote MCP server to local server.
func addRemoteToolsToServer(ctx context.Context, remoteClient *client.Client, localServer *server.MCPServer) error {
	toolsRequest := mcp.ListToolsRequest{}
	for {
		tools, err := remoteClient.ListTools(ctx, toolsRequest)
		if err != nil {
			return err
		}
		if len(tools.Tools) == 0 {
			break
		}
		fmt.Fprintf(os.Stderr, "Registering %d tools from remote server\n", len(tools.Tools))
		for _, tool := range tools.Tools {
			fmt.Fprintf(os.Stderr, "Registering tool: %s\n", tool.Name)
			localServer.AddTool(tool, remoteClient.CallTool)
		}
		if tools.NextCursor == "" {
			break
		}
		toolsRequest.Params.Cursor = tools.NextCursor
	}
	return nil
}
