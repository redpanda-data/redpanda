// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package publicapi

import (
	"net/http"
	"net/url"
	"sync"
	"time"

	"buf.build/gen/go/redpandadata/ai-gateway/connectrpc/go/redpanda/api/aigateway/v1/aigatewayv1connect"
	"connectrpc.com/connect"
)

// AIGatewayClientSet holds the respective service clients to interact with
// the AI Gateway endpoints of the Public API.
type AIGatewayClientSet struct {
	AccessControl  aigatewayv1connect.AccessControlServiceClient
	Account        aigatewayv1connect.AccountServiceClient
	Analytics      aigatewayv1connect.AnalyticsServiceClient
	Audit          aigatewayv1connect.AuditServiceClient
	Auth           aigatewayv1connect.AuthServiceClient
	BackendPool    aigatewayv1connect.BackendPoolServiceClient
	Config         aigatewayv1connect.ConfigServiceClient
	Gateway        aigatewayv1connect.GatewayServiceClient
	GatewayConfig  aigatewayv1connect.GatewayConfigServiceClient
	Guardrail      aigatewayv1connect.GuardrailServiceClient
	IAMSettings    aigatewayv1connect.IAMSettingsServiceClient
	MCPTools       aigatewayv1connect.MCPToolsServiceClient
	ModelPricing   aigatewayv1connect.ModelPricingServiceClient
	ModelProviders aigatewayv1connect.ModelProvidersServiceClient
	Models         aigatewayv1connect.ModelsServiceClient
	OAuth2Client   aigatewayv1connect.OAuth2ClientServiceClient
	OAuth2Key      aigatewayv1connect.OAuth2KeyServiceClient
	Organization   aigatewayv1connect.OrganizationServiceClient
	ProviderConfig aigatewayv1connect.ProviderConfigServiceClient
	RateLimit      aigatewayv1connect.RateLimitServiceClient
	Role           aigatewayv1connect.RoleServiceClient
	Routing        aigatewayv1connect.RoutingServiceClient
	Settings       aigatewayv1connect.SettingsServiceClient
	SpendLimit     aigatewayv1connect.SpendLimitServiceClient
	SSO            aigatewayv1connect.SSOServiceClient
	Team           aigatewayv1connect.TeamServiceClient
	User           aigatewayv1connect.UserServiceClient
	VisualMetadata aigatewayv1connect.VisualMetadataServiceClient
	Workspace      aigatewayv1connect.WorkspaceServiceClient

	m         sync.RWMutex
	authToken string
}

type AIGatewayURLContextKey struct{}

type AIGatewayDynamicTransport struct {
	Base http.RoundTripper
}

func (t *AIGatewayDynamicTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if urlStr, ok := req.Context().Value(AIGatewayURLContextKey{}).(string); ok {
		u, err := url.Parse(urlStr)
		if err == nil {
			req.URL.Host = u.Host
			req.URL.Scheme = u.Scheme
			req.URL.Path = "/.redpanda/api" + req.URL.Path
		}
	}
	return t.Base.RoundTrip(req)
}

func (cl *AIGatewayClientSet) Token() string {
	cl.m.RLock()
	defer cl.m.RUnlock()
	return cl.authToken
}

func (cl *AIGatewayClientSet) UpdateAuthToken(authToken string) {
	cl.m.Lock()
	defer cl.m.Unlock()
	cl.authToken = authToken
}

// NewAIGatewayClientSet creates a Public API client set for AI Gateway services.
func NewAIGatewayClientSet(host, authToken string, opts ...connect.ClientOption) (*AIGatewayClientSet, error) {
	cl := &AIGatewayClientSet{}
	cl.authToken = authToken

	var authInterceptor connect.UnaryInterceptorFunc
	if host == "" {
		authInterceptor = newReloadingAuthInterceptor(cl.Token)
	} else {
		authInterceptor = newAuthInterceptor(authToken)
	}

	opts = append([]connect.ClientOption{
		connect.WithInterceptors(
			authInterceptor,
			newLoggerInterceptor(),
			newAgentInterceptor(defaultRpkUserAgent()),
		),
	}, opts...)

	var httpCl *http.Client
	if host == "" {
		httpCl = &http.Client{
			Timeout:   time.Second * 30,
			Transport: &AIGatewayDynamicTransport{Base: http.DefaultTransport},
		}
	} else {
		httpCl = http.DefaultClient
	}

	cl.AccessControl = aigatewayv1connect.NewAccessControlServiceClient(httpCl, host, opts...)
	cl.Account = aigatewayv1connect.NewAccountServiceClient(httpCl, host, opts...)
	cl.Analytics = aigatewayv1connect.NewAnalyticsServiceClient(httpCl, host, opts...)
	cl.Audit = aigatewayv1connect.NewAuditServiceClient(httpCl, host, opts...)
	cl.Auth = aigatewayv1connect.NewAuthServiceClient(httpCl, host, opts...)
	cl.BackendPool = aigatewayv1connect.NewBackendPoolServiceClient(httpCl, host, opts...)
	cl.Config = aigatewayv1connect.NewConfigServiceClient(httpCl, host, opts...)
	cl.Gateway = aigatewayv1connect.NewGatewayServiceClient(httpCl, host, opts...)
	cl.GatewayConfig = aigatewayv1connect.NewGatewayConfigServiceClient(httpCl, host, opts...)
	cl.Guardrail = aigatewayv1connect.NewGuardrailServiceClient(httpCl, host, opts...)
	cl.IAMSettings = aigatewayv1connect.NewIAMSettingsServiceClient(httpCl, host, opts...)
	cl.MCPTools = aigatewayv1connect.NewMCPToolsServiceClient(httpCl, host, opts...)
	cl.ModelPricing = aigatewayv1connect.NewModelPricingServiceClient(httpCl, host, opts...)
	cl.ModelProviders = aigatewayv1connect.NewModelProvidersServiceClient(httpCl, host, opts...)
	cl.Models = aigatewayv1connect.NewModelsServiceClient(httpCl, host, opts...)
	cl.OAuth2Client = aigatewayv1connect.NewOAuth2ClientServiceClient(httpCl, host, opts...)
	cl.OAuth2Key = aigatewayv1connect.NewOAuth2KeyServiceClient(httpCl, host, opts...)
	cl.Organization = aigatewayv1connect.NewOrganizationServiceClient(httpCl, host, opts...)
	cl.ProviderConfig = aigatewayv1connect.NewProviderConfigServiceClient(httpCl, host, opts...)
	cl.RateLimit = aigatewayv1connect.NewRateLimitServiceClient(httpCl, host, opts...)
	cl.Role = aigatewayv1connect.NewRoleServiceClient(httpCl, host, opts...)
	cl.Routing = aigatewayv1connect.NewRoutingServiceClient(httpCl, host, opts...)
	cl.Settings = aigatewayv1connect.NewSettingsServiceClient(httpCl, host, opts...)
	cl.SpendLimit = aigatewayv1connect.NewSpendLimitServiceClient(httpCl, host, opts...)
	cl.SSO = aigatewayv1connect.NewSSOServiceClient(httpCl, host, opts...)
	cl.Team = aigatewayv1connect.NewTeamServiceClient(httpCl, host, opts...)
	cl.User = aigatewayv1connect.NewUserServiceClient(httpCl, host, opts...)
	cl.VisualMetadata = aigatewayv1connect.NewVisualMetadataServiceClient(httpCl, host, opts...)
	cl.Workspace = aigatewayv1connect.NewWorkspaceServiceClient(httpCl, host, opts...)

	return cl, nil
}
