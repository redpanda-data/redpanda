package auth0

import (
	"context"

	"github.com/pkg/browser"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cloud/cloudcfg"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth"
)

/////////////////////////////////
// Auth0 Client Implementation //
/////////////////////////////////

// The auth0 endpoint information to get dev tokens from.
var prodAuth0Endpoint = endpoint{
	URL:      "https://auth.prd.cloud.redpanda.com",
	Audience: "cloudv2-production.redpanda.cloud",
}

// RPKClientID is the auth0 client ID, it is public and is safe to have it here.
var RPKClientID = "ehNhbNUwjPIWUinE2b7njFyE4VBrxXZm"

// endpoint groups what url and audience to use for getting tokens.
type (
	endpoint struct {
		URL      string
		Audience string
	}

	// Client it's the auth0 implementation of the oauth interface.
	Client struct {
		httpCl   *httpapi.Client
		endpoint endpoint
	}
)

func NewClient(cfg *cloudcfg.Config) *Client {
	opts := []httpapi.Opt{
		httpapi.Err4xx(func(code int) error { return &oauth.TokenResponseError{Code: code} }),
	}
	httpCl := httpapi.NewClient(opts...)

	if cfg.CloudURL == "" {
		cfg.CloudURL = "https://cloud-api.prd.cloud.redpanda.com" // Once we migrate to use Auth0 this must change to use AuthURL.
	}

	auth0Endpoint := endpoint{
		URL:      cfg.AuthURL,
		Audience: cfg.AuthAudience,
	}
	if auth0Endpoint.URL == "" && auth0Endpoint.Audience == "" {
		auth0Endpoint = prodAuth0Endpoint
		cfg.AuthURL = prodAuth0Endpoint.URL
		cfg.AuthAudience = prodAuth0Endpoint.Audience
	}

	if cfg.AuthClientID == "" {
		cfg.AuthClientID = RPKClientID
	}

	return &Client{
		httpCl:   httpCl,
		endpoint: auth0Endpoint,
	}
}

func (*Client) URLOpener(url string) error {
	return browser.OpenURL(url)
}

func (cl *Client) Audience() string {
	return cl.endpoint.Audience
}

func (cl *Client) Token(ctx context.Context, cfg cloudcfg.Config) (oauth.Token, error) {
	return cl.getToken(ctx, cfg.AuthURL, cfg.AuthAudience, cfg.ClientID, cfg.ClientSecret)
}

// For all device code flow below we intentionally use the Cloud URL as the host
// while we migrate to Auth0.

func (cl *Client) DeviceCode(ctx context.Context, cfg cloudcfg.Config) (oauth.DeviceCode, error) {
	return cl.initDeviceAuthorization(ctx, cfg.CloudURL, cfg.AuthClientID)
}

func (cl *Client) DeviceToken(ctx context.Context, cfg cloudcfg.Config, deviceCode string) (oauth.Token, error) {
	return cl.getDeviceToken(ctx, cfg.CloudURL, cfg.AuthClientID, deviceCode)
}
