package auth0

import (
	"context"

	"github.com/pkg/browser"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloudapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
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
const RPKClientID = "ehNhbNUwjPIWUinE2b7njFyE4VBrxXZm"

// endpoint groups what url and audience to use for getting tokens.
type (
	endpoint struct {
		URL      string
		Audience string
	}

	// Client it's the auth0 implementation of the oauth interface.
	Client struct {
		httpCl       *httpapi.Client
		authClientID string
		cloudURL     string
		endpoint     endpoint
	}
)

func NewClient(overrides config.DevOverrides) *Client {
	opts := []httpapi.Opt{
		httpapi.Err4xx(func(code int) error { return &oauth.TokenResponseError{Code: code} }),
	}
	httpCl := httpapi.NewClient(opts...)

	cl := &Client{
		httpCl:       httpCl,
		cloudURL:     cloudapi.ProdURL, // Once we migrate to use Auth0 this must change to use AuthURL.
		authClientID: RPKClientID,
		endpoint:     prodAuth0Endpoint,
	}

	for _, override := range []struct {
		src string
		dst *string
	}{
		{overrides.CloudAPIURL, &cl.cloudURL},
		{overrides.CloudAuthAppClientID, &cl.authClientID},
		{overrides.CloudAuthURL, &cl.endpoint.URL},
		{overrides.CloudAuthAudience, &cl.endpoint.Audience},
	} {
		if override.src != "" {
			*override.dst = override.src
		}
	}

	return cl
}

func (*Client) URLOpener(url string) error {
	return browser.OpenURL(url)
}

func (cl *Client) AuthClientID() string {
	return cl.authClientID
}

func (cl *Client) Audience() string {
	return cl.endpoint.Audience
}

func (cl *Client) Token(ctx context.Context, clientID, clientSecret string) (oauth.Token, error) {
	return cl.getToken(ctx, cl.endpoint.URL, cl.endpoint.Audience, clientID, clientSecret)
}

// For all device code flow below we intentionally use the Cloud URL as the host
// while we migrate to Auth0.

func (cl *Client) DeviceCode(ctx context.Context) (oauth.DeviceCode, error) {
	return cl.initDeviceAuthorization(ctx, cl.cloudURL, cl.authClientID)
}

func (cl *Client) DeviceToken(ctx context.Context, deviceCode string) (oauth.Token, error) {
	return cl.getDeviceToken(ctx, cl.cloudURL, cl.authClientID, deviceCode)
}
