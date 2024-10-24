package auth0

import (
	"context"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth"
)

func (cl *Client) getToken(ctx context.Context, host, audience, clientID, clientSecret string) (oauth.Token, error) {
	path := host + "/oauth/token"
	form := httpapi.Values(
		"grant_type", "client_credentials",
		"client_id", clientID,
		"client_secret", clientSecret,
		"audience", audience,
	)

	var token oauth.Token
	return token, cl.httpCl.PostForm(ctx, path, nil, form, &token)
}

func (cl *Client) initDeviceAuthorization(ctx context.Context, host, clientID string) (oauth.DeviceCode, error) {
	path := host + "/oauth/device/code"
	body := struct {
		ClientID string `json:"client_id"`
	}{clientID}

	var code oauth.DeviceCode
	return code, cl.httpCl.Post(ctx, path, nil, "application/json", body, &code)
}

func (cl *Client) getDeviceToken(ctx context.Context, host, authClientID, deviceCode string) (oauth.Token, error) {
	path := host + "/oauth/token"
	body := struct {
		ClientID   string `json:"client_id"`
		DeviceCode string `json:"device_code"`
		GrantType  string `json:"grant_type"`
	}{authClientID, deviceCode, "urn:ietf:params:oauth:grant-type:device_code"}

	var token oauth.Token
	return token, cl.httpCl.Post(ctx, path, nil, "application/json", body, &token)
}
