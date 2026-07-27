// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package bundle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
)

const servicePath = "/oxla.admin.v1.DebugService/"

// Auth carries the credentials sent on every request. Bearer wins over Basic
// when both are set.
type Auth struct {
	Bearer string
	User   string
	Pass   string
}

// ConnectError is the Connect unary error envelope returned on non-2xx: a JSON
// body of {"code","message"}. httpStatus is retained for cases where the body is
// absent or not a Connect envelope (e.g. a 404 from the router).
type ConnectError struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	httpStatus int
}

func (e *ConnectError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("connect error (%s): %s", e.Code, e.Message)
	}
	if e.Message != "" {
		return fmt.Sprintf("http %d: %s", e.httpStatus, e.Message)
	}
	return fmt.Sprintf("http %d", e.httpStatus)
}

type Client struct {
	cl *httpapi.Client
}

func NewClient(baseURL string, hc *http.Client, auth Auth) *Client {
	opts := []httpapi.Opt{
		httpapi.Host(baseURL),
		httpapi.HTTPClient(hc),
		// One attempt per RPC: a failing or unreachable node is recorded and
		// the bundle moves on, rather than stalling on retry backoff.
		httpapi.Retries(0),
		httpapi.Headers("Connect-Protocol-Version", "1"),
		// Connect returns its {"code","message"} envelope alongside a mapped
		// HTTP status; decode 4xx bodies into it for a useful error string.
		httpapi.Err4xx(func(status int) error { return &ConnectError{httpStatus: status} }),
	}
	switch {
	case auth.Bearer != "":
		opts = append(opts, httpapi.BearerAuth(auth.Bearer))
	case auth.User != "" || auth.Pass != "":
		opts = append(opts, httpapi.BasicAuth(auth.User, auth.Pass))
	}
	return &Client{cl: httpapi.NewClient(opts...)}
}

// CallRaw invokes one unary method and returns the response body as raw JSON.
// req is marshaled as the request message; pass emptyRequest for no-field
// requests.
func (c *Client) CallRaw(ctx context.Context, method string, req any) (json.RawMessage, error) {
	var raw []byte
	if err := c.cl.Post(ctx, servicePath+method, nil, "application/json", req, &raw); err != nil {
		return nil, connectErr(err)
	}
	return json.RawMessage(raw), nil
}

// Call invokes a method and unmarshals the response into out.
func (c *Client) Call(ctx context.Context, method string, req, out any) error {
	raw, err := c.CallRaw(ctx, method, req)
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("%s: decode response: %w", method, err)
	}
	return nil
}

// connectErr normalizes a non-4xx httpapi status error (e.g. a 5xx Connect
// error) into a ConnectError so recorded messages stay uniform; 4xx bodies
// already arrive as a ConnectError via the Err4xx hook.
func connectErr(err error) error {
	var be *httpapi.BodyError
	if errors.As(err, &be) {
		return &ConnectError{httpStatus: be.StatusCode}
	}
	return err
}

// emptyRequest marshals to `{}`, the body for no-field request messages.
var emptyRequest = struct{}{}
