// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package admin

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// Config represents a Redpanda configuration. There are many keys returned, so
// the raw response is just unmarshaled into an interface. The expectation is
// that the client will just dump the response out as json.
type Config interface{}

// Config returns a single admin endpoint's configuration. This errors if
// multiple URLs are configured.
func (a *AdminAPI) Config() (Config, error) {
	var rawResp []byte
	err := a.sendOne(http.MethodGet, "/v1/config", nil, &rawResp)
	if err != nil {
		return nil, err
	}
	// The current config endpoint returns a json object wrapped in quotes.
	// We trim the quotes here, then unmarshal the object into an
	// interface{}. We do not expect to hit an unmarshal error here.
	rawResp = bytes.Trim(rawResp, `"`)
	var unmarshaled Config
	if err := json.Unmarshal(rawResp, &unmarshaled); err != nil {
		return nil, fmt.Errorf("unable to decode response body: %w", err)
	}
	return unmarshaled, nil
}

// SetLogLevel sets the logger level for the logger `name` to the given level
// for the single admin host in this client. This function will return an error
// if the client has multiple URLs configured.
//
// Expiry configures how long the log level override will persist. If zero,
// Redpanda will persist the override until it shuts down.
func (a *AdminAPI) SetLogLevel(name, level string, expirySeconds int) error {
	if expirySeconds < 0 {
		return fmt.Errorf("invalid negative expiry of %ds", expirySeconds)
	}
	switch level = strings.ToLower(level); level {
	case "error",
		"warn",
		"info",
		"debug",
		"trace":
	default:
		return fmt.Errorf("unknown logger level %q", level)
	}

	path := fmt.Sprintf("/v1/config/log_level/%s?level=%s&expires=%d", url.PathEscape(name), level, expirySeconds)
	return a.sendOne(http.MethodPut, path, nil, nil)
}
