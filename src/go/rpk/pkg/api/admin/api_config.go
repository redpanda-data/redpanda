// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// Config represents a Redpanda configuration. There are many keys returned, so
// the raw response is just unmarshaled into an interface.
type Config map[string]interface{}

// Config returns a single admin endpoint's configuration. This errors if
// multiple URLs are configured.
//
// If includeDefaults is true, all properties are returned, including those
// that are simply reporting their defaults.  Otherwise, only properties with
// non-default values are included (i.e. those which have been set at some
// point).
func (a *AdminAPI) Config(ctx context.Context, includeDefaults bool) (Config, error) {
	var rawResp []byte
	err := a.sendAny(ctx, http.MethodGet, fmt.Sprintf("/v1/cluster_config?include_defaults=%t", includeDefaults), nil, &rawResp)
	if err != nil {
		return nil, err
	}
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
func (a *AdminAPI) SetLogLevel(ctx context.Context, name, level string, expirySeconds int) error {
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
	return a.sendOne(ctx, http.MethodPut, path, nil, nil, false)
}

type ConfigPropertyItems struct {
	Type string `json:"type"` // A swagger scalar type, like 'string', 'integer'
}

type ConfigPropertyMetadata struct {
	Type         string              `json:"type"`                  // Swagger type like 'string', 'integer', 'array'
	Description  string              `json:"description"`           // One liner human readable string
	Nullable     bool                `json:"nullable"`              // If true, may be null
	NeedsRestart bool                `json:"needs_restart"`         // If true, won't take effect until restart
	IsSecret     bool                `json:"is_secret"`             // If true, the field should be redacted.
	Visibility   string              `json:"visibility"`            // One of 'user', 'deprecated', 'tunable'
	Units        string              `json:"units,omitempty"`       // A unit like 'ms', or empty.
	Example      string              `json:"example,omitempty"`     // A non-default value for use in docs or tests
	EnumValues   []string            `json:"enum_values,omitempty"` // Permitted values, or empty list.
	Items        ConfigPropertyItems `json:"items,omitempty"`       // If this is an array, the contained value type
}

type ConfigSchema map[string]ConfigPropertyMetadata

type ConfigSchemaResponse struct {
	Properties ConfigSchema `json:"properties"`
}

func (a *AdminAPI) ClusterConfigSchema(ctx context.Context) (ConfigSchema, error) {
	var response ConfigSchemaResponse
	err := a.sendAny(ctx, http.MethodGet, "/v1/cluster_config/schema", nil, &response)
	if err != nil {
		return nil, err
	}

	return response.Properties, nil
}

type ClusterConfigWriteResult struct {
	ConfigVersion int `json:"config_version"`
}

func (a *AdminAPI) PatchClusterConfig(
	ctx context.Context, upsert map[string]interface{}, remove []string,
) (ClusterConfigWriteResult, error) {
	body := map[string]interface{}{
		"upsert": upsert,
		"remove": remove,
	}

	var result ClusterConfigWriteResult
	err := a.sendToLeader(ctx, http.MethodPut, "/v1/cluster_config", body, &result)
	if err != nil {
		return result, err
	}

	return result, nil
}

type ConfigStatus struct {
	NodeID        int64    `json:"node_id"`
	Restart       bool     `json:"restart"`
	ConfigVersion int64    `json:"config_version"`
	Invalid       []string `json:"invalid"`
	Unknown       []string `json:"unknown"`
}

type ConfigStatusResponse []ConfigStatus

func (a *AdminAPI) ClusterConfigStatus(ctx context.Context, sendToLeader bool) (ConfigStatusResponse, error) {
	var result ConfigStatusResponse
	var err error
	path := "/v1/cluster_config/status"
	if sendToLeader {
		err = a.sendToLeader(ctx, http.MethodGet, path, nil, &result)
	} else {
		err = a.sendAny(ctx, http.MethodGet, path, nil, &result)
	}
	if err != nil {
		return nil, err
	}

	return result, nil
}
