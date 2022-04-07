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
func (a *AdminAPI) Config() (Config, error) {
	var rawResp []byte
	err := a.sendAny(http.MethodGet, "/v1/config", nil, &rawResp)
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
	return a.sendOne(http.MethodPut, path, nil, nil, false)
}

type ConfigPropertyItems struct {
	Type string `json:"type"` // A swagger scalar type, like 'string', 'integer'
}

type ConfigPropertyMetadata struct {
	Type         string              `json:"type"`                  // Swagger type like 'string', 'integer', 'array'
	Description  string              `json:"description"`           // One liner human readable string
	Nullable     bool                `json:"nullable"`              // If true, may be null
	NeedsRestart bool                `json:"needs_restart"`         // If true, won't take effect until restart
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

func (a *AdminAPI) ClusterConfigSchema() (ConfigSchema, error) {
	var response ConfigSchemaResponse
	err := a.sendAny(http.MethodGet, "/v1/cluster_config/schema", nil, &response)
	if err != nil {
		return nil, err
	}

	return response.Properties, nil
}

type ClusterConfigWriteResult struct {
	ConfigVersion int `json:"config_version"`
}

func (a *AdminAPI) PatchClusterConfig(
	upsert map[string]interface{}, remove []string,
) (ClusterConfigWriteResult, error) {

	body := map[string]interface{}{
		"upsert": upsert,
		"remove": remove,
	}

	var result ClusterConfigWriteResult
	err := a.sendAny(http.MethodPut, "/v1/cluster_config", body, &result)
	if err != nil {
		return result, err
	}

	return result, nil
}

type ConfigStatus struct {
	NodeId        int64    `json:"node_id"`
	Restart       bool     `json:"restart"`
	ConfigVersion int64    `json:"config_version"`
	Invalid       []string `json:"invalid"`
	Unknown       []string `json:"unknown"`
}

type ConfigStatusResponse []ConfigStatus

func (a *AdminAPI) ClusterConfigStatus() (ConfigStatusResponse, error) {
	var result ConfigStatusResponse
	err := a.sendAny(http.MethodGet, "/v1/cluster_config/status", nil, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}
