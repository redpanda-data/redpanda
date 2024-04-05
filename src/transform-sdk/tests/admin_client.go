// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package integration_tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// AdminAPIClient is a wrapper around Redpanda's Admin API
type AdminAPIClient struct {
	baseUrl string
	client  *http.Client
}

// NewAdminAPIClient creates a new Admin API
func NewAdminAPIClient(baseURL string) *AdminAPIClient {
	return &AdminAPIClient{
		baseUrl: baseURL,
		client:  http.DefaultClient,
	}
}

// TransformDeployMetadata is the metadata that can be specified when deploying a transform.
type TransformDeployMetadata struct {
	// The name or ID of the transform.
	Name string `json:"name"`
	// The input topic to read data from.
	InputTopic string `json:"input_topic"`
	// The output topics that are writable for this data transform.
	OutputTopics []string `json:"output_topics"`
	// The environment variables made accessible to the running transform.
	Environment []EnvironmentVariable `json:"environment,omitempty"`
}

// DeployTransform deploys data transform to Redpanda.
//
// The InputTopic and OutputTopics for the transform must already exist before this deploy, and the binary must be a valid
// WebAssembly module compiled with Redpanda's Data Transform SDKs.
//
// See https://docs.redpanda.com/current/develop/data-transforms/ for more information about data transforms.
func (cl *AdminAPIClient) DeployTransform(ctx context.Context, metadata TransformDeployMetadata, binary io.Reader) error {
	jsonReq, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal create user request: %w", err)
	}

	endpoint, err := url.JoinPath(cl.baseUrl, "/v1/transform/deploy")
	if err != nil {
		return fmt.Errorf("failed to join url path: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, io.MultiReader(bytes.NewReader(jsonReq), binary))
	if err != nil {
		return fmt.Errorf("failed to build http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := cl.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	if err := checkResponse(resp); err != nil {
		return err
	}

	// Wait until the deployed artifact is running before marking it as a successful deploy.
	for i := 0; i < 10; i += 1 {
		transforms, err := cl.ListTransforms(ctx)
		if err != nil {
			return err
		}
		if isTransformsRunning(transforms, metadata.Name) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("deployed transform failed to run")
}

func isTransformsRunning(transforms []TransformMetadata, name string) bool {
	for _, t := range transforms {
		if t.Name != name {
			continue
		}
		for _, s := range t.Status {
			if s.Status != "running" {
				return false
			}
		}
		return true
	}
	return false
}

// TransformMetadata is the metadata for a live running transform on a cluster.
type TransformMetadata struct {
	Name         string                     `json:"name"`
	InputTopic   string                     `json:"input_topic"`
	OutputTopics []string                   `json:"output_topics"`
	Status       []PartitionTransformStatus `json:"status,omitempty"`
	Environment  []EnvironmentVariable      `json:"environment,omitempty"`
}

// EnvironmentVariable is a configuration key/value that can be injected into to a data transform.
type EnvironmentVariable struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// PartitionTransformStatus is the status of a single transform that is running on an input partition.
type PartitionTransformStatus struct {
	// The node on which this transform is processing records on.
	NodeID int `json:"node_id"`
	// The partition this transform is reading from.
	Partition int `json:"partition"`
	// Status is an enum of: ["running", "inactive", "errored", "unknown"].
	Status string `json:"status"`
	// The number of records behind the tip of the log this transform is.
	Lag int `json:"lag"`
}

// ListTransforms lists the active state of data transforms within Redpanda.
func (cl *AdminAPIClient) ListTransforms(ctx context.Context) ([]TransformMetadata, error) {
	endpoint, err := url.JoinPath(cl.baseUrl, "/v1/transform")
	if err != nil {
		return nil, fmt.Errorf("failed to join url path: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to build http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := cl.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	if err := checkResponse(resp); err != nil {
		return nil, err
	}
	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("unable to read response body: %w", err)
	}
	metadata := []TransformMetadata{}
	err = json.Unmarshal(buf, &metadata)
	return metadata, err
}

// DeleteTransform deletes a transform by a given name in Redpanda.
func (cl *AdminAPIClient) DeleteTransform(ctx context.Context, name string) error {
	endpoint, err := url.JoinPath(cl.baseUrl, "/v1/transform", url.PathEscape(name))
	if err != nil {
		return fmt.Errorf("failed to join url path: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return fmt.Errorf("failed to build http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := cl.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	return checkResponse(resp)
}

// SetLog level for a broker logger
func (cl *AdminAPIClient) SetLogLevel(ctx context.Context, logger string, level string) error {
	endpoint, err := url.JoinPath(cl.baseUrl, "/v1/config/log_level", url.PathEscape(logger))
	if err != nil {
		return fmt.Errorf("failed to join url path: %w", err)
	}
	endpoint += fmt.Sprintf("?level=%s", level)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, nil)
	if err != nil {
		return fmt.Errorf("failed to build http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := cl.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	return checkResponse(resp)
}

func checkResponse(resp *http.Response) error {
	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
		return fmt.Errorf("unexpected status code in response: %d. Response body is: %q", resp.StatusCode, body)
	}
	return nil
}
