// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/redpanda-data/common-go/rpadmin"
)

// testCatalogPath is the ConnectRPC unary URL for the TestCatalog RPC on
// the DatalakeService. We hit it directly rather than via a typed
// connectrpc-go client because the Go bindings for this RPC are only
// published to BSR after the proto change is merged.
const testCatalogPath = "/redpanda.core.admin.internal.datalake.v1.DatalakeService/TestCatalog"

// testCatalogRequest mirrors proto::admin::test_catalog_request.
type testCatalogRequest struct {
	PropertyOverrides map[string]string `json:"propertyOverrides,omitempty"`
}

// testCatalogResponse mirrors proto::admin::test_catalog_response.
//
// catalog_describe_error_code is the iceberg::catalog_errc enum name on
// failure (or "invalid_request" if the request was rejected before
// probing), or empty on success.
type testCatalogResponse struct {
	CatalogDescribeErrorCode    string `json:"catalogDescribeErrorCode,omitempty"`
	CatalogDescribeErrorMessage string `json:"catalogDescribeErrorMessage,omitempty"`
}

// invokeTestCatalog sends a TestCatalog ConnectRPC unary request to the
// admin API.
func invokeTestCatalog(ctx context.Context, cl *rpadmin.AdminAPI, overrides map[string]string) (*testCatalogResponse, error) {
	body := testCatalogRequest{PropertyOverrides: overrides}
	res, err := cl.SendOneStream(ctx, http.MethodPost, testCatalogPath, body, false)
	if err != nil {
		return nil, fmt.Errorf("invoking TestCatalog: %w", err)
	}
	defer res.Body.Close()

	rawBody, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("reading TestCatalog response: %w", err)
	}
	if res.StatusCode/100 != 2 {
		return nil, fmt.Errorf("TestCatalog returned HTTP %d: %s", res.StatusCode, string(rawBody))
	}
	var parsed testCatalogResponse
	if err := json.Unmarshal(rawBody, &parsed); err != nil {
		return nil, fmt.Errorf("decoding TestCatalog response: %w (raw: %s)", err, string(rawBody))
	}
	return &parsed, nil
}
