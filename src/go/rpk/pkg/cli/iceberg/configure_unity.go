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

	"github.com/redpanda-data/common-go/rpadmin"
)

type unityFlow struct{}

func (*unityFlow) name() string { return "Unity Catalog (Databricks)" }

func (*unityFlow) collect(
	ctx context.Context,
	cl *rpadmin.AdminAPI,
	current rpadmin.Config,
	_ rpadmin.ConfigSchema,
) (map[string]string, bool, error) {
	return collectWithRetry(ctx, cl, func(o map[string]string) error {
		o["iceberg_catalog_type"] = "rest"
		o["iceberg_rest_catalog_authentication_mode"] = "oauth2"

		if err := stepEndpoint(o, current,
			"Unity catalog REST endpoint "+
				"(e.g. https://<workspace-instance>"+
				"/api/2.1/unity-catalog/iceberg-rest):",
		); err != nil {
			return err
		}
		if err := stepOAuth2(o, current, oauth2Opts{
			clientIDMsg:       "OAuth2 client ID (Databricks service principal application ID):",
			clientSecretMsg:   "OAuth2 client secret:",
			serverURIMsg:      "OAuth2 token endpoint (e.g. https://<workspace-instance>/oidc/v1/token):",
			serverURIRequired: true,
			scopeFixed:        "all-apis",
		}); err != nil {
			return err
		}
		return stepWarehouse(o, current,
			"Warehouse name (e.g. <unity-catalog-name>, leave blank to skip):",
			false)
	})
}
