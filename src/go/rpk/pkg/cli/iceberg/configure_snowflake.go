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

type snowflakeFlow struct{}

func (*snowflakeFlow) name() string { return "Snowflake Open Catalog (Polaris)" }

func (*snowflakeFlow) collect(
	ctx context.Context,
	cl *rpadmin.AdminAPI,
	current rpadmin.Config,
	_ rpadmin.ConfigSchema,
) (map[string]string, bool, error) {
	return collectWithRetry(ctx, cl, func(o map[string]string) error {
		o["iceberg_catalog_type"] = "rest"
		o["iceberg_rest_catalog_authentication_mode"] = "oauth2"

		if err := stepEndpoint(o, current,
			"Snowflake Open Catalog REST endpoint "+
				"(e.g. https://<snowflake-orgname>-<open-catalog-account-name>"+
				".snowflakecomputing.com/polaris/api/catalog):",
		); err != nil {
			return err
		}
		if err := stepOAuth2(o, current, oauth2Opts{
			clientIDMsg:     "OAuth2 client ID (e.g. <open-catalog-connection-client-id>):",
			clientSecretMsg: "OAuth2 client secret:",
			scopeDefault:    "PRINCIPAL_ROLE:ALL",
			scopeMsg:        "OAuth2 scope:",
		}); err != nil {
			return err
		}
		return stepWarehouse(o, current,
			"Warehouse / catalog name (e.g. <open-catalog-name>):", true)
	})
}
