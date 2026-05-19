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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
)

type restFlow struct{}

func (*restFlow) name() string { return "Generic Iceberg REST catalog" }

func (*restFlow) collect(
	ctx context.Context,
	cl *rpadmin.AdminAPI,
	current rpadmin.Config,
	_ rpadmin.ConfigSchema,
) (map[string]string, bool, error) {
	return collectWithRetry(ctx, cl, func(o map[string]string) error {
		o["iceberg_catalog_type"] = "rest"

		if err := stepEndpoint(o, current, "REST catalog endpoint URL:"); err != nil {
			return err
		}
		authMode, err := out.Pick(
			[]string{"none", "bearer", "oauth2", "aws_sigv4", "gcp"},
			"Authentication mode (currently: %s)",
			currentStringOr(
				"iceberg_rest_catalog_authentication_mode", "none", current),
		)
		if err != nil {
			return err
		}
		o["iceberg_rest_catalog_authentication_mode"] = authMode

		switch authMode {
		case "bearer":
			if err := stepBearer(o, current, "Bearer token:"); err != nil {
				return err
			}
		case "oauth2":
			if err := stepOAuth2(o, current, oauth2Opts{
				clientIDMsg:     "Client ID:",
				clientSecretMsg: "Client secret:",
				serverURIMsg:    "OAuth2 token endpoint (blank to use catalog's /v1/oauth/tokens):",
				scopeDefault:    "PRINCIPAL_ROLE:ALL",
				scopeMsg:        "OAuth2 scope:",
			}); err != nil {
				return err
			}
		case "aws_sigv4":
			if err := stepSigV4(o, current, "us-east-1"); err != nil {
				return err
			}
		case "gcp":
			if err := stepGCP(o, current); err != nil {
				return err
			}
		}
		return stepWarehouse(o, current,
			"Warehouse name (optional):", false)
	})
}
