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

type glueFlow struct{}

func (*glueFlow) name() string { return "AWS Glue (via Iceberg REST + SigV4)" }

func (*glueFlow) collect(
	ctx context.Context,
	cl *rpadmin.AdminAPI,
	current rpadmin.Config,
	_ rpadmin.ConfigSchema,
) (map[string]string, bool, error) {
	return collectWithRetry(ctx, cl, func(o map[string]string) error {
		o["iceberg_catalog_type"] = "rest"
		o["iceberg_rest_catalog_authentication_mode"] = "aws_sigv4"
		o["iceberg_rest_catalog_aws_service_name"] = "glue"

		if err := stepEndpoint(o, current,
			"Glue Iceberg REST endpoint "+
				"(e.g. https://glue.us-east-1.amazonaws.com/iceberg):",
		); err != nil {
			return err
		}
		if err := stepGlueBaseLocation(o, current); err != nil {
			return err
		}
		return stepSigV4(o, current, "us-east-1")
	})
}
