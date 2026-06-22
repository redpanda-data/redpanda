// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Composable steps for the per-catalog wizard flows. Each step here
// prompts for a logically related set of cluster properties and mutates
// the passed-in overrides map. Per-backend flows compose them into a
// sequence.

package iceberg

import (
	"context"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
)

// buildOverrides is the per-flow build function: collect prompts, mutate
// the provided overrides map, return error on cancellation/IO failure.
type buildOverrides func(overrides map[string]string) error

// collectWithRetry drives the per-flow validate-and-retry loop. The
// per-flow `build` function is called repeatedly until either (a) the
// resulting override set passes the cluster TestCatalog probe, or (b)
// the user declines to re-enter values after a failed validation. The
// returned `validated` mirrors `promptCheckpoint`'s.
func collectWithRetry(
	ctx context.Context,
	cl *rpadmin.AdminAPI,
	build buildOverrides,
) (map[string]string, bool, error) {
	for {
		overrides := map[string]string{}
		if err := build(overrides); err != nil {
			return nil, false, err
		}
		retry, validated := promptCheckpoint(ctx, cl, overrides)
		if !retry {
			return overrides, validated, nil
		}
	}
}

// stepEndpoint prompts for `iceberg_rest_catalog_endpoint` and writes it
// into overrides. Required.
func stepEndpoint(
	overrides map[string]string,
	current rpadmin.Config,
	msg string,
) error {
	v, err := promptStringRequiredWithCurrent(
		"iceberg_rest_catalog_endpoint", msg, current)
	if err != nil {
		return err
	}
	overrides["iceberg_rest_catalog_endpoint"] = v
	return nil
}

// stepWarehouse prompts for `iceberg_rest_catalog_warehouse`. When
// required is false, an empty input is left out of the override map.
func stepWarehouse(
	overrides map[string]string,
	current rpadmin.Config,
	msg string,
	required bool,
) error {
	if required {
		v, err := promptStringRequiredWithCurrent(
			"iceberg_rest_catalog_warehouse", msg, current)
		if err != nil {
			return err
		}
		overrides["iceberg_rest_catalog_warehouse"] = v
		return nil
	}
	v, err := promptStringWithCurrent(
		"iceberg_rest_catalog_warehouse", "", msg, current)
	if err != nil {
		return err
	}
	if v != "" {
		overrides["iceberg_rest_catalog_warehouse"] = v
	}
	return nil
}

// oauth2Opts customizes stepOAuth2's per-backend wording and defaults.
type oauth2Opts struct {
	clientIDMsg     string
	clientSecretMsg string
	// If serverURIMsg is empty, the oauth2_server_uri prompt is skipped
	// entirely (use this when the backend serves token requests under its
	// own catalog URL, e.g. Snowflake Polaris).
	serverURIMsg      string
	serverURIRequired bool
	// If non-empty, the scope is hard-set to this value with no prompt.
	// If empty, the user is prompted with scopeDefault as the suggestion.
	scopeFixed   string
	scopeDefault string
	scopeMsg     string
}

// stepOAuth2 prompts for the OAuth2 properties: client_id, client_secret,
// optionally oauth2_server_uri, and oauth2_scope. Properties the user
// keeps at their existing cluster value (secret pressed-Enter-to-keep,
// optional server_uri left blank) are omitted from the override map.
func stepOAuth2(
	overrides map[string]string,
	current rpadmin.Config,
	opts oauth2Opts,
) error {
	clientID, err := promptStringRequiredWithCurrent(
		"iceberg_rest_catalog_client_id", opts.clientIDMsg, current)
	if err != nil {
		return err
	}
	overrides["iceberg_rest_catalog_client_id"] = clientID

	secret, includeSecret, err := promptSecretWithCurrent(
		"iceberg_rest_catalog_client_secret", opts.clientSecretMsg, current)
	if err != nil {
		return err
	}
	if includeSecret {
		overrides["iceberg_rest_catalog_client_secret"] = secret
	}

	if opts.serverURIMsg != "" {
		var serverURI string
		if opts.serverURIRequired {
			serverURI, err = promptStringRequiredWithCurrent(
				"iceberg_rest_catalog_oauth2_server_uri",
				opts.serverURIMsg, current)
		} else {
			serverURI, err = promptStringWithCurrent(
				"iceberg_rest_catalog_oauth2_server_uri",
				"", opts.serverURIMsg, current)
		}
		if err != nil {
			return err
		}
		if serverURI != "" {
			overrides["iceberg_rest_catalog_oauth2_server_uri"] = serverURI
		}
	}

	if opts.scopeFixed != "" {
		overrides["iceberg_rest_catalog_oauth2_scope"] = opts.scopeFixed
		return nil
	}
	scope, err := promptStringWithCurrent(
		"iceberg_rest_catalog_oauth2_scope",
		opts.scopeDefault, opts.scopeMsg, current)
	if err != nil {
		return err
	}
	overrides["iceberg_rest_catalog_oauth2_scope"] = scope
	return nil
}

// stepBearer prompts for `iceberg_rest_catalog_token`. Empty input
// (when current value is set) leaves the existing token in place.
func stepBearer(
	overrides map[string]string,
	current rpadmin.Config,
	msg string,
) error {
	token, includeSecret, err := promptSecretWithCurrent(
		"iceberg_rest_catalog_token", msg, current)
	if err != nil {
		return err
	}
	if includeSecret {
		overrides["iceberg_rest_catalog_token"] = token
	}
	return nil
}

// stepSigV4 prompts for the AWS SigV4 properties: region,
// credentials_source, and conditional access/secret keys (only when
// credentials_source is config_file).
func stepSigV4(
	overrides map[string]string,
	current rpadmin.Config,
	regionDefault string,
) error {
	region, err := promptStringWithCurrent(
		"iceberg_rest_catalog_aws_region",
		regionDefault, "AWS region:", current)
	if err != nil {
		return err
	}
	overrides["iceberg_rest_catalog_aws_region"] = region

	credSource, err := out.Pick(
		[]string{"config_file", "aws_instance_metadata", "sts"},
		"AWS credentials source (currently: %s)",
		currentStringOr(
			"iceberg_rest_catalog_aws_credentials_source",
			"unset", current),
	)
	if err != nil {
		return err
	}
	overrides["iceberg_rest_catalog_aws_credentials_source"] = credSource

	// Only config_file needs explicit access/secret keys.
	if credSource != "config_file" {
		return nil
	}
	accessKey, err := promptStringWithCurrent(
		"iceberg_rest_catalog_aws_access_key",
		"", "AWS access key:", current)
	if err != nil {
		return err
	}
	if accessKey != "" {
		overrides["iceberg_rest_catalog_aws_access_key"] = accessKey
	}
	secretKey, includeSecret, err := promptSecretWithCurrent(
		"iceberg_rest_catalog_aws_secret_key",
		"AWS secret key:", current)
	if err != nil {
		return err
	}
	if secretKey != "" && includeSecret {
		overrides["iceberg_rest_catalog_aws_secret_key"] = secretKey
	}
	return nil
}

// stepGCP prompts for the optional GCP user-project override. Actual
// credentials come from GCE instance metadata via the credential
// manager — no secret prompts here.
func stepGCP(
	overrides map[string]string,
	current rpadmin.Config,
) error {
	userProject, err := promptStringWithCurrent(
		"iceberg_rest_catalog_gcp_user_project",
		"",
		"GCP user project for quota/billing "+
			"(sets x-goog-user-project; leave blank to skip):",
		current)
	if err != nil {
		return err
	}
	if userProject != "" {
		overrides["iceberg_rest_catalog_gcp_user_project"] = userProject
	}
	return nil
}

// stepGlueBaseLocation prompts for `iceberg_rest_catalog_base_location`,
// which AWS Glue requires (the catalog client tells the server where
// the table data lives in S3).
func stepGlueBaseLocation(
	overrides map[string]string,
	current rpadmin.Config,
) error {
	v, err := promptStringRequiredWithCurrent(
		"iceberg_rest_catalog_base_location",
		"Base S3 URI for Iceberg tables "+
			"(e.g. s3://<cluster-storage-bucket-name>/<warehouse-path>) — "+
			"Glue requires this:",
		current)
	if err != nil {
		return err
	}
	overrides["iceberg_rest_catalog_base_location"] = v
	return nil
}
