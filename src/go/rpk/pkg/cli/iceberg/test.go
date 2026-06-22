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
	"fmt"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

// testResult is the structured form printed when --format json/yaml is set.
type testResult struct {
	OK      bool   `json:"ok" yaml:"ok"`
	Code    string `json:"code,omitempty" yaml:"code,omitempty"`
	Message string `json:"message,omitempty" yaml:"message,omitempty"`
}

func newTestCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var setOverrides []string
	cmd := &cobra.Command{
		Use:   "test",
		Short: "Probe the cluster's Iceberg catalog for connectivity",
		Long: `Probe the cluster's Iceberg catalog for connectivity and credential validity.

Without any flags, this tests the currently-applied cluster configuration.
With one or more '--set key=value' overrides, the server constructs an
ephemeral catalog from the running config overlaid with the given
properties and probes that instead. Overrides are not applied to the
cluster.

The probe makes one ` + "`/v1/config`" + ` (or filesystem stat) round-trip against the
configured catalog. It validates: reachability, TLS, and whatever auth
the catalog's '/v1/config' endpoint checks (OAuth2 client credentials,
bearer token, AWS SigV4). It does not validate write permissions to
create namespaces / commit tables; those failures will only surface
when Redpanda first commits an Iceberg topic.`,
		Example: `Test the currently-applied cluster config:
  rpk iceberg test

Test a proposed REST catalog endpoint without applying it:
  rpk iceberg test --set iceberg_rest_catalog_endpoint=http://my-rest:8181

Test multiple proposed overrides at once:
  rpk iceberg test \
    --set iceberg_catalog_type=rest \
    --set iceberg_rest_catalog_endpoint=http://my-rest:8181 \
    --set iceberg_rest_catalog_authentication_mode=oauth2 \
    --set iceberg_rest_catalog_client_id=panda-user \
    --set iceberg_rest_catalog_client_secret=*****`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help(testResult{}); ok {
				out.Exit(h)
			}

			vp, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			overrides, err := parseSetFlags(setOverrides)
			out.MaybeDie(err, "%v", err)

			cl, err := adminapi.NewClient(cmd.Context(), fs, vp)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			resp, err := invokeTestCatalog(cmd.Context(), cl, overrides)
			out.MaybeDie(err, "%v", err)

			ok := resp.CatalogDescribeErrorCode == ""
			result := testResult{
				OK:      ok,
				Code:    resp.CatalogDescribeErrorCode,
				Message: resp.CatalogDescribeErrorMessage,
			}
			if isText, _, formatted, err := f.Format(result); !isText {
				out.MaybeDieErr(err)
				fmt.Println(formatted)
				if !ok {
					out.Exit("")
				}
				return
			}
			if ok {
				fmt.Println("  ✓ Iceberg catalog reachable. Credentials OK.")
				return
			}
			out.Die("  ✗ Iceberg catalog test failed (%s): %s", resp.CatalogDescribeErrorCode, resp.CatalogDescribeErrorMessage)
		},
	}
	cmd.Flags().StringArrayVar(&setOverrides, "set", nil,
		"Cluster property override of the form key=value, repeatable. Tested without being applied to the cluster")
	p.InstallFormatFlag(cmd)
	return cmd
}

// parseSetFlags turns ["k=v","k2=v2"] into a map. Empty input returns nil
// (sentinel meaning "test running config").
func parseSetFlags(flags []string) (map[string]string, error) {
	if len(flags) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(flags))
	for _, raw := range flags {
		idx := strings.IndexByte(raw, '=')
		if idx <= 0 {
			return nil, fmt.Errorf("--set value %q is not of the form key=value", raw)
		}
		k := raw[:idx]
		v := raw[idx+1:]
		if _, exists := out[k]; exists {
			return nil, fmt.Errorf("--set %q specified more than once", k)
		}
		out[k] = v
	}
	return out, nil
}
