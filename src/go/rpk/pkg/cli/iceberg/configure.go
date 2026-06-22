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
	"fmt"
	"sort"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

// catalogFlow is the interface each per-catalog wizard implements.
type catalogFlow interface {
	// name is the human-facing name shown in the catalog type picker.
	name() string
	// collect runs the per-catalog prompts and returns the proposed
	// property override map. `current` holds the cluster's currently
	// applied property values (used to pre-fill prompts with existing
	// values where appropriate); `schema` is the property metadata
	// (used to distinguish secret properties). The `validated` return
	// indicates whether the last cluster validation of `overrides`
	// succeeded — runConfigure uses it to gate the apply step with an
	// extra safety prompt when the user chose to proceed despite a
	// failed validation.
	collect(
		ctx context.Context,
		cl *rpadmin.AdminAPI,
		current rpadmin.Config,
		schema rpadmin.ConfigSchema,
	) (overrides map[string]string, validated bool, err error)
}

func newConfigureCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "configure",
		Short: "Interactive wizard for setting up an Iceberg catalog",
		Long: `Interactive wizard for setting up an Iceberg catalog.

The wizard walks through the properties required for the chosen catalog
backend (Unity, AWS Glue, Snowflake Open Catalog, generic REST, or
object_storage), validates the proposed config against the cluster via
'rpk iceberg test' after each step, prints the proposed property set,
and applies it after a confirmation prompt. Answer "no" at the
confirmation to walk away without changing anything.

For non-interactive setup, use 'rpk cluster config set' directly.`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			vp, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			cl, err := adminapi.NewClient(cmd.Context(), fs, vp)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)
			runConfigure(cmd.Context(), cl)
		},
	}
	return cmd
}

func runConfigure(ctx context.Context, cl *rpadmin.AdminAPI) {
	schema, err := cl.ClusterConfigSchema(ctx)
	out.MaybeDie(err, "unable to fetch cluster config schema: %v", err)
	current, err := cl.Config(ctx, true)
	out.MaybeDie(err, "unable to fetch cluster config: %v", err)

	flows := []catalogFlow{
		&unityFlow{},
		&glueFlow{},
		&snowflakeFlow{},
		&restFlow{},
		&objectStorageFlow{},
	}
	options := make([]string, len(flows))
	for i, f := range flows {
		options[i] = f.name()
	}
	idx, err := out.PickIndex(options, "Which Iceberg catalog backend do you want to configure?")
	out.MaybeDie(err, "%v", err)
	chosen := flows[idx]

	overrides, validated, err := chosen.collect(ctx, cl, current, schema)
	out.MaybeDie(err, "%v", err)

	if len(overrides) == 0 {
		out.Exit("Wizard cancelled; no properties to apply.")
	}

	fmt.Println("\nProposed cluster properties:")
	printOverrideTable(overrides, schema)

	if !validated {
		fmt.Println(
			"\n⚠ These properties did NOT pass cluster validation.",
		)
	}
	confirm, err := out.Confirm("Apply these properties to the cluster?")
	out.MaybeDie(err, "%v", err)
	if !confirm {
		out.Exit("Aborted; no changes applied.")
	}
	if !validated {
		sure, err := out.ConfirmDefaultNo(
			"Are you sure? These configs could not be successfully " +
				"validated against the cluster.",
		)
		out.MaybeDie(err, "%v", err)
		if !sure {
			out.Exit("Aborted; no changes applied.")
		}
	}

	upsert := make(map[string]any, len(overrides))
	for k, v := range overrides {
		upsert[k] = v
	}
	if _, err := cl.PatchClusterConfig(ctx, upsert, []string{}); err != nil {
		out.Die("failed to apply cluster config: %v", err)
	}
	fmt.Println("\nApplied. If any property requires a cluster restart to take effect, " +
		"see 'rpk cluster config status' for the list of pending restarts.")
}

// validateOverrides calls TestCatalog with the accumulated map. Returns
// nil on success or a user-presentable error message on failure.
func validateOverrides(ctx context.Context, cl *rpadmin.AdminAPI, overrides map[string]string) error {
	resp, err := invokeTestCatalog(ctx, cl, overrides)
	if err != nil {
		return fmt.Errorf("unable to validate against cluster: %w", err)
	}
	if resp.CatalogDescribeErrorCode == "" {
		return nil
	}
	return fmt.Errorf("%s: %s", resp.CatalogDescribeErrorCode, resp.CatalogDescribeErrorMessage)
}

// promptCheckpoint runs TestCatalog with the accumulated map.
//
// Returns (retry, validated):
//   - retry=true: validation failed and the user asked to re-prompt.
//   - retry=false, validated=true: validation succeeded.
//   - retry=false, validated=false: validation failed and the user chose
//     not to re-prompt (will proceed to apply step gated by safety prompt).
func promptCheckpoint(ctx context.Context, cl *rpadmin.AdminAPI, overrides map[string]string) (retry, validated bool) {
	fmt.Println("\nValidating against cluster...")
	if err := validateOverrides(ctx, cl, overrides); err != nil {
		fmt.Printf("  ✗ %v\n", err)
		retry, _ := out.Confirm("Re-enter values?")
		return retry, false
	}
	fmt.Println("  ✓ Catalog reachable.")
	return false, true
}

func printOverrideTable(overrides map[string]string, schema rpadmin.ConfigSchema) {
	keys := sortedKeys(overrides)
	for _, k := range keys {
		v := overrides[k]
		if isSecret(k, schema) {
			v = "<redacted>"
		}
		fmt.Printf("  %-50s %s\n", k, v)
	}
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// isSecret returns true if the named cluster property is flagged is_secret
// in the admin-served schema.
func isSecret(name string, schema rpadmin.ConfigSchema) bool {
	meta, ok := schema[name]
	return ok && meta.IsSecret
}

// currentString returns the cluster's current string value for `name`,
// or "" if unset / not a string.
func currentString(name string, current rpadmin.Config) string {
	v, ok := current[name]
	if !ok || v == nil {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}

// currentStringOr is currentString with a fallback for unset / non-string
// values. Used to format human-facing "current value" hints in prompts.
func currentStringOr(name, fallback string, current rpadmin.Config) string {
	s := currentString(name, current)
	if s == "" {
		return fallback
	}
	return s
}

// promptStringWithCurrent prompts for a string with the cluster's current
// value (if any) pre-filled as the suggestion; otherwise falls back to
// `fallback`. The user can press Enter to accept the pre-fill.
func promptStringWithCurrent(
	name, fallback, msg string, current rpadmin.Config,
) (string, error) {
	suggestion := currentString(name, current)
	if suggestion == "" {
		suggestion = fallback
	}
	if suggestion == "" {
		return out.Prompt("%s", msg)
	}
	return out.PromptWithSuggestion(suggestion, "%s", msg)
}

// promptStringRequiredWithCurrent is promptStringWithCurrent with required
// (non-empty) validation. Re-prompts on empty input.
func promptStringRequiredWithCurrent(
	name, msg string, current rpadmin.Config,
) (string, error) {
	suggestion := currentString(name, current)
	for {
		var (
			v   string
			err error
		)
		if suggestion != "" {
			v, err = out.PromptWithSuggestion(suggestion, "%s", msg)
		} else {
			v, err = out.Prompt("%s", msg)
		}
		if err != nil {
			return "", err
		}
		if v != "" {
			return v, nil
		}
		fmt.Println("  Value is required.")
	}
}

// promptSecretWithCurrent prompts for a secret. When the property is
// already set on the cluster, the prompt is annotated so the user knows
// pressing Enter keeps the existing value; when unset, the prompt is
// plain (showing a "currently unset" label next to a password input
// reads as if asterisks are a masked default, which they aren't).
//
// Returns (value, include, err) where include=false means "skip this
// property in the override map" — either because the user pressed Enter
// to keep the existing secret, or because they pressed Enter on an unset
// property (no value to set).
func promptSecretWithCurrent(
	name, msg string, current rpadmin.Config,
) (string, bool, error) {
	// The admin server returns "[secret]" as a placeholder when the
	// property is set. Any non-empty value counts as "set".
	isSet := false
	if v, ok := current[name]; ok && v != nil {
		if s, ok := v.(string); ok && s != "" {
			isSet = true
		}
	}
	prompt := msg
	if isSet {
		prompt = msg + " (press Enter to keep current value)"
	}
	v, err := out.PromptPassword("%s", prompt)
	if err != nil {
		return "", false, err
	}
	if v == "" {
		return "", false, nil
	}
	return v, true, nil
}
