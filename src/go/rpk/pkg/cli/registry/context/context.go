// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package context

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
)

const qualifiedSubjectsConfigKey = "schema_registry_enable_qualified_subjects"

type contextResponse struct {
	Name          string `json:"name" yaml:"name"`
	Mode          string `json:"mode" yaml:"mode"`
	Compatibility string `json:"compatibility" yaml:"compatibility"`
}

// ListContexts calls cl.Contexts and translates a 404 into a message
// indicating that the Redpanda cluster does not support schema contexts.
func ListContexts(ctx context.Context, cl *sr.Client) ([]string, error) {
	contexts, err := cl.Contexts(ctx)
	if err != nil {
		var re *sr.ResponseError
		if errors.As(err, &re) && re.StatusCode == 404 {
			return nil, fmt.Errorf("schema registry contexts are not supported by this cluster")
		}
		return nil, err
	}
	return contexts, nil
}

// checkQualifiedSubjectsEnabled verifies that the cluster has the
// schema_registry_enable_qualified_subjects config set to true via the
// Admin API.
func checkQualifiedSubjectsEnabled(ctx context.Context, fs afero.Fs, profile *config.RpkProfile) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cl, err := adminapi.NewClient(ctx, fs, profile)
	if err != nil {
		return fmt.Errorf("unable to verify schema context support via admin API: %w\nUse --skip-context-check to skip this verification", err)
	}
	cfg, err := cl.SingleKeyConfig(ctx, qualifiedSubjectsConfigKey)
	if err != nil {
		return fmt.Errorf("unable to verify schema context support via admin API: %w\nUse --skip-context-check to skip this verification", err)
	}
	val, exists := cfg[qualifiedSubjectsConfigKey]
	if !exists {
		return fmt.Errorf("schema contexts are not supported by this cluster (config key %q not found); the cluster may need upgrading", qualifiedSubjectsConfigKey)
	}
	enabled, ok := val.(bool)
	if !ok {
		return fmt.Errorf("schema contexts are not supported by this cluster (unexpected value for %q: %v)", qualifiedSubjectsConfigKey, val)
	}
	if !enabled {
		return fmt.Errorf("schema contexts are not enabled on this cluster; you may enable it using:\n  rpk cluster config set %s true", qualifiedSubjectsConfigKey)
	}
	return nil
}

// IsContextSupported checks whether the cluster supports schema contexts
// by verifying the admin API feature flag.
func IsContextSupported(ctx context.Context, fs afero.Fs, profile *config.RpkProfile, skipAdminCheck bool) error {
	if skipAdminCheck {
		return nil
	}
	return checkQualifiedSubjectsEnabled(ctx, fs, profile)
}

// ValidateContext validates the schema context name format, loads the
// profile, and confirms the cluster supports contexts via the admin API
// feature flag.
func ValidateContext(ctx context.Context, schemaCtx string, fs afero.Fs, p *config.Params, skipAdminCheck bool) error {
	if schemaCtx[0] != '.' {
		return fmt.Errorf("invalid schema context %q: context names must start with a '.'", schemaCtx)
	}
	if strings.Contains(schemaCtx, ":") {
		return fmt.Errorf("invalid schema context %q: context names must not contain ':'", schemaCtx)
	}
	profile, err := p.LoadVirtualProfile(fs)
	if err != nil {
		return fmt.Errorf("rpk unable to load config: %w", err)
	}
	return IsContextSupported(ctx, fs, profile, skipAdminCheck)
}

func NewCommand(fs afero.Fs, p *config.Params, _ *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "context",
		Args:  cobra.ExactArgs(0),
		Short: "Manage schema registry contexts",
		Long: `Manage schema registry contexts.

Schema contexts provide namespace isolation within the schema registry,
allowing multiple independent sets of subjects and schemas to coexist.

Before using schema contexts, the cluster must have the
schema_registry_enable_qualified_subjects configuration set to true. You
can enable it with:

  rpk cluster config set schema_registry_enable_qualified_subjects true

Use the --schema-context flag on the parent 'registry' command to scope
operations to a specific context.
`,
	}
	cmd.AddCommand(
		listCommand(fs, p),
		deleteCommand(fs, p),
	)
	return cmd
}
