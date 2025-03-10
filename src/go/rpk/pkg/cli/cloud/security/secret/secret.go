// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package secret

import (
	"fmt"
	"regexp"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "secret",
		Short: "Manage secrets for Redpanda Cloud clusters",
		Long: `Manage secrets for Redpanda Cloud clusters.

This command allows you to manage secrets for your cloud clusters.
`,
	}

	cmd.AddCommand(
		newCreateCommand(fs, p),
		newDeleteCommand(fs, p),
		newListCommand(fs, p),
		newUpdateCommand(fs, p),
	)

	return cmd
}

func validateSecretName(secretName string) error {
	if secretName == "" {
		return fmt.Errorf("secret name cannot be empty")
	}
	if len(secretName) > 255 {
		return fmt.Errorf("secret name cannot exceed 255 characters")
	}
	matched, err := regexp.MatchString(`^[A-Z][A-Z0-9_]*$`, secretName)
	if err != nil {
		return fmt.Errorf("error validating secret name: %v", err)
	}
	if !matched {
		return fmt.Errorf("secret name must start with an uppercase letter and can only contain uppercase letters, digits, and underscores")
	}
	return nil
}
