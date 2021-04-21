// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package acl_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/acl"
)

func TestACLUserCommands(t *testing.T) {
	tests := []struct {
		name           string
		command        func(func() (admin.AdminAPI, error)) *cobra.Command
		mockAdminAPI   func() (admin.AdminAPI, error)
		args           []string
		expectedOut    string
		expectedErrMsg string
	}{{
		name:    "create should fail if building the admin API client fails",
		command: acl.NewCreateUserCommand,
		mockAdminAPI: func() (admin.AdminAPI, error) {
			return nil, errors.New("Woops, sorry")
		},
		args: []string{
			"--new-username", "user",
			"--new-password", "pass",
		},
		expectedErrMsg: "Woops, sorry",
	}, {
		name:    "create should fail if --new-username isn't passed",
		command: acl.NewCreateUserCommand,
		mockAdminAPI: func() (admin.AdminAPI, error) {
			return nil, nil
		},
		args: []string{
			"--new-password", "pass",
		},
		expectedErrMsg: `required flag(s) "new-username" not set`,
	}, {
		name:    "create should fail if --new-password isn't passed",
		command: acl.NewCreateUserCommand,
		mockAdminAPI: func() (admin.AdminAPI, error) {
			return nil, nil
		},
		args: []string{
			"--new-username", "user",
		},
		expectedErrMsg: `required flag(s) "new-password" not set`,
	}, {
		name:    "create should fail if creating the user fails",
		command: acl.NewCreateUserCommand,
		mockAdminAPI: func() (admin.AdminAPI, error) {
			return &admin.MockAdminAPI{
				MockCreateUser: func(_, _ string) error {
					return errors.New("user creation request failed")
				},
			}, nil
		},
		args: []string{
			"--new-username", "user",
			"--new-password", "pass",
		},
		expectedErrMsg: "user creation request failed",
	}, {
		name:    "create should print the created user",
		command: acl.NewCreateUserCommand,
		mockAdminAPI: func() (admin.AdminAPI, error) {
			return &admin.MockAdminAPI{}, nil
		},
		args: []string{
			"--new-username", "user",
			"--new-password", "pass",
		},
		expectedOut: "Created user 'user'",
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			var out bytes.Buffer
			cmd := tt.command(tt.mockAdminAPI)
			logrus.SetOutput(&out)
			cmd.SetArgs(tt.args)
			err := cmd.Execute()
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			require.NoError(st, err)
			require.Contains(st, out.String(), tt.expectedOut)
		})
	}
}
