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
	}, {
		name:    "delete should fail if building the admin API client fails",
		command: acl.NewDeleteUserCommand,
		mockAdminAPI: func() (admin.AdminAPI, error) {
			return nil, errors.New("Woops, sorry")
		},
		args: []string{
			"--delete-username", "user",
		},
		expectedErrMsg: "Woops, sorry",
	}, {
		name:    "delete should fail if --delete-username isn't passed",
		command: acl.NewDeleteUserCommand,
		mockAdminAPI: func() (admin.AdminAPI, error) {
			return nil, nil
		},
		expectedErrMsg: `required flag(s) "delete-username" not set`,
	}, {
		name:    "delete should fail if deleting the user fails",
		command: acl.NewDeleteUserCommand,
		mockAdminAPI: func() (admin.AdminAPI, error) {
			return &admin.MockAdminAPI{
				MockDeleteUser: func(_ string) error {
					return errors.New("user deletion request failed")
				},
			}, nil
		},
		args: []string{
			"--delete-username", "user",
		},
		expectedErrMsg: "user deletion request failed",
	}, {
		name:    "create should print the deleted user",
		command: acl.NewDeleteUserCommand,
		mockAdminAPI: func() (admin.AdminAPI, error) {
			return &admin.MockAdminAPI{}, nil
		},
		args: []string{
			"--delete-username", "user",
		},
		expectedOut: "Deleted user 'user'",
	}, {
		name:    "list should fail if building the admin API client fails",
		command: acl.NewListUsersCommand,
		mockAdminAPI: func() (admin.AdminAPI, error) {
			return nil, errors.New("Woops, sorry")
		},
		expectedErrMsg: "Woops, sorry",
	}, {
		name:    "list should fail if listing the users fails",
		command: acl.NewListUsersCommand,
		mockAdminAPI: func() (admin.AdminAPI, error) {
			return &admin.MockAdminAPI{
				MockListUsers: func() ([]string, error) {
					return nil, errors.New("user list request failed")
				},
			}, nil
		},
		expectedErrMsg: "user list request failed",
	}, {
		name:    "list should print the users",
		command: acl.NewListUsersCommand,
		mockAdminAPI: func() (admin.AdminAPI, error) {
			return &admin.MockAdminAPI{
				MockListUsers: func() ([]string, error) {
					return []string{
						"Michael",
						"Jim",
						"Pam",
						"Dwight",
						"Kelly",
						"Bob Vance, Vance Refrigeration",
					}, nil
				},
			}, nil
		},
		expectedOut: `  USERNAME                        
                                  
  Michael                         
  Jim                             
  Pam                             
  Dwight                          
  Kelly                           
  Bob Vance, Vance Refrigeration  
`,
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
