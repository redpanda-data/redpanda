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
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/acl"
)

type mockUserAPI struct {
	mockCreateUser func(username, password string) error
	mockDeleteUser func(username string) error
	mockListUsers  func() ([]string, error)
}

func (m *mockUserAPI) CreateUser(username, password string) error {
	if m.mockCreateUser != nil {
		return m.mockCreateUser(username, password)
	}
	return nil
}

func (m *mockUserAPI) DeleteUser(username string) error {
	if m.mockDeleteUser != nil {
		return m.mockDeleteUser(username)
	}
	return nil
}

func (m *mockUserAPI) ListUsers() ([]string, error) {
	if m.mockListUsers != nil {
		return m.mockListUsers()
	}
	return []string{}, nil
}

func TestACLUserCommands(t *testing.T) {
	tests := []struct {
		name           string
		command        func(func() (acl.UserAPI, error)) *cobra.Command
		mockUserAPI    func() (acl.UserAPI, error)
		args           []string
		expectedOut    string
		expectedErrMsg string
	}{{
		name:    "create should fail if building the admin API client fails",
		command: acl.NewCreateUserCommand,
		mockUserAPI: func() (acl.UserAPI, error) {
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
		mockUserAPI: func() (acl.UserAPI, error) {
			return nil, nil
		},
		args: []string{
			"--new-password", "pass",
		},
		expectedErrMsg: `required flag(s) "new-username" not set`,
	}, {
		name:    "create should fail if --new-password isn't passed",
		command: acl.NewCreateUserCommand,
		mockUserAPI: func() (acl.UserAPI, error) {
			return nil, nil
		},
		args: []string{
			"--new-username", "user",
		},
		expectedErrMsg: `required flag(s) "new-password" not set`,
	}, {
		name:    "create should fail if creating the user fails",
		command: acl.NewCreateUserCommand,
		mockUserAPI: func() (acl.UserAPI, error) {
			return &mockUserAPI{
				mockCreateUser: func(_, _ string) error {
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
		mockUserAPI: func() (acl.UserAPI, error) {
			return &mockUserAPI{}, nil
		},
		args: []string{
			"--new-username", "user",
			"--new-password", "pass",
		},
		expectedOut: "Created user 'user'",
	}, {
		name:    "delete should fail if building the admin API client fails",
		command: acl.NewDeleteUserCommand,
		mockUserAPI: func() (acl.UserAPI, error) {
			return nil, errors.New("Woops, sorry")
		},
		args: []string{
			"--delete-username", "user",
		},
		expectedErrMsg: "Woops, sorry",
	}, {
		name:    "delete should fail if --delete-username isn't passed",
		command: acl.NewDeleteUserCommand,
		mockUserAPI: func() (acl.UserAPI, error) {
			return nil, nil
		},
		expectedErrMsg: `required flag(s) "delete-username" not set`,
	}, {
		name:    "delete should fail if deleting the user fails",
		command: acl.NewDeleteUserCommand,
		mockUserAPI: func() (acl.UserAPI, error) {
			return &mockUserAPI{
				mockDeleteUser: func(_ string) error {
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
		mockUserAPI: func() (acl.UserAPI, error) {
			return &mockUserAPI{}, nil
		},
		args: []string{
			"--delete-username", "user",
		},
		expectedOut: "Deleted user 'user'",
	}, {
		name:    "list should fail if building the admin API client fails",
		command: acl.NewListUsersCommand,
		mockUserAPI: func() (acl.UserAPI, error) {
			return nil, errors.New("Woops, sorry")
		},
		expectedErrMsg: "Woops, sorry",
	}, {
		name:    "list should fail if listing the users fails",
		command: acl.NewListUsersCommand,
		mockUserAPI: func() (acl.UserAPI, error) {
			return &mockUserAPI{
				mockListUsers: func() ([]string, error) {
					return nil, errors.New("user list request failed")
				},
			}, nil
		},
		expectedErrMsg: "user list request failed",
	}, {
		name:    "list should print the users",
		command: acl.NewListUsersCommand,
		mockUserAPI: func() (acl.UserAPI, error) {
			return &mockUserAPI{
				mockListUsers: func() ([]string, error) {
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
			cmd := tt.command(tt.mockUserAPI)
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
