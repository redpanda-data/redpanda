// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package admin

type MockAdminAPI struct {
	MockCreateUser func(username, password string) error
	MockDeleteUser func(username string) error
	MockListUsers  func() ([]string, error)
}

func (m *MockAdminAPI) CreateUser(username, password string) error {
	if m.MockCreateUser != nil {
		return m.MockCreateUser(username, password)
	}
	return nil
}

func (m *MockAdminAPI) DeleteUser(username string) error {
	if m.MockDeleteUser != nil {
		return m.MockDeleteUser(username)
	}
	return nil
}

func (m *MockAdminAPI) ListUsers() ([]string, error) {
	if m.MockListUsers != nil {
		return m.MockListUsers()
	}
	return []string{}, nil
}
