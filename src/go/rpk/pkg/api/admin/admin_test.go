// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package admin

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
)

func TestCreateUser(t *testing.T) {
	username := "Joss"
	password := "momorocks"
	algorithm := sarama.SASLTypeSCRAMSHA256
	body := newUser{
		User:      username,
		Password:  password,
		Algorithm: algorithm,
	}
	bs, err := json.Marshal(body)
	require.NoError(t, err)

	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, err := ioutil.ReadAll(r.Body)
			require.NoError(t, err)
			require.Exactly(t, bs, b)
			w.WriteHeader(http.StatusOK)
		}),
	)
	defer ts.Close()

	adminClient, err := NewAdminAPI(ts.URL, nil)
	err = adminClient.CreateUser(username, password)
	require.NoError(t, err)
}

func TestDeleteUser(t *testing.T) {
	username := "Lola"

	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Exactly(t, "/v1/security/users/Lola", r.URL.Path)
		}),
	)
	defer ts.Close()

	adminClient, err := NewAdminAPI(ts.URL, nil)
	err = adminClient.DeleteUser(username)
	require.NoError(t, err)
}

func TestListUsers(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(
				`["Joss", "lola", "jeff", "tobias"]`,
			))
		}),
	)
	defer ts.Close()

	adminClient, err := NewAdminAPI(ts.URL, nil)
	users, err := adminClient.ListUsers()
	require.NoError(t, err)
	require.Exactly(t, []string{"Joss", "lola", "jeff", "tobias"}, users)
}
