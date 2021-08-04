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
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/Shopify/sarama"
)

const usersEndpoint = "/v1/security/users"

type newUser struct {
	User      string `json:"username"`
	Password  string `json:"password"`
	Algorithm string `json:"algorithm"`
}

func (a *AdminAPI) CreateUser(username, password string) error {
	if username == "" {
		return errors.New("empty username")
	}
	if password == "" {
		return errors.New("empty password")
	}
	u := newUser{
		User:      username,
		Password:  password,
		Algorithm: sarama.SASLTypeSCRAMSHA256,
	}
	urls := make([]string, len(a.urls))
	for i := 0; i < len(a.urls); i++ {
		urls[i] = fmt.Sprintf("%s%s", a.urls[i], usersEndpoint)
	}
	_, err := sendToMultiple(urls, http.MethodPost, u, a.client)
	return err
}

func (a *AdminAPI) DeleteUser(username string) error {
	if username == "" {
		return errors.New("empty username")
	}

	urls := make([]string, len(a.urls))
	for i := 0; i < len(a.urls); i++ {
		urls[i] = fmt.Sprintf("%s%s/%s", a.urls[i], usersEndpoint, username)
	}
	_, err := sendToMultiple(urls, http.MethodDelete, nil, a.client)
	return err
}

func (a *AdminAPI) ListUsers() ([]string, error) {
	urls := make([]string, len(a.urls))
	for i := 0; i < len(a.urls); i++ {
		urls[i] = fmt.Sprintf("%s%s", a.urls[i], usersEndpoint)
	}
	res, err := sendToMultiple(urls, http.MethodGet, nil, a.client)
	if err != nil {
		return nil, err
	}
	bs, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	usernames := []string{}
	err = json.Unmarshal(bs, &usernames)
	return usernames, err
}
