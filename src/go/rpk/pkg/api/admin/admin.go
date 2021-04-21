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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/prometheus/common/log"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	vtls "github.com/vectorizedio/redpanda/src/go/rpk/pkg/tls"
)

const (
	usersEndpoint = "/v1/security/users"
	httpPrefix    = "http://"
	httpsPrefix   = "https://"
)

type AdminAPI interface {
	CreateUser(username, password string) error
	DeleteUser(username string) error
	ListUsers() ([]string, error)
}

type adminAPI struct {
	url    string
	client *http.Client
}

type newUser struct {
	User      string `json:"username"`
	Password  string `json:"password"`
	Algorithm string `json:"algorithm"`
}

func NewAdminAPI(url string, tlsConf *config.TLS) (AdminAPI, error) {
	var err error

	// Go's http library requires that the URL have a protocol.
	if !(strings.HasPrefix(url, httpPrefix) ||
		strings.HasPrefix(url, httpsPrefix)) {

		prefix := httpPrefix

		if tlsConf != nil {
			// If TLS will be enabled, use HTTPS as the protocol
			prefix = httpsPrefix
		}

		url = strings.TrimRight(url, "/")
		url = fmt.Sprintf("%s%s", prefix, url)
	}

	var tlsConfig *tls.Config
	if tlsConf != nil {
		tlsConfig, err = vtls.BuildTLSConfig(
			tlsConf.CertFile,
			tlsConf.KeyFile,
			tlsConf.TruststoreFile,
		)
		if err != nil {
			return nil, err
		}
	}

	tr := &http.Transport{
		TLSClientConfig: tlsConfig,
	}

	client := &http.Client{Transport: tr}
	return &adminAPI{url: url, client: client}, nil
}

func (a *adminAPI) CreateUser(username, password string) error {
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
	url := fmt.Sprintf("%s%s", a.url, usersEndpoint)
	_, err := send(url, http.MethodPost, u, a.client)
	return err
}

func (a *adminAPI) DeleteUser(username string) error {
	if username == "" {
		return errors.New("empty username")
	}
	url := fmt.Sprintf("%s%s/%s", a.url, usersEndpoint, username)
	_, err := send(url, http.MethodDelete, nil, a.client)
	return err
}

func (a *adminAPI) ListUsers() ([]string, error) {
	url := fmt.Sprintf("%s%s", a.url, usersEndpoint)
	res, err := send(url, http.MethodGet, nil, a.client)
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

func send(
	url, method string, body interface{}, client *http.Client,
) (*http.Response, error) {
	var bodyBuffer io.Reader
	if body != nil {
		bs, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("couldn't encode request: %v", err)
		}
		bodyBuffer = bytes.NewBuffer(bs)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(
		ctx,
		method,
		url,
		bodyBuffer,
	)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := client.Do(req)

	if res != nil && res.StatusCode >= 400 {
		resBody, err := ioutil.ReadAll(res.Body)
		if err != nil {
			log.Error(err)
		}
		return res, fmt.Errorf(
			"Request failed with status %d: %s",
			res.StatusCode,
			string(resBody),
		)
	}

	return res, err
}
