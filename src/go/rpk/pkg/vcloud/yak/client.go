// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package yak

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	log "github.com/sirupsen/logrus"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/vcloud/config"
)

type YakClient struct {
	conf config.ConfigReaderWriter
}

const (
	namespaceRoute = "namespace"

	// TODO(av) make this configurable and point it to production
	DefaultYakUrl = "https://backend.dev.vectorized.cloud"
)

func NewYakClient(conf config.ConfigReaderWriter) CloudApiClient {
	return &YakClient{
		conf: conf,
	}
}

func (yc *YakClient) GetNamespaces() ([]*Namespace, error) {
	token, err := yc.conf.ReadToken()
	if err != nil {
		return nil, ErrLoginTokenMissing{err}
	}
	url := fmt.Sprintf("%s/%s", DefaultYakUrl, namespaceRoute)
	req, err := http.NewRequest(http.MethodGet, url, bytes.NewBuffer([]byte{}))
	if err != nil {
		return nil, fmt.Errorf("error creating new request. %w", err)
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error calling api: %w", err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %w", err)
	}

	// targetting HTTP codes 401, 403 which are used by yak
	if resp.StatusCode > 400 && resp.StatusCode < 404 {
		log.Debug(body)
		return nil, ErrNotAuthorized
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("error retrieving resource, http code %d. %s", resp.StatusCode, body)
	}
	var namespaces []*Namespace
	err = json.Unmarshal(body, &namespaces)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling response. %w", err)
	}
	return namespaces, nil
}
