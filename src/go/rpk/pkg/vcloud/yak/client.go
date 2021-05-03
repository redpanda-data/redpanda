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
	// requires namespaceId
	getClustersRouteTemplate = namespaceRoute + "/%s/cluster"

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
	_, body, err := yc.get(token, url)
	if err != nil {
		return nil, fmt.Errorf("error calling api: %w", err)
	}

	var namespaces []*Namespace
	err = json.Unmarshal(body, &namespaces)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling response. %w", err)
	}
	return namespaces, nil
}

func (yc *YakClient) GetClusters(namespaceName string) ([]*Cluster, error) {
	token, err := yc.conf.ReadToken()
	if err != nil {
		return nil, ErrLoginTokenMissing{err}
	}
	// map namespace name to namespace id
	namespaceId, err := yc.getNamespaceId(namespaceName)
	if err != nil {
		return nil, err
	}
	// retrieve clusters
	url := fmt.Sprintf("%s/%s", DefaultYakUrl, fmt.Sprintf(getClustersRouteTemplate, namespaceId))
	_, body, err := yc.get(token, url)
	if err != nil {
		return nil, fmt.Errorf("error calling api: %w", err)
	}

	var clusters []*Cluster
	err = json.Unmarshal(body, &clusters)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling response. %w", err)
	}
	return clusters, nil
}

func (yc *YakClient) get(token, url string) (*http.Response, []byte, error) {
	log.Debugf("Calling yak api on url %s", url)
	req, err := http.NewRequest(http.MethodGet, url, bytes.NewBuffer(nil))
	if err != nil {
		return nil, nil, fmt.Errorf("error creating new request. %w", err)
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading response body: %w", err)
	}
	err = yc.handleErrResponseCodes(resp, body)
	if err != nil {
		return resp, body, err
	}
	return resp, body, nil
}

func (yc *YakClient) handleErrResponseCodes(
	resp *http.Response, body []byte,
) error {
	// targetting HTTP codes 401, 403 which are used by yak
	if resp.StatusCode > 400 && resp.StatusCode < 404 {
		log.Debug(string(body))
		return ErrNotAuthorized
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("error retrieving resource, http code %d. %s", resp.StatusCode, body)
	}
	return nil
}

func (yc *YakClient) getNamespaceId(namespaceName string) (string, error) {
	ns, err := yc.GetNamespaces()
	if err != nil {
		return "", err
	}
	for _, n := range ns {
		if n.Name == namespaceName {
			return n.Id, nil
		}
	}
	return "", ErrNamespaceDoesNotExist{namespaceName}
}
