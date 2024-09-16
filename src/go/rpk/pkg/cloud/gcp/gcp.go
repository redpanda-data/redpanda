// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package gcp

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"time"

	"cloud.google.com/go/compute/metadata"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud/provider"
)

const name = "gcp"

type GcpProvider struct{}

type InitializedGcpProvider struct {
	client *metadata.Client
}

func (*GcpProvider) Name() string {
	return name
}

func (*GcpProvider) Init() (provider.InitializedProvider, error) {
	timeout := 500 * time.Millisecond
	client := metadata.NewClient(&http.Client{Timeout: timeout})
	if available(client, timeout) {
		return &InitializedGcpProvider{client}, nil
	}
	return nil, errors.New("provider GCP couldn't be initialized")
}

func (v *InitializedGcpProvider) VMType() (string, error) {
	t, err := v.client.GetWithContext(context.Background(), "instance/machine-type")
	if err != nil {
		return "", err
	}
	// t will have the form projects/<project ID>/machineTypes/<machine type>
	// so we have to return only the last segment.
	// See https://cloud.google.com/compute/docs/storing-retrieving-metadata#default
	// for more info.
	return filepath.Base(t), nil
}

func (*InitializedGcpProvider) Name() string {
	return name
}

func available(client *metadata.Client, timeout time.Duration) bool {
	result := make(chan bool)

	go func(c *metadata.Client, res chan<- bool) {
		_, err := c.ProjectIDWithContext(context.Background())
		res <- err == nil
	}(client, result)

	select {
	case res := <-result:
		return res
	case <-time.After(timeout):
		return false
	}
}
