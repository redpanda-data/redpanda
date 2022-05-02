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
	"errors"
	"net/http"
	"path/filepath"
	"time"

	"cloud.google.com/go/compute/metadata"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud/vendor"
)

const name = "gcp"

type GcpVendor struct{}

type InitializedGcpVendor struct {
	client *metadata.Client
}

func (*GcpVendor) Name() string {
	return name
}

func (*GcpVendor) Init() (vendor.InitializedVendor, error) {
	timeout := 500 * time.Millisecond
	client := metadata.NewClient(&http.Client{Timeout: timeout})
	if available(client, timeout) {
		return &InitializedGcpVendor{client}, nil
	}
	return nil, errors.New("vendor GCP couldn't be initialized")
}

func (v *InitializedGcpVendor) VMType() (string, error) {
	t, err := v.client.Get("instance/machine-type")
	if err != nil {
		return "", err
	}
	// t will have the form projects/<project ID>/machineTypes/<machine type>
	// so we have to return only the last segment.
	// See https://cloud.google.com/compute/docs/storing-retrieving-metadata#default
	// for more info.
	return filepath.Base(t), nil
}

func (*InitializedGcpVendor) Name() string {
	return name
}

func available(client *metadata.Client, timeout time.Duration) bool {
	result := make(chan bool)

	go func(c *metadata.Client, res chan<- bool) {
		_, err := c.ProjectID()
		res <- err == nil
	}(client, result)

	select {
	case res := <-result:
		return res
	case <-time.After(timeout):
		return false
	}
}
