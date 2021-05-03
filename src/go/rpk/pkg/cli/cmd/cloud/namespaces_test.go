// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cloud_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/cloud"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/vcloud/yak"
)

func TestNamespaces(t *testing.T) {
	tests := []struct {
		name           string
		client         yak.CloudApiClient
		expectedOutput []string
		expectedError  string
	}{
		{
			name:           "success",
			client:         &mockYakClient{},
			expectedOutput: []string{"test", "2"},
			expectedError:  "",
		},
		{
			name:           "not logged in",
			client:         &erroredYakClient{yak.ErrLoginTokenMissing{errors.New("inner")}},
			expectedOutput: []string{"rpk cloud login"},
			expectedError:  "retrieving login token",
		},
		{
			name:           "generic client error",
			client:         &erroredYakClient{errors.New("other error")},
			expectedOutput: []string{},
			expectedError:  "other error",
		},
	}

	for _, tt := range tests {
		var buf bytes.Buffer
		logrus.SetOutput(&buf)
		err := cloud.GetNamespaces(tt.client, &buf)
		if len(tt.expectedOutput) > 0 {
			for _, s := range tt.expectedOutput {
				if !strings.Contains(buf.String(), s) {
					t.Errorf("%s: expecting string %s in output %s", tt.name, s, buf.String())
				}
			}
		}
		if tt.expectedError != "" {
			if err == nil || !strings.Contains(err.Error(), tt.expectedError) {
				t.Errorf("%s: expecting error %s got %v", tt.name, tt.expectedError, err)
			}
		}
	}
}

// Yak client returning fixed mocked responses
type mockYakClient struct {
}

func (yc *mockYakClient) GetNamespaces() ([]*yak.Namespace, error) {
	return []*yak.Namespace{
		{
			Id:         "test",
			Name:       "test",
			ClusterIds: []string{"1", "2"},
		},
	}, nil
}

func (yc *mockYakClient) GetClusters(
	namespaceName string,
) ([]*yak.Cluster, error) {
	return []*yak.Cluster{
		{
			Id:    "notready",
			Name:  "notready",
			Ready: false,
		},
	}, nil
}

// Yak client returning error provided on creation
type erroredYakClient struct {
	err error
}

func (yc *erroredYakClient) GetNamespaces() ([]*yak.Namespace, error) {
	return nil, yc.err
}

func (yc *erroredYakClient) GetClusters(
	namespaceName string,
) ([]*yak.Cluster, error) {
	return nil, yc.err
}
