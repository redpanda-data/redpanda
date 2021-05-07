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

func TestClusters(t *testing.T) {
	tests := []struct {
		name           string
		client         yak.CloudApiClient
		expectedOutput []string
		expectedError  string
	}{
		{
			"success",
			&mockYakClient{},
			[]string{"notready", "false"},
			"",
		},
		{
			"not logged in",
			&erroredYakClient{yak.ErrLoginTokenMissing{errors.New("inner")}},
			[]string{"rpk cloud login"},
			"retrieving login token",
		},
		{
			"generic client error",
			&erroredYakClient{errors.New("other error")},
			[]string{},
			"other error",
		},
	}

	for _, tt := range tests {
		var buf bytes.Buffer
		logrus.SetOutput(&buf)
		err := cloud.GetClusters(tt.client, &buf, "ns")
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
