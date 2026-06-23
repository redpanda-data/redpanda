// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package topic

import (
	"context"
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestPrintAlterConfigResults(t *testing.T) {
	results := []alterConfigResult{
		{Topic: "foo", Status: "OK"},
		{Topic: "bar", Status: "Invalid topic"},
	}

	f := config.OutFormatter{Kind: "text"}
	b := &strings.Builder{}
	printAlterConfigResults(f, results, b)
	require.Equal(t, [][]string{
		{"TOPIC", "STATUS"},
		{"foo", "OK"},
		{"bar", "Invalid", "topic"},
	}, out.TableRows(b.String()))
}

func TestAlterConfigRegex(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	fs := afero.NewMemMapFs()
	configPath := "/tmp/rpk.yaml"
	require.NoError(t, afero.WriteFile(fs, configPath, []byte(testConfig(cluster.ListenAddrs())), 0o644))

	params := &config.Params{
		ConfigFlag: configPath,
		Formatter:  config.OutFormatter{Kind: "text"},
	}

	// Seed two topics that match "foo.*" and one that does not.
	kgoClient, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	require.NoError(t, err)
	t.Cleanup(kgoClient.Close)
	adm := kadm.NewClient(kgoClient)
	_, err = adm.CreateTopics(context.Background(), 1, 1, nil, "foo", "foo2", "bar")
	require.NoError(t, err)

	// Apply a config to every topic matching the regex, without naming each.
	cmd := newAlterConfigCommand(fs, params)
	cmd.SetArgs([]string{"alter-config", "-r", "foo.*", "--set", "retention.ms=3600000"})
	output := captureOutput(func() {
		err := cmd.Execute()
		require.NoError(t, err)
	})

	// Matched topics are reported; the non-matching topic is left untouched.
	require.Contains(t, output, "foo")
	require.Contains(t, output, "foo2")
	require.NotContains(t, output, "bar")

	// The config is applied only to the topics that matched the regex.
	rcs, err := adm.DescribeTopicConfigs(context.Background(), "foo", "foo2", "bar")
	require.NoError(t, err)
	got := make(map[string]string)
	for _, rc := range rcs {
		require.NoError(t, rc.Err)
		for _, c := range rc.Configs {
			if c.Key == "retention.ms" && c.Value != nil {
				got[rc.Name] = *c.Value
			}
		}
	}
	require.Equal(t, "3600000", got["foo"])
	require.Equal(t, "3600000", got["foo2"])
	require.NotEqual(t, "3600000", got["bar"])
}
