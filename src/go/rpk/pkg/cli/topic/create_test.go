package topic

import (
	"context"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestTopicCreateCommand(t *testing.T) {
	// Create fake Kafka cluster
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	// Create in-memory filesystem with config
	fs := afero.NewMemMapFs()
	configPath := "/tmp/rpk.yaml"
	configContent := testConfig(cluster.ListenAddrs())
	require.NoError(t, afero.WriteFile(fs, configPath, []byte(configContent), 0o644))

	// Create params with the config
	params := &config.Params{
		ConfigFlag: configPath,
		Formatter:  config.OutFormatter{Kind: "text"},
	}

	// Create topic successfully
	t.Run("create topic", func(t *testing.T) {
		cmd := newCreateCommand(fs, params, func(code int) {
			t.Fatalf("unexpected os.Exit call with code %d", code)
		})
		cmd.SetArgs([]string{"create", "test-topic"})

		output := captureOutput(func() {
			err := cmd.Execute()
			require.NoError(t, err)
		})

		require.Contains(t, output, "test-topic")
		require.Contains(t, output, "OK")
		require.NotContains(t, output, "already exists")
	})

	// Verify topic exists
	kgoClient, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
	)
	require.NoError(t, err)
	t.Cleanup(kgoClient.Close)

	adminClient := kadm.NewClient(kgoClient)
	topics, err := adminClient.ListTopics(context.Background())
	require.NoError(t, err)
	_, exists := topics["test-topic"]
	require.True(t, exists, "topic should exist after creation")

	t.Run("create topic again fails", func(t *testing.T) {
		exited := false
		cmd := newCreateCommand(fs, params, func(code int) {
			exited = true
		})
		cmd.SetArgs([]string{"create", "test-topic"})

		output := captureOutput(func() {
			err := cmd.Execute()
			require.NoError(t, err)
		})

		require.True(t, exited, "expected os.Exit to be called")
		require.Contains(t, output, "test-topic")
		require.Contains(t, output, "TOPIC_ALREADY_EXISTS")
	})

	t.Run("create topic again with if not exists succeeds", func(t *testing.T) {
		cmd := newCreateCommand(fs, params, func(code int) {
			t.Fatalf("unexpected os.Exit call with code %d", code)
		})
		cmd.SetArgs([]string{"create", "test-topic", "--if-not-exists"})

		output := captureOutput(func() {
			err := cmd.Execute()
			require.NoError(t, err)
		})

		require.Contains(t, output, "test-topic")
		require.Contains(t, output, "OK (topic already exists)")
	})
}
