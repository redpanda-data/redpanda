package main

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCalculateRedpandaID(t *testing.T) {
	redpandaIDFile := ".redpanda_id"
	t.Run("Clean state - empty Redpanda data folder", func(t *testing.T) {
		tmp := t.TempDir()
		cfg := config.Config{}

		statefulsetOrdinal := 2

		err := calculateRedpandaID(&cfg,
			configuratorConfig{
				dataDirPath: tmp,
			},
			brokerID(statefulsetOrdinal))
		assert.NoError(t, err)

		redpandaID, err := os.ReadFile(filepath.Join(tmp, redpandaIDFile))
		require.NoError(t, err)

		rpID, err := strconv.Atoi(string(redpandaID))
		require.NoError(t, err)

		assert.Equal(t, rpID, cfg.Redpanda.ID)
		assert.NotEqual(t, statefulsetOrdinal, cfg.Redpanda.ID)
	})

	t.Run("Fake not empty data Redpanda", func(t *testing.T) {
		tmp := t.TempDir()
		err := os.WriteFile(filepath.Join(tmp, "test"), []byte("test"), 0o666)
		require.NoError(t, err)

		statefulsetOrdinal := 2

		cfg := config.Config{}
		err = calculateRedpandaID(&cfg,
			configuratorConfig{
				dataDirPath: tmp,
			},
			brokerID(statefulsetOrdinal))
		assert.NoError(t, err)

		redpandaID, err := os.ReadFile(filepath.Join(tmp, redpandaIDFile))
		require.NoError(t, err)

		rpID, err := strconv.Atoi(string(redpandaID))
		require.NoError(t, err)

		assert.Equal(t, rpID, cfg.Redpanda.ID)
		assert.Equal(t, statefulsetOrdinal, cfg.Redpanda.ID)
	})

	t.Run(".redpanda_id file exists", func(t *testing.T) {
		tmp := t.TempDir()
		storedRedpandaID := 0
		err := os.WriteFile(filepath.Join(tmp, ".redpanda_id"), []byte(strconv.Itoa(storedRedpandaID)), 0o666)
		require.NoError(t, err)

		statefulsetOrdinal := 2

		cfg := config.Config{}
		err = calculateRedpandaID(&cfg,
			configuratorConfig{
				dataDirPath: tmp,
			},
			brokerID(statefulsetOrdinal))
		assert.NoError(t, err)

		redpandaID, err := os.ReadFile(filepath.Join(tmp, redpandaIDFile))
		require.NoError(t, err)

		rpID, err := strconv.Atoi(string(redpandaID))
		require.NoError(t, err)

		assert.Equal(t, storedRedpandaID, cfg.Redpanda.ID)
		assert.Equal(t, rpID, cfg.Redpanda.ID)
		assert.NotEqual(t, statefulsetOrdinal, cfg.Redpanda.ID)
	})
}

func TestInitSeedServerList(t *testing.T) {
	t.Run("empty Redpanda data folder", func(t *testing.T) {
		t.Run("first POD", func(t *testing.T) {
			tmp := t.TempDir()

			statefulsetOrdinal := 0

			cfg := config.Config{
				Redpanda: config.RedpandaConfig{
					SeedServers: []config.SeedServer{
						{
							Host: config.SocketAddress{
								Address: "testAddress",
								Port:    768,
							},
						},
					},
				},
			}
			err := initializeSeedSeverList(&cfg,
				configuratorConfig{
					dataDirPath: tmp,
				},
				brokerID(statefulsetOrdinal))
			assert.NoError(t, err)

			assert.Len(t, cfg.Redpanda.SeedServers, 0)
		})

		t.Run("not first POD", func(t *testing.T) {
			tmp := t.TempDir()

			statefulsetOrdinal := 2

			cfg := config.Config{
				Redpanda: config.RedpandaConfig{
					SeedServers: []config.SeedServer{
						{
							Host: config.SocketAddress{
								Address: "testAddress",
								Port:    768,
							},
						},
					},
				},
			}
			err := initializeSeedSeverList(&cfg,
				configuratorConfig{
					dataDirPath: tmp,
				},
				brokerID(statefulsetOrdinal))
			assert.NoError(t, err)

			assert.Len(t, cfg.Redpanda.SeedServers, 1)
		})
	})

	t.Run("Fake Redpanda data folder that is not empty", func(t *testing.T) {
		t.Run("first POD", func(t *testing.T) {
			tmp := t.TempDir()
			err := os.WriteFile(filepath.Join(tmp, "test"), []byte("test"), 0o666)
			require.NoError(t, err)

			statefulsetOrdinal := 0

			cfg := config.Config{
				Redpanda: config.RedpandaConfig{
					SeedServers: []config.SeedServer{
						{
							Host: config.SocketAddress{
								Address: "testAddress",
								Port:    768,
							},
						},
					},
				},
			}
			err = initializeSeedSeverList(&cfg,
				configuratorConfig{
					dataDirPath: tmp,
				},
				brokerID(statefulsetOrdinal))
			assert.NoError(t, err)

			assert.Len(t, cfg.Redpanda.SeedServers, 1)
		})

		t.Run("not first POD", func(t *testing.T) {
			tmp := t.TempDir()
			err := os.WriteFile(filepath.Join(tmp, "test"), []byte("test"), 0o666)
			require.NoError(t, err)

			statefulsetOrdinal := 2

			cfg := config.Config{
				Redpanda: config.RedpandaConfig{
					SeedServers: []config.SeedServer{
						{
							Host: config.SocketAddress{
								Address: "testAddress",
								Port:    768,
							},
						},
					},
				},
			}
			err = initializeSeedSeverList(&cfg,
				configuratorConfig{
					dataDirPath: tmp,
				},
				brokerID(statefulsetOrdinal))
			assert.NoError(t, err)

			assert.Len(t, cfg.Redpanda.SeedServers, 1)
		})
	})
}
