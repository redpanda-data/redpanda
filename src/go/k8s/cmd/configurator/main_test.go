package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
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
	err := redpandav1alpha1.AddToScheme(scheme.Scheme)
	assert.NoError(t, err)

	t.Run("empty Redpanda data folder", func(t *testing.T) {
		t.Run("first POD", func(t *testing.T) {
			statefulsetOrdinal := 0
			t.Run("with more than 1 pod", func(t *testing.T) {
				replicas := int32(3)
				t.Run("without other pods running from Redpanda cluster", func(t *testing.T) {
					tmp := t.TempDir()

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

					c := fake.NewClientBuilder().WithObjects(&appsv1.StatefulSet{
						ObjectMeta: metav1.ObjectMeta{
							Name: "testHostName",
						},
						Spec: appsv1.StatefulSetSpec{
							Replicas: &replicas,
						},
					}).Build()

					err = initializeSeedSeverList(&cfg,
						configuratorConfig{
							dataDirPath: tmp,
							hostName:    "testHostName-1",
						},
						brokerID(statefulsetOrdinal), c)
					assert.NoError(t, err)

					assert.Len(t, cfg.Redpanda.SeedServers, 0)
				})

				t.Run("with one running pod from Redpanda cluster without TLS", func(t *testing.T) {
					tmp := t.TempDir()

					srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.Write([]byte(`[
  {
    "node_id": 0,
    "num_cores": 2,
    "membership_status": "active",
    "is_alive": false,
    "disk_space": [
      {
        "path": "/var/lib/redpanda/data",
        "free": 1103825203200,
        "total": 1249389649920
      }
    ],
    "version": "v21.11.15 - 7325762b6f9e1586efc60ab97b8596f08510b31a-dirty"
  },
  {
    "node_id": 1,
    "num_cores": 2,
    "membership_status": "active",
    "is_alive": true,
    "disk_space": [
      {
        "path": "/var/lib/redpanda/data",
        "free": 1103853776896,
        "total": 1249389649920
      }
    ],
    "version": "v21.11.15 - 7325762b6f9e1586efc60ab97b8596f08510b31a-dirty"
  },
  {
    "node_id": 2,
    "num_cores": 2,
    "membership_status": "active",
    "is_alive": true,
    "disk_space": [
      {
        "path": "/var/lib/redpanda/data",
        "free": 1117177769984,
        "total": 1249389649920
      }
    ],
    "version": "v21.11.15 - 7325762b6f9e1586efc60ab97b8596f08510b31a-dirty"
  }
]`))
					}))
					defer srv.Close()

					add := strings.Split(srv.Listener.Addr().String(), ":")
					port, err := strconv.Atoi(add[1])
					assert.NoError(t, err)

					cfg := config.Config{
						Redpanda: config.RedpandaConfig{
							SeedServers: []config.SeedServer{
								{
									Host: config.SocketAddress{
										Address: add[0],
										Port:    port,
									},
								},
							},
						},
					}

					c := fake.NewClientBuilder().WithObjects(&appsv1.StatefulSet{
						ObjectMeta: metav1.ObjectMeta{
							Name: "testHostName",
						},
						Spec: appsv1.StatefulSetSpec{
							Replicas: &replicas,
						},
					}, &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								labels.InstanceKey: "testHostName",
							},
						},
						Status: v1.PodStatus{
							Phase: v1.PodRunning,
						},
					}, &redpandav1alpha1.Cluster{
						ObjectMeta: metav1.ObjectMeta{
							Name: "testHostName",
						},
						Spec: redpandav1alpha1.ClusterSpec{
							Configuration: redpandav1alpha1.RedpandaConfig{
								AdminAPI: []redpandav1alpha1.AdminAPI{
									{
										Port: 9644,
										TLS: redpandav1alpha1.AdminAPITLS{
											Enabled: false,
										},
									},
								},
							},
						},
						Status: redpandav1alpha1.ClusterStatus{},
					}).Build()

					err = initializeSeedSeverList(&cfg,
						configuratorConfig{
							dataDirPath: tmp,
							hostName:    "testHostName-1",
						},
						brokerID(statefulsetOrdinal), c)
					assert.NoError(t, err)

					assert.Len(t, cfg.Redpanda.SeedServers, 1)
				})

				t.Run("with one running pod from Redpanda cluster with TLS", func(t *testing.T) {
					tmp := t.TempDir()

					srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.Write([]byte(`[
  {
    "node_id": 0,
    "num_cores": 2,
    "membership_status": "active",
    "is_alive": false,
    "disk_space": [
      {
        "path": "/var/lib/redpanda/data",
        "free": 1103825203200,
        "total": 1249389649920
      }
    ],
    "version": "v21.11.15 - 7325762b6f9e1586efc60ab97b8596f08510b31a-dirty"
  },
  {
    "node_id": 1,
    "num_cores": 2,
    "membership_status": "active",
    "is_alive": true,
    "disk_space": [
      {
        "path": "/var/lib/redpanda/data",
        "free": 1103853776896,
        "total": 1249389649920
      }
    ],
    "version": "v21.11.15 - 7325762b6f9e1586efc60ab97b8596f08510b31a-dirty"
  },
  {
    "node_id": 2,
    "num_cores": 2,
    "membership_status": "active",
    "is_alive": true,
    "disk_space": [
      {
        "path": "/var/lib/redpanda/data",
        "free": 1117177769984,
        "total": 1249389649920
      }
    ],
    "version": "v21.11.15 - 7325762b6f9e1586efc60ab97b8596f08510b31a-dirty"
  }
]`))
					}))
					defer srv.Close()

					add := strings.Split(srv.Listener.Addr().String(), ":")
					port, err := strconv.Atoi(add[1])
					assert.NoError(t, err)

					cfg := config.Config{
						Redpanda: config.RedpandaConfig{
							SeedServers: []config.SeedServer{
								{
									Host: config.SocketAddress{
										Address: add[0],
										Port:    port,
									},
								},
							},
						},
					}

					c := fake.NewClientBuilder().WithObjects(&appsv1.StatefulSet{
						ObjectMeta: metav1.ObjectMeta{
							Name: "testHostName",
						},
						Spec: appsv1.StatefulSetSpec{
							Replicas: &replicas,
						},
					}, &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								labels.InstanceKey: "testHostName",
							},
						},
						Status: v1.PodStatus{
							Phase: v1.PodRunning,
						},
					}, &redpandav1alpha1.Cluster{
						ObjectMeta: metav1.ObjectMeta{
							Name: "testHostName",
						},
						Spec: redpandav1alpha1.ClusterSpec{
							Configuration: redpandav1alpha1.RedpandaConfig{
								AdminAPI: []redpandav1alpha1.AdminAPI{
									{
										Port: 9644,
										TLS: redpandav1alpha1.AdminAPITLS{
											Enabled: true,
										},
									},
								},
							},
						},
						Status: redpandav1alpha1.ClusterStatus{},
					}).Build()

					err = initializeSeedSeverList(&cfg,
						configuratorConfig{
							dataDirPath: tmp,
							hostName:    "testHostName-1",
						},
						brokerID(statefulsetOrdinal), c)
					assert.NoError(t, err)

					assert.Len(t, cfg.Redpanda.SeedServers, 1)
				})
			})
		})

		t.Run("not first POD", func(t *testing.T) {
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
			c := fake.NewClientBuilder().Build()
			err := initializeSeedSeverList(&cfg,
				configuratorConfig{},
				brokerID(statefulsetOrdinal), c)
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
			c := fake.NewClientBuilder().Build()
			err = initializeSeedSeverList(&cfg,
				configuratorConfig{
					dataDirPath: tmp,
					hostName:    "testHostName-1",
				},
				brokerID(statefulsetOrdinal), c)
			assert.NoError(t, err)

			assert.Len(t, cfg.Redpanda.SeedServers, 1)
		})

		t.Run("not first POD", func(t *testing.T) {
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
			c := fake.NewClientBuilder().Build()
			err := initializeSeedSeverList(&cfg,
				configuratorConfig{},
				brokerID(statefulsetOrdinal), c)
			assert.NoError(t, err)

			assert.Len(t, cfg.Redpanda.SeedServers, 1)
		})
	})
}

func TestRenderStsName(t *testing.T) {
	podName := "node-1"
	assert.Equal(t, "node", renderStsName(podName, 1))
}
