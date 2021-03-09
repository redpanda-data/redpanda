// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package resources

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	cmetav1 "github.com/jetstack/cert-manager/pkg/apis/meta/v1"
	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/labels"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	baseSuffix    = "-base"
	dataDirectory = "/var/lib/redpanda/data"

	tlsDir = "/etc/tls/certs"
)

var _ Resource = &ConfigMapResource{}

// ConfigMapResource contains definition and reconciliation logic for operator's ConfigMap.
// The ConfigMap contains the configuration as well as init script.
type ConfigMapResource struct {
	k8sclient.Client
	scheme       *runtime.Scheme
	pandaCluster *redpandav1alpha1.Cluster

	serviceFQDN string
	logger      logr.Logger
}

// NewConfigMap creates ConfigMapResource
func NewConfigMap(
	client k8sclient.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
	serviceFQDN string,
	logger logr.Logger,
) *ConfigMapResource {
	return &ConfigMapResource{
		client,
		scheme,
		pandaCluster,
		serviceFQDN,
		logger.WithValues("Kind", configMapKind()),
	}
}

// Ensure will manage kubernetes v1.ConfigMap for redpanda.vectorized.io CR
func (r *ConfigMapResource) Ensure(ctx context.Context) error {
	return GetOrCreate(ctx, r, &corev1.ConfigMap{}, "ConfigMap", r.logger)
}

// Obj returns resource managed client.Object
func (r *ConfigMapResource) Obj() (k8sclient.Object, error) {
	cfgBytes, err := yaml.Marshal(r.createConfiguration())
	if err != nil {
		return nil, err
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.Key().Namespace,
			Name:      r.Key().Name,
			Labels:    labels.ForCluster(r.pandaCluster),
		},
		Data: map[string]string{
			"redpanda.yaml": string(cfgBytes),
		},
	}

	err = controllerutil.SetControllerReference(r.pandaCluster, cm, r.scheme)
	if err != nil {
		return nil, err
	}

	return cm, nil
}

func (r *ConfigMapResource) createConfiguration() *config.Config {
	cfgRpk := config.Default()

	c := r.pandaCluster.Spec.Configuration
	cr := &cfgRpk.Redpanda

	cr.KafkaApi = []config.NamedSocketAddress{
		{
			SocketAddress: config.SocketAddress{
				Address: "0.0.0.0",
				Port:    c.KafkaAPI.Port,
			},
			Name: "Internal",
		},
	}

	cr.RPCServer.Port = clusterCRPortOrRPKDefault(c.RPCServer.Port, cr.RPCServer.Port)
	cr.AdvertisedRPCAPI = &config.SocketAddress{
		Address: "0.0.0.0",
		Port:    clusterCRPortOrRPKDefault(c.RPCServer.Port, cr.RPCServer.Port),
	}

	cr.AdminApi.Port = clusterCRPortOrRPKDefault(c.AdminAPI.Port, cr.AdminApi.Port)
	cr.DeveloperMode = c.DeveloperMode
	cr.Directory = dataDirectory
	if r.pandaCluster.Spec.Configuration.TLS.KafkaAPIEnabled {
		cr.KafkaApiTLS = config.ServerTLS{
			KeyFile:           fmt.Sprintf("%s/%s", tlsDir, corev1.TLSPrivateKeyKey), // tls.key
			CertFile:          fmt.Sprintf("%s/%s", tlsDir, corev1.TLSCertKey),       // tls.crt
			TruststoreFile:    fmt.Sprintf("%s/%s", tlsDir, cmetav1.TLSCAKey),
			Enabled:           true,
			RequireClientAuth: r.pandaCluster.Spec.Configuration.TLS.RequireClientAuth,
		}
	}

	replicas := *r.pandaCluster.Spec.Replicas
	for i := int32(0); i < replicas; i++ {
		cr.SeedServers = append(cr.SeedServers, config.SeedServer{
			Host: config.SocketAddress{
				// Example address: cluster-sample-0.cluster-sample.default.svc.cluster.local
				Address: fmt.Sprintf("%s-%d.%s", r.pandaCluster.Name, i, r.serviceFQDN),
				Port:    clusterCRPortOrRPKDefault(c.RPCServer.Port, cr.RPCServer.Port),
			},
		})
	}

	return cfgRpk
}

func clusterCRPortOrRPKDefault(clusterPort, defaultPort int) int {
	if clusterPort == 0 {
		return defaultPort
	}

	return clusterPort
}

// Key returns namespace/name object that is used to identify object.
// For reference please visit types.NamespacedName docs in k8s.io/apimachinery
func (r *ConfigMapResource) Key() types.NamespacedName {
	return ConfigMapKey(r.pandaCluster)
}

// ConfigMapKey provides config map name that derived from redpanda.vectorized.io CR
func ConfigMapKey(pandaCluster *redpandav1alpha1.Cluster) types.NamespacedName {
	return types.NamespacedName{Name: pandaCluster.Name + baseSuffix, Namespace: pandaCluster.Namespace}
}

// Kind returns v1.ConfigMap kind
func (r *ConfigMapResource) Kind() string {
	return configMapKind()
}

func configMapKind() string {
	var cfg corev1.ConfigMap
	return cfg.Kind
}
