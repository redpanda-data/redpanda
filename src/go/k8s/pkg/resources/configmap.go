// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package resources contains reconciliation logic for redpanda.vectorized.io CRD
package resources

import (
	"context"
	"path/filepath"
	"strconv"

	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/labels"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	baseSuffix	= "-base"
	dataDirectory	= "/var/lib/redpanda/data"
	fsGroup		= 101

	configDir		= "/etc/redpanda"
	configuratorDir		= "/mnt/operator"
	configuratorScript	= "configurator.sh"
)

var (
	configPath		= filepath.Join(configDir, "redpanda.yaml")
	configuratorPath	= filepath.Join(configuratorDir, configuratorScript)
)

var _ Resource = &ConfigMapResource{}

// ConfigMapResource contains definition and reconciliation logic for operator's ConfigMap.
// The ConfigMap contains the configuration as well as init script.
type ConfigMapResource struct {
	k8sclient.Client
	scheme		*runtime.Scheme
	pandaCluster	*redpandav1alpha1.Cluster
}

// NewConfigMap creates ConfigMapResource
func NewConfigMap(
	client k8sclient.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
) *ConfigMapResource {
	return &ConfigMapResource{
		client, scheme, pandaCluster,
	}
}

// Ensure will manage kubernetes v1.ConfigMap for redpanda.vectorized.io CR
func (r *ConfigMapResource) Ensure(ctx context.Context) error {
	var cfgm corev1.ConfigMap

	err := r.Get(ctx, r.Key(), &cfgm)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	if errors.IsNotFound(err) {
		obj, err := r.Obj()
		if err != nil {
			return err
		}

		return r.Create(ctx, obj)
	}

	return nil
}

// Obj returns resource managed client.Object
func (r *ConfigMapResource) Obj() (k8sclient.Object, error) {
	serviceAddress := r.pandaCluster.Name + "." + r.pandaCluster.Namespace + ".svc.cluster.local"
	cfg := config.Default()
	cfg.Redpanda = copyConfig(&r.pandaCluster.Spec.Configuration, &cfg.Redpanda)
	cfg.Redpanda.Id = 0
	cfg.Redpanda.AdvertisedKafkaApi.Port = cfg.Redpanda.KafkaApi.Port
	cfg.Redpanda.AdvertisedRPCAPI.Port = cfg.Redpanda.RPCServer.Port
	cfg.Redpanda.Directory = dataDirectory
	cfg.Redpanda.SeedServers = []config.SeedServer{
		{
			Host: config.SocketAddress{
				// Example address: cluster-sample-0.cluster-sample.default.svc.cluster.local
				Address:	r.pandaCluster.Name + "-0." + serviceAddress,
				Port:		cfg.Redpanda.AdvertisedRPCAPI.Port,
			},
		},
	}

	cfgBytes, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	script :=
		`set -xe;
		CONFIG=` + configPath + `;
		ORDINAL_INDEX=${HOSTNAME##*-};
		SERVICE_NAME=${HOSTNAME}.` + serviceAddress + `
		cp /mnt/operator/redpanda.yaml $CONFIG;
		rpk --config $CONFIG config set redpanda.node_id $ORDINAL_INDEX;
		if [ "$ORDINAL_INDEX" = "0" ]; then
			rpk --config $CONFIG config set redpanda.seed_servers '[]' --format yaml;
		fi;
		rpk --config $CONFIG config set redpanda.advertised_rpc_api.address $SERVICE_NAME;
		rpk --config $CONFIG config set redpanda.advertised_rpc_api.port ` + strconv.Itoa(cfg.Redpanda.AdvertisedRPCAPI.Port) + `;
		rpk --config $CONFIG config set redpanda.advertised_kafka_api.address $SERVICE_NAME;
		rpk --config $CONFIG config set redpanda.advertised_kafka_api.port ` + strconv.Itoa(cfg.Redpanda.AdvertisedKafkaApi.Port) + `;
		cat $CONFIG`

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:	r.Key().Namespace,
			Name:		r.Key().Name,
			Labels:		labels.ForCluster(r.pandaCluster),
		},
		Data: map[string]string{
			"redpanda.yaml":	string(cfgBytes),
			"configurator.sh":	script,
		},
	}

	err = controllerutil.SetControllerReference(r.pandaCluster, cm, r.scheme)
	if err != nil {
		return nil, err
	}

	return cm, nil
}

func copyConfig(
	c *redpandav1alpha1.RedpandaConfig, cfgDefaults *config.RedpandaConfig,
) config.RedpandaConfig {
	rpcServerPort := c.RPCServer.Port
	if c.RPCServer.Port == 0 {
		rpcServerPort = cfgDefaults.RPCServer.Port
	}

	kafkaAPIPort := c.KafkaAPI.Port
	if c.KafkaAPI.Port == 0 {
		kafkaAPIPort = cfgDefaults.KafkaApi.Port
	}

	AdminAPIPort := c.AdminAPI.Port
	if c.AdminAPI.Port == 0 {
		AdminAPIPort = cfgDefaults.AdminApi.Port
	}

	return config.RedpandaConfig{
		RPCServer: config.SocketAddress{
			Address:	"0.0.0.0",
			Port:		rpcServerPort,
		},
		AdvertisedRPCAPI:	&config.SocketAddress{},
		KafkaApi: config.SocketAddress{
			Address:	"0.0.0.0",
			Port:		kafkaAPIPort,
		},
		AdvertisedKafkaApi:	&config.SocketAddress{},
		AdminApi: config.SocketAddress{
			Address:	"0.0.0.0",
			Port:		AdminAPIPort,
		},
		DeveloperMode:	c.DeveloperMode,
	}
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
	var cfg corev1.ConfigMap
	return cfg.Kind
}
