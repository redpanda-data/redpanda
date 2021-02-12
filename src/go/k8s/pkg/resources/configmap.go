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
	"fmt"
	"path/filepath"

	"github.com/go-logr/logr"
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

// Config is the smallest subset of rpk config
type Config struct {
	ConfigFile	string		`yaml:"config_file" mapstructure:"config_file" json:"configFile"`
	Redpanda	RedpandaConfig	`yaml:"redpanda" mapstructure:"redpanda" json:"redpanda"`
}

// RedpandaConfig is the old version copy of the rpk redpanda config
type RedpandaConfig struct {
	Directory		string			`yaml:"data_directory" mapstructure:"data_directory" json:"dataDirectory"`
	RPCServer		config.SocketAddress	`yaml:"rpc_server" mapstructure:"rpc_server" json:"rpcServer"`
	AdvertisedRPCAPI	*config.SocketAddress	`yaml:"advertised_rpc_api,omitempty" mapstructure:"advertised_rpc_api,omitempty" json:"advertisedRpcApi,omitempty"`
	KafkaAPI		config.SocketAddress	`yaml:"kafka_api" mapstructure:"kafka_api" json:"kafkaApi"`
	AdvertisedKafkaAPI	config.SocketAddress	`yaml:"advertised_kafka_api,omitempty" mapstructure:"advertised_kafka_api,omitempty" json:"advertisedKafkaApi,omitempty"`
	KafkaAPITLS		config.ServerTLS	`yaml:"kafka_api_tls,omitempty" mapstructure:"kafka_api_tls,omitempty" json:"kafkaApiTls"`
	AdminAPI		config.SocketAddress	`yaml:"admin" mapstructure:"admin" json:"admin"`
	ID			int			`yaml:"node_id" mapstructure:"node_id" json:"id"`
	SeedServers		[]config.SeedServer	`yaml:"seed_servers" mapstructure:"seed_servers" json:"seedServers"`
	DeveloperMode		bool			`yaml:"developer_mode" mapstructure:"developer_mode" json:"developerMode"`
}

// ConfigMapResource contains definition and reconciliation logic for operator's ConfigMap.
// The ConfigMap contains the configuration as well as init script.
type ConfigMapResource struct {
	k8sclient.Client
	scheme		*runtime.Scheme
	pandaCluster	*redpandav1alpha1.Cluster

	svc				*ServiceResource
	polymorphicAdvertisedAPI	bool
	logger				logr.Logger
}

// NewConfigMap creates ConfigMapResource
func NewConfigMap(
	client k8sclient.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
	svc *ServiceResource,
	polymorphicAdvertisedAPI bool,
	logger logr.Logger,
) *ConfigMapResource {
	return &ConfigMapResource{
		client, scheme, pandaCluster, svc, polymorphicAdvertisedAPI, logger.WithValues("Kind", configMapKind()),
	}
}

// Ensure will manage kubernetes v1.ConfigMap for redpanda.vectorized.io CR
//nolint:dupl // we expect this to not be duplicated when more logic is added
func (r *ConfigMapResource) Ensure(ctx context.Context) error {
	var cfgm corev1.ConfigMap

	err := r.Get(ctx, r.Key(), &cfgm)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	if errors.IsNotFound(err) {
		r.logger.Info(fmt.Sprintf("ConfigMap %s does not exist, going to create one", r.Key().Name))

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
	serviceAddress := r.svc.HeadlessServiceFQDN()

	cfgBytes, err := yaml.Marshal(r.createConfiguration())
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
		rpk --config $CONFIG config set redpanda.advertised_kafka_api.address $SERVICE_NAME;
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

func (r *ConfigMapResource) createConfiguration() interface{} {
	serviceAddress := r.pandaCluster.Name + "." + r.pandaCluster.Namespace + ".svc.cluster.local"
	cfgRpk := config.Default()

	c := r.pandaCluster.Spec.Configuration
	cr := cfgRpk.Redpanda

	if !r.polymorphicAdvertisedAPI {
		defaultKafkaAPIPort := 0
		if len(cfgRpk.Redpanda.KafkaApi) > 0 {
			defaultKafkaAPIPort = cfgRpk.Redpanda.KafkaApi[0].Port
		}

		return &Config{
			ConfigFile:	configPath,
			Redpanda: RedpandaConfig{
				RPCServer: config.SocketAddress{
					Address:	"0.0.0.0",
					Port:		getPort(c.RPCServer.Port, cr.RPCServer.Port),
				},
				AdvertisedRPCAPI: &config.SocketAddress{
					Address:	"0.0.0.0",
					Port:		getPort(c.RPCServer.Port, cr.RPCServer.Port),
				},
				KafkaAPI: config.SocketAddress{
					Address:	"0.0.0.0",
					Port:		getPort(c.KafkaAPI.Port, defaultKafkaAPIPort),
				},
				AdvertisedKafkaAPI: config.SocketAddress{
					Address:	"0.0.0.0",
					Port:		getPort(c.KafkaAPI.Port, defaultKafkaAPIPort),
				},
				AdminAPI: config.SocketAddress{
					Address:	"0.0.0.0",
					Port:		getPort(c.AdminAPI.Port, cr.AdminApi.Port),
				},
				DeveloperMode:	c.DeveloperMode,
				ID:		0,
				SeedServers: []config.SeedServer{
					{
						Host: config.SocketAddress{
							// Example address: cluster-sample-0.cluster-sample.default.svc.cluster.local
							Address:	r.pandaCluster.Name + "-0." + serviceAddress,
							Port:		getPort(c.RPCServer.Port, cr.RPCServer.Port),
						},
					},
				},
				Directory:	dataDirectory,
			},
		}
	}

	cr.RPCServer.Port = getPort(c.RPCServer.Port, cr.RPCServer.Port)
	cr.AdvertisedRPCAPI.Port = getPort(c.RPCServer.Port, cr.AdvertisedRPCAPI.Port)
	cr.AdminApi.Port = getPort(c.AdminAPI.Port, cr.AdminApi.Port)
	cr.RPCServer.Port = getPort(c.RPCServer.Port, cr.RPCServer.Port)
	cr.DeveloperMode = c.DeveloperMode
	cr.SeedServers = []config.SeedServer{
		{
			Host: config.SocketAddress{
				// Example address: cluster-sample-0.cluster-sample.default.svc.cluster.local
				Address:	r.pandaCluster.Name + "-0." + serviceAddress,
				Port:		getPort(c.RPCServer.Port, cr.AdvertisedRPCAPI.Port),
			},
		},
	}
	cr.Directory = dataDirectory

	return cr
}

func getPort(clusterPort, defaultPort int) int {
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
