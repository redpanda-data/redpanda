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
	"strconv"

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
	nodeIPsList		= "node-list.txt"
	confFile		= "redpanda.yaml"
)

var (
	configPath		= filepath.Join(configDir, confFile)
	configuratorPath	= filepath.Join(configuratorDir, configuratorScript)
	nodeListPath		= filepath.Join(configuratorDir, nodeIPsList)
)

// NodesLister defines necessary function to construct list of nodes
// internal and external IPs
type NodesLister interface {
	// RegisterConfigMap start updating config map nodeIPsList file
	// based on the changes in nodes
	RegisterConfigMap(types.NamespacedName) error
}

var _ Resource = &ConfigMapResource{}

// ConfigMapResource contains definition and reconciliation logic for operator's ConfigMap.
// The ConfigMap contains the configuration as well as init script.
type ConfigMapResource struct {
	k8sclient.Client
	scheme		*runtime.Scheme
	pandaCluster	*redpandav1alpha1.Cluster
	serviceFQDN	string
	nodesLister	NodesLister
	logger		logr.Logger
	nodePort	int32
}

// NewConfigMap creates ConfigMapResource
func NewConfigMap(
	client k8sclient.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
	serviceFQDN string,
	nodesLister NodesLister,
	logger logr.Logger,
) *ConfigMapResource {
	return &ConfigMapResource{
		client,
		scheme,
		pandaCluster,
		serviceFQDN,
		nodesLister,
		logger.WithValues("Kind", configMapKind()),
		0,
	}
}

// Ensure will manage kubernetes v1.ConfigMap for redpanda.vectorized.io CR
func (r *ConfigMapResource) Ensure(ctx context.Context) error {
	var cfgm corev1.ConfigMap

	if r.pandaCluster.Spec.ExternalConnectivity {
		svc := corev1.Service{}
		err := r.Get(ctx, (&NodePortServiceResource{pandaCluster: r.pandaCluster}).Key(), &svc)
		if err != nil {
			return fmt.Errorf("unable to find node port service: %w", err)
		}

		r.nodePort = getNodePort(&svc)
	}

	err := r.Get(ctx, r.Key(), &cfgm)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	if !errors.IsNotFound(err) {
		return nil
	}

	r.logger.Info(fmt.Sprintf("ConfigMap %s does not exist, going to create one", r.Key().Name))

	obj, err := r.Obj()
	if err != nil {
		return err
	}

	err = r.Create(ctx, obj)
	if err != nil {
		return fmt.Errorf("unable to create configmap: %w", err)
	}
	if r.pandaCluster.Spec.ExternalConnectivity {
		if err := r.nodesLister.RegisterConfigMap(r.Key()); err != nil {
			return fmt.Errorf("unable to register config map in node watcher: %w", err)
		}
	}

	return nil
}

// Obj returns resource managed client.Object
func (r *ConfigMapResource) Obj() (k8sclient.Object, error) {
	serviceAddress := r.serviceFQDN

	cfgBytes, err := yaml.Marshal(r.createConfiguration())
	if err != nil {
		return nil, err
	}

	kafkaInternalPort, kafkaExternalPort := r.getKafkaAdvertisedPorts()

	script :=
		`set -xe;
		CONFIG=` + configPath + `;
		ORDINAL_INDEX=${HOSTNAME##*-};
		SERVICE_NAME=${HOSTNAME}.` + serviceAddress + `
		cp ` + filepath.Join(configuratorDir, confFile) + ` $CONFIG;
		rpk --config $CONFIG config set redpanda.node_id $ORDINAL_INDEX;
		if [ "$ORDINAL_INDEX" = "0" ]; then
			rpk --config $CONFIG config set redpanda.seed_servers '[]' --format yaml;
		fi;`
	var advertisedKafkaAPIScript string
	if r.pandaCluster.Spec.ExternalConnectivity {
		advertisedKafkaAPIScript = `
			EXTERNAL_IP=$(cat ` + nodeListPath + ` | grep $HOST_IP | awk '{print $2}')
			rpk --config $CONFIG config set redpanda.advertised_kafka_api --format json '[{"name": "external","address":"'$EXTERNAL_IP'","port":` + kafkaExternalPort + `},{"name": "internal","address":"'$SERVICE_NAME'","port":` + kafkaInternalPort + `}]'
			cat $CONFIG`
	} else {
		advertisedKafkaAPIScript = `
			rpk --config $CONFIG config set redpanda.advertised_kafka_api --format json '[{"name": "internal","address":"'$SERVICE_NAME'","port":` + kafkaInternalPort + `}]'
			cat $CONFIG`
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:	r.Key().Namespace,
			Name:		r.Key().Name,
			Labels:		labels.ForCluster(r.pandaCluster),
		},
		Data: map[string]string{
			"redpanda.yaml":	string(cfgBytes),
			"configurator.sh":	script + advertisedKafkaAPIScript,
		},
	}

	err = controllerutil.SetControllerReference(r.pandaCluster, cm, r.scheme)
	if err != nil {
		return nil, err
	}

	return cm, nil
}

func (r *ConfigMapResource) getKafkaAdvertisedPorts() (
	internalPort, externalPort string,
) {
	return strconv.Itoa(r.pandaCluster.Spec.Configuration.KafkaAPI.Port), strconv.Itoa(int(r.nodePort))
}

func (r *ConfigMapResource) createConfiguration() *config.Config {
	serviceAddress := r.serviceFQDN
	cfgRpk := config.Default()

	c := r.pandaCluster.Spec.Configuration
	cr := &cfgRpk.Redpanda

	cr.KafkaApi = []config.NamedSocketAddress{
		{
			SocketAddress: config.SocketAddress{
				Address:	"0.0.0.0",
				Port:		c.KafkaAPI.Port,
			},
			Name:	"internal",
		},
	}
	cr.AdvertisedKafkaApi = []config.NamedSocketAddress{
		{
			SocketAddress: config.SocketAddress{
				Address:	"0.0.0.0",
				Port:		c.KafkaAPI.Port,
			},
			Name:	"internal",
		},
	}

	cr.RPCServer.Port = clusterCRPortOrRPKDefault(c.RPCServer.Port, cr.RPCServer.Port)
	cr.AdvertisedRPCAPI = &config.SocketAddress{
		Address:	"0.0.0.0",
		Port:		clusterCRPortOrRPKDefault(c.RPCServer.Port, cr.RPCServer.Port),
	}

	cr.AdminApi.Port = clusterCRPortOrRPKDefault(c.AdminAPI.Port, cr.AdminApi.Port)
	cr.DeveloperMode = c.DeveloperMode
	cr.SeedServers = []config.SeedServer{
		{
			Host: config.SocketAddress{
				// Example address: cluster-sample-0.cluster-sample.default.svc.cluster.local
				Address:	r.pandaCluster.Name + "-0." + serviceAddress,
				Port:		clusterCRPortOrRPKDefault(c.RPCServer.Port, cr.RPCServer.Port),
			},
		},
	}
	cr.Directory = dataDirectory

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
