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
	"errors"
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
	"k8s.io/utils/pointer"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	baseSuffix    = "-base"
	dataDirectory = "/var/lib/redpanda/data"

	tlsDir   = "/etc/tls/certs"
	tlsDirCA = "/etc/tls/certs/ca"

	tlsAdminDir = "/etc/tls/certs/admin"
)

var errKeyDoesNotExistInSecretData = errors.New("cannot find key in secret data")
var errCloudStorageSecretKeyCannotBeEmpty = errors.New("cloud storage SecretKey string cannot be empty")

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
	obj, err := r.obj(ctx)
	if err != nil {
		return fmt.Errorf("unable to construct object: %w", err)
	}
	created, err := CreateIfNotExists(ctx, r, obj, r.logger)
	if err != nil || created {
		return err
	}
	var cm corev1.ConfigMap
	err = r.Get(ctx, r.Key(), &cm)
	if err != nil {
		return fmt.Errorf("error while fetching ConfigMap resource: %w", err)
	}
	return Update(ctx, &cm, obj, r.Client, r.logger)
}

// obj returns resource managed client.Object
func (r *ConfigMapResource) obj(ctx context.Context) (k8sclient.Object, error) {
	conf, err := r.createConfiguration(ctx)
	if err != nil {
		return nil, err
	}

	cfgBytes, err := yaml.Marshal(conf)
	if err != nil {
		return nil, err
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.Key().Namespace,
			Name:      r.Key().Name,
			Labels:    labels.ForCluster(r.pandaCluster),
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
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

// nolint:funlen // let's keep the configuration in one function for now and refactor later
func (r *ConfigMapResource) createConfiguration(
	ctx context.Context,
) (*config.Config, error) {
	cfgRpk := config.Default()

	c := r.pandaCluster.Spec.Configuration
	cr := &cfgRpk.Redpanda

	// I think this block would only be useful if the configurator didn't run. Should we remove it?
	for _, listener := range c.KafkaAPI {
		if listener.External.Enabled {
			continue
		}
		cr.KafkaApi = append(cr.KafkaApi, config.NamedSocketAddress{
			SocketAddress: config.SocketAddress{
				Address: "0.0.0.0",
				Port:    listener.Port,
			},
			Name: listener.Name,
		})
	}

	internalListener := r.pandaCluster.InternalListener()
	if r.pandaCluster.ExternalListener() != nil {
		cr.KafkaApi = append(cr.KafkaApi, config.NamedSocketAddress{
			SocketAddress: config.SocketAddress{
				Address: "0.0.0.0",
				Port:    calculateExternalPort(internalListener.Port),
			},
			Name: "External",
		})
	}

	cr.RPCServer.Port = clusterCRPortOrRPKDefault(c.RPCServer.Port, cr.RPCServer.Port)
	cr.AdvertisedRPCAPI = &config.SocketAddress{
		Address: "0.0.0.0",
		Port:    clusterCRPortOrRPKDefault(c.RPCServer.Port, cr.RPCServer.Port),
	}

	cr.AdminApi.Port = clusterCRPortOrRPKDefault(c.AdminAPI.Port, cr.AdminApi.Port)
	cr.DeveloperMode = c.DeveloperMode
	cr.Directory = dataDirectory
	tlsListener := r.pandaCluster.KafkaTLSListener()
	if tlsListener != nil {
		// If external connectivity is enabled the TLS config will be applied to the external listener,
		// otherwise TLS will be applied to the internal listener. // TODO support multiple TLS configs
		name := "Internal"
		externalListener := r.pandaCluster.ExternalListener()
		if externalListener != nil {
			name = externalListener.Name
		}
		tls := config.ServerTLS{
			Name:              name,
			KeyFile:           fmt.Sprintf("%s/%s", tlsDir, corev1.TLSPrivateKeyKey), // tls.key
			CertFile:          fmt.Sprintf("%s/%s", tlsDir, corev1.TLSCertKey),       // tls.crt
			Enabled:           true,
			RequireClientAuth: tlsListener.TLS.RequireClientAuth,
		}
		if tlsListener.TLS.RequireClientAuth {
			tls.TruststoreFile = fmt.Sprintf("%s/%s", tlsDirCA, cmetav1.TLSCAKey)
		}
		cr.KafkaApiTLS = []config.ServerTLS{
			tls,
		}
	}
	if r.pandaCluster.Spec.Configuration.TLS.AdminAPI.Enabled {
		cr.AdminApiTLS = config.ServerTLS{
			KeyFile:           fmt.Sprintf("%s/%s", tlsAdminDir, corev1.TLSPrivateKeyKey),
			CertFile:          fmt.Sprintf("%s/%s", tlsAdminDir, corev1.TLSCertKey),
			Enabled:           true,
			RequireClientAuth: r.pandaCluster.Spec.Configuration.TLS.AdminAPI.RequireClientAuth,
		}
		if r.pandaCluster.Spec.Configuration.TLS.AdminAPI.RequireClientAuth {
			cr.AdminApiTLS.TruststoreFile = fmt.Sprintf("%s/%s", tlsAdminDir, cmetav1.TLSCAKey)
		}
	}

	if r.pandaCluster.Spec.CloudStorage.Enabled {
		secretName := types.NamespacedName{
			Name:      r.pandaCluster.Spec.CloudStorage.SecretKeyRef.Name,
			Namespace: r.pandaCluster.Spec.CloudStorage.SecretKeyRef.Namespace,
		}
		// We need to retrieve the Secret containing the provided cloud storage secret key and extract the key itself.
		secretKeyStr, err := r.getSecretValue(ctx, secretName, r.pandaCluster.Spec.CloudStorage.SecretKeyRef.Name)
		if err != nil {
			return nil, fmt.Errorf("cannot retrieve cloud storage secret for data archival: %w", err)
		}
		if secretKeyStr == "" {
			return nil, fmt.Errorf("secret name %s, ns %s: %w", secretName.Name, secretName.Namespace, errCloudStorageSecretKeyCannotBeEmpty)
		}
		r.prepareCloudStorage(cr, secretKeyStr)
	}

	for _, user := range r.pandaCluster.Spec.Superusers {
		cr.Superusers = append(cr.Superusers, user.Username)
	}

	if r.pandaCluster.Spec.EnableSASL {
		cr.EnableSASL = pointer.BoolPtr(true)
	}

	partitions := r.pandaCluster.Spec.Configuration.GroupTopicPartitions
	if partitions != 0 {
		cr.GroupTopicPartitions = &partitions
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

	return cfgRpk, nil
}

// calculateExternalPort can calculate external Kafka API port based on the internal Kafka API port
func calculateExternalPort(kafkaInternalPort int) int {
	if kafkaInternalPort < 0 || kafkaInternalPort > 65535 {
		return 0
	}
	return kafkaInternalPort + 1
}

func (r *ConfigMapResource) prepareCloudStorage(
	cr *config.RedpandaConfig, secretKeyStr string,
) {
	cr.CloudStorageEnabled = pointer.BoolPtr(r.pandaCluster.Spec.CloudStorage.Enabled)
	cr.CloudStorageAccessKey = pointer.StringPtr(r.pandaCluster.Spec.CloudStorage.AccessKey)
	cr.CloudStorageRegion = pointer.StringPtr(r.pandaCluster.Spec.CloudStorage.Region)
	cr.CloudStorageBucket = pointer.StringPtr(r.pandaCluster.Spec.CloudStorage.Bucket)
	cr.CloudStorageSecretKey = pointer.StringPtr(secretKeyStr)
	cr.CloudStorageDisableTls = pointer.BoolPtr(r.pandaCluster.Spec.CloudStorage.DisableTLS)

	interval := r.pandaCluster.Spec.CloudStorage.ReconcilicationIntervalMs
	if interval != 0 {
		cr.CloudStorageReconciliationIntervalMs = &interval
	}
	maxCon := r.pandaCluster.Spec.CloudStorage.MaxConnections
	if maxCon != 0 {
		cr.CloudStorageMaxConnections = &maxCon
	}
	apiEndpoint := r.pandaCluster.Spec.CloudStorage.APIEndpoint
	if apiEndpoint != "" {
		cr.CloudStorageApiEndpoint = &apiEndpoint
	}
	endpointPort := r.pandaCluster.Spec.CloudStorage.APIEndpointPort
	if endpointPort != 0 {
		cr.CloudStorageApiEndpointPort = &endpointPort
	}
	trustfile := r.pandaCluster.Spec.CloudStorage.Trustfile
	if trustfile != "" {
		cr.CloudStorageTrustFile = &trustfile
	}
}

func (r *ConfigMapResource) getSecretValue(
	ctx context.Context, nsName types.NamespacedName, key string,
) (string, error) {
	var secret corev1.Secret
	err := r.Get(ctx, nsName, &secret)
	if err != nil {
		return "", err
	}

	if v, exists := secret.Data[key]; exists {
		return string(v), nil
	}

	return "", fmt.Errorf("secret name %s, ns %s, data key %s: %w", nsName.Name, nsName.Namespace, key, errKeyDoesNotExistInSecretData)
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

func configMapKind() string {
	var cfg corev1.ConfigMap
	return cfg.Kind
}
