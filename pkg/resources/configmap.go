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
	"crypto/rand"
	"errors"
	"fmt"
	"strconv"

	"github.com/go-logr/logr"
	cmetav1 "github.com/jetstack/cert-manager/pkg/apis/meta/v1"
	"github.com/spf13/afero"
	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/labels"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources/featuregates"
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
	baseSuffix                  = "base"
	dataDirectory               = "/var/lib/redpanda/data"
	archivalCacheIndexDirectory = "/var/lib/shadow-index-cache"

	cloudStorageCacheDirectory = "cloud_storage_cache_directory"

	// We need 2 secrets and 2 mount points for each API endpoint that supports TLS and mTLS:
	// 1. The Node certs used by the API endpoint to sign requests
	// 2. The CA used to sign mTLS client certs which will be use by the endpoint to validate mTLS client certs
	//
	// Node certs might be signed by a provided Issuer (i.e. when using Letsencrypt), in which case
	// the operator won't generate a CA to sign the node certs. But, if at the same time mTLS is enabled
	// a CA will be created to sign the mTLS client certs, and it will be stored in a separate secret.
	tlsKafkaAPIDir         = "/etc/tls/certs"
	tlsKafkaAPIDirCA       = "/etc/tls/certs/ca"
	tlsAdminAPIDir         = "/etc/tls/certs/admin"
	tlsAdminAPIDirCA       = "/etc/tls/certs/admin/ca"
	tlsPandaproxyAPIDir    = "/etc/tls/certs/pandaproxy"
	tlsPandaproxyAPIDirCA  = "/etc/tls/certs/pandaproxy/ca"
	tlsSchemaRegistryDir   = "/etc/tls/certs/schema-registry"
	tlsSchemaRegistryDirCA = "/etc/tls/certs/schema-registry/ca"

	oneMB          = 1024 * 1024
	logSegmentSize = 512 * oneMB

	saslMechanism = "SCRAM-SHA-256"
)

var (
	errKeyDoesNotExistInSecretData        = errors.New("cannot find key in secret data")
	errCloudStorageSecretKeyCannotBeEmpty = errors.New("cloud storage SecretKey string cannot be empty")
)

var _ Resource = &ConfigMapResource{}

// ConfigMapResource contains definition and reconciliation logic for operator's ConfigMap.
// The ConfigMap contains the configuration as well as init script.
type ConfigMapResource struct {
	k8sclient.Client
	scheme       *runtime.Scheme
	pandaCluster *redpandav1alpha1.Cluster

	serviceFQDN            string
	pandaproxySASLUser     types.NamespacedName
	schemaRegistrySASLUser types.NamespacedName
	logger                 logr.Logger
}

// NewConfigMap creates ConfigMapResource
func NewConfigMap(
	client k8sclient.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
	serviceFQDN string,
	pandaproxySASLUser types.NamespacedName,
	schemaRegistrySASLUser types.NamespacedName,
	logger logr.Logger,
) *ConfigMapResource {
	return &ConfigMapResource{
		client,
		scheme,
		pandaCluster,
		serviceFQDN,
		pandaproxySASLUser,
		schemaRegistrySASLUser,
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
	_, err = Update(ctx, &cm, obj, r.Client, r.logger)
	return err
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

	internalListener := r.pandaCluster.InternalListener()
	cr.KafkaApi = []config.NamedSocketAddress{} // we don't want to inherit default kafka port
	cr.KafkaApi = append(cr.KafkaApi, config.NamedSocketAddress{
		SocketAddress: config.SocketAddress{
			Address: "0.0.0.0",
			Port:    internalListener.Port,
		},
		Name: InternalListenerName,
	})

	if r.pandaCluster.ExternalListener() != nil {
		cr.KafkaApi = append(cr.KafkaApi, config.NamedSocketAddress{
			SocketAddress: config.SocketAddress{
				Address: "0.0.0.0",
				Port:    calculateExternalPort(internalListener.Port),
			},
			Name: ExternalListenerName,
		})
	}

	cr.RPCServer.Port = clusterCRPortOrRPKDefault(c.RPCServer.Port, cr.RPCServer.Port)
	cr.AdvertisedRPCAPI = &config.SocketAddress{
		Address: "0.0.0.0",
		Port:    clusterCRPortOrRPKDefault(c.RPCServer.Port, cr.RPCServer.Port),
	}

	cr.AdminApi[0].Port = clusterCRPortOrRPKDefault(r.pandaCluster.AdminAPIInternal().Port, cr.AdminApi[0].Port)
	cr.AdminApi[0].Name = AdminPortName
	if r.pandaCluster.AdminAPIExternal() != nil {
		externalAdminAPI := config.NamedSocketAddress{
			SocketAddress: config.SocketAddress{
				Address: cr.AdminApi[0].Address,
				Port:    cr.AdminApi[0].Port + 1,
			},
			Name: AdminPortExternalName,
		}
		cr.AdminApi = append(cr.AdminApi, externalAdminAPI)
	}

	cr.DeveloperMode = c.DeveloperMode
	cr.Directory = dataDirectory
	tlsListener := r.pandaCluster.KafkaTLSListener()
	if tlsListener != nil {
		// Only one TLS listener is supported (restricted by the webhook).
		// Determine the listener name based on being internal or external.
		name := InternalListenerName
		if tlsListener.External.Enabled {
			name = ExternalListenerName
		}
		tls := config.ServerTLS{
			Name:              name,
			KeyFile:           fmt.Sprintf("%s/%s", tlsKafkaAPIDir, corev1.TLSPrivateKeyKey), // tls.key
			CertFile:          fmt.Sprintf("%s/%s", tlsKafkaAPIDir, corev1.TLSCertKey),       // tls.crt
			Enabled:           true,
			RequireClientAuth: tlsListener.TLS.RequireClientAuth,
		}
		if tlsListener.TLS.RequireClientAuth {
			tls.TruststoreFile = fmt.Sprintf("%s/%s", tlsKafkaAPIDirCA, cmetav1.TLSCAKey)
		}
		cr.KafkaApiTLS = []config.ServerTLS{
			tls,
		}
	}
	adminAPITLSListener := r.pandaCluster.AdminAPITLS()
	if adminAPITLSListener != nil {
		// Only one TLS listener is supported (restricted by the webhook).
		// Determine the listener name based on being internal or external.
		name := AdminPortName
		if adminAPITLSListener.External.Enabled {
			name = AdminPortExternalName
		}
		adminTLS := config.ServerTLS{
			Name:              name,
			KeyFile:           fmt.Sprintf("%s/%s", tlsAdminAPIDir, corev1.TLSPrivateKeyKey),
			CertFile:          fmt.Sprintf("%s/%s", tlsAdminAPIDir, corev1.TLSCertKey),
			Enabled:           true,
			RequireClientAuth: adminAPITLSListener.TLS.RequireClientAuth,
		}
		if adminAPITLSListener.TLS.RequireClientAuth {
			adminTLS.TruststoreFile = fmt.Sprintf("%s/%s", tlsAdminAPIDirCA, cmetav1.TLSCAKey)
		}
		cr.AdminApiTLS = append(cr.AdminApiTLS, adminTLS)
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

	if cr.Other == nil {
		cr.Other = make(map[string]interface{})
	}
	cr.Other["auto_create_topics_enabled"] = r.pandaCluster.Spec.Configuration.AutoCreateTopics
	cr.Other["enable_idempotence"] = true
	cr.Other["enable_transactions"] = true
	if featuregates.ShadowIndex(r.pandaCluster.Spec.Version) {
		cr.Other["cloud_storage_segment_max_upload_interval_sec"] = 60 * 30 // 60s * 30 = 30 minutes
	}

	segmentSize := logSegmentSize
	cr.LogSegmentSize = &segmentSize

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

	r.preparePandaproxy(cfgRpk)
	r.preparePandaproxyTLS(cfgRpk)
	err := r.preparePandaproxyClient(ctx, cfgRpk)
	if err != nil {
		return nil, err
	}

	if sr := r.pandaCluster.Spec.Configuration.SchemaRegistry; sr != nil {
		cfgRpk.SchemaRegistry.SchemaRegistryAPI = []config.NamedSocketAddress{
			{
				SocketAddress: config.SocketAddress{
					Address: "0.0.0.0",
					Port:    sr.Port,
				},
				Name: SchemaRegistryPortName,
			},
		}
	}
	r.prepareSchemaRegistryTLS(cfgRpk)
	err = r.prepareSchemaRegistryClient(ctx, cfgRpk)
	if err != nil {
		return nil, err
	}

	mgr := config.NewManager(afero.NewOsFs())
	err = mgr.Merge(cfgRpk)
	if err != nil {
		return nil, err
	}

	// Add arbitrary parameters to configuration
	for k, v := range r.pandaCluster.Spec.AdditionalConfiguration {
		if buildInType(v) {
			err = mgr.Set(k, v, "single")
			if err != nil {
				return nil, err
			}
		} else {
			err = mgr.Set(k, v, "")
			if err != nil {
				return nil, err
			}
		}
	}

	return mgr.Get()
}

func buildInType(value string) bool {
	if _, err := strconv.Atoi(value); err == nil {
		return true
	}
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return true
	}
	if _, err := strconv.ParseBool(value); err == nil {
		return true
	}
	return false
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

	if featuregates.ShadowIndex(r.pandaCluster.Spec.Version) {
		if cr.Other == nil {
			cr.Other = make(map[string]interface{})
		}
		cr.Other[cloudStorageCacheDirectory] = archivalCacheIndexDirectory

		if r.pandaCluster.Spec.CloudStorage.CacheStorage != nil && r.pandaCluster.Spec.CloudStorage.CacheStorage.Capacity.Value() > 0 {
			size := strconv.FormatInt(r.pandaCluster.Spec.CloudStorage.CacheStorage.Capacity.Value(), 10)
			cr.Other["cloud_storage_cache_size"] = size
		}
	}
}

func (r *ConfigMapResource) preparePandaproxy(cfgRpk *config.Config) {
	internal := r.pandaCluster.PandaproxyAPIInternal()
	if internal == nil {
		return
	}

	cfgRpk.Pandaproxy.PandaproxyAPI = []config.NamedSocketAddress{
		{
			SocketAddress: config.SocketAddress{
				Address: "0.0.0.0",
				Port:    internal.Port,
			},
			Name: PandaproxyPortInternalName,
		},
	}

	if r.pandaCluster.PandaproxyAPIExternal() != nil {
		cfgRpk.Pandaproxy.PandaproxyAPI = append(cfgRpk.Pandaproxy.PandaproxyAPI,
			config.NamedSocketAddress{
				SocketAddress: config.SocketAddress{
					Address: "0.0.0.0",
					Port:    calculateExternalPort(internal.Port),
				},
				Name: PandaproxyPortExternalName,
			})
	}
}

func (r *ConfigMapResource) preparePandaproxyClient(
	ctx context.Context, cfgRpk *config.Config,
) error {
	if internal := r.pandaCluster.PandaproxyAPIInternal(); internal == nil {
		return nil
	}

	replicas := *r.pandaCluster.Spec.Replicas
	cfgRpk.PandaproxyClient = &config.KafkaClient{}
	for i := int32(0); i < replicas; i++ {
		cfgRpk.PandaproxyClient.Brokers = append(cfgRpk.PandaproxyClient.Brokers, config.SocketAddress{
			Address: fmt.Sprintf("%s-%d.%s", r.pandaCluster.Name, i, r.serviceFQDN),
			Port:    r.pandaCluster.InternalListener().Port,
		})
	}

	if !r.pandaCluster.Spec.EnableSASL {
		return nil
	}

	// Retrieve SCRAM credentials
	var secret corev1.Secret
	err := r.Get(ctx, r.pandaproxySASLUser, &secret)
	if err != nil {
		return err
	}

	// Populate configuration with SCRAM credentials
	username := string(secret.Data[corev1.BasicAuthUsernameKey])
	password := string(secret.Data[corev1.BasicAuthPasswordKey])
	mechanism := saslMechanism
	cfgRpk.PandaproxyClient.SCRAMUsername = &username
	cfgRpk.PandaproxyClient.SCRAMPassword = &password
	cfgRpk.PandaproxyClient.SASLMechanism = &mechanism

	// Add username as superuser
	cfgRpk.Redpanda.Superusers = append(cfgRpk.Redpanda.Superusers, username)

	return nil
}

func (r *ConfigMapResource) prepareSchemaRegistryClient(
	ctx context.Context, cfgRpk *config.Config,
) error {
	if r.pandaCluster.Spec.Configuration.SchemaRegistry == nil {
		return nil
	}

	replicas := *r.pandaCluster.Spec.Replicas
	cfgRpk.SchemaRegistryClient = &config.KafkaClient{}
	for i := int32(0); i < replicas; i++ {
		cfgRpk.SchemaRegistryClient.Brokers = append(cfgRpk.SchemaRegistryClient.Brokers, config.SocketAddress{
			Address: fmt.Sprintf("%s-%d.%s", r.pandaCluster.Name, i, r.serviceFQDN),
			Port:    r.pandaCluster.InternalListener().Port,
		})
	}

	if !r.pandaCluster.Spec.EnableSASL {
		return nil
	}

	// Retrieve SCRAM credentials
	var secret corev1.Secret
	err := r.Get(ctx, r.schemaRegistrySASLUser, &secret)
	if err != nil {
		return err
	}

	// Populate configuration with SCRAM credentials
	username := string(secret.Data[corev1.BasicAuthUsernameKey])
	password := string(secret.Data[corev1.BasicAuthPasswordKey])
	mechanism := saslMechanism
	cfgRpk.SchemaRegistryClient.SCRAMUsername = &username
	cfgRpk.SchemaRegistryClient.SCRAMPassword = &password
	cfgRpk.SchemaRegistryClient.SASLMechanism = &mechanism

	// Add username as superuser
	cfgRpk.Redpanda.Superusers = append(cfgRpk.Redpanda.Superusers, username)

	return nil
}

func (r *ConfigMapResource) preparePandaproxyTLS(cfgRpk *config.Config) {
	tlsListener := r.pandaCluster.PandaproxyAPITLS()
	if tlsListener != nil {
		// Only one TLS listener is supported (restricted by the webhook).
		// Determine the listener name based on being internal or external.
		name := PandaproxyPortInternalName
		if tlsListener.External.Enabled {
			name = PandaproxyPortExternalName
		}
		tls := config.ServerTLS{
			Name:              name,
			KeyFile:           fmt.Sprintf("%s/%s", tlsPandaproxyAPIDir, corev1.TLSPrivateKeyKey), // tls.key
			CertFile:          fmt.Sprintf("%s/%s", tlsPandaproxyAPIDir, corev1.TLSCertKey),       // tls.crt
			Enabled:           true,
			RequireClientAuth: tlsListener.TLS.RequireClientAuth,
		}
		if tlsListener.TLS.RequireClientAuth {
			tls.TruststoreFile = fmt.Sprintf("%s/%s", tlsPandaproxyAPIDirCA, cmetav1.TLSCAKey)
		}
		cfgRpk.Pandaproxy.PandaproxyAPITLS = []config.ServerTLS{tls}
	}
}

func (r *ConfigMapResource) prepareSchemaRegistryTLS(cfgRpk *config.Config) {
	if r.pandaCluster.Spec.Configuration.SchemaRegistry != nil &&
		r.pandaCluster.Spec.Configuration.SchemaRegistry.TLS != nil {
		name := SchemaRegistryPortName

		tls := config.ServerTLS{
			Name:              name,
			KeyFile:           fmt.Sprintf("%s/%s", tlsSchemaRegistryDir, corev1.TLSPrivateKeyKey), // tls.key
			CertFile:          fmt.Sprintf("%s/%s", tlsSchemaRegistryDir, corev1.TLSCertKey),       // tls.crt
			Enabled:           true,
			RequireClientAuth: r.pandaCluster.Spec.Configuration.SchemaRegistry.TLS.RequireClientAuth,
		}
		if r.pandaCluster.Spec.Configuration.SchemaRegistry.TLS.RequireClientAuth {
			tls.TruststoreFile = fmt.Sprintf("%s/%s", tlsSchemaRegistryDirCA, cmetav1.TLSCAKey)
		}
		cfgRpk.SchemaRegistry.SchemaRegistryAPITLS = []config.ServerTLS{tls}
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
	return types.NamespacedName{Name: resourceNameTrim(pandaCluster.Name, baseSuffix), Namespace: pandaCluster.Namespace}
}

func configMapKind() string {
	var cfg corev1.ConfigMap
	return cfg.Kind
}

// TODO move to utilities
var letters = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func generatePassword(length int) (string, error) {
	bytes := make([]byte, length)

	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}

	for i, b := range bytes {
		bytes[i] = letters[b%byte(len(letters))]
	}

	return string(bytes), nil
}
