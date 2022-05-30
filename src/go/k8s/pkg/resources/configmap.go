// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package resources

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/go-logr/logr"
	cmetav1 "github.com/jetstack/cert-manager/pkg/apis/meta/v1"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/configuration"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/featuregates"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	baseSuffix                  = "base"
	dataDirectory               = "/var/lib/redpanda/data"
	archivalCacheIndexDirectory = "/var/lib/shadow-index-cache"

	superusersConfigurationKey = "superusers"

	oneMB          = 1024 * 1024
	logSegmentSize = 512 * oneMB

	saslMechanism = "SCRAM-SHA-256"

	configKey           = "redpanda.yaml"
	bootstrapConfigFile = ".bootstrap.yaml"
)

var (
	errKeyDoesNotExistInSecretData        = errors.New("cannot find key in secret data")
	errCloudStorageSecretKeyCannotBeEmpty = errors.New("cloud storage SecretKey string cannot be empty")

	// LastAppliedConfigurationAnnotationKey is used to store the last applied centralized configuration for doing three-way merge
	LastAppliedConfigurationAnnotationKey = redpandav1alpha1.GroupVersion.Group + "/last-applied-configuration"
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

	return r.update(ctx, &cm, obj.(*corev1.ConfigMap), r.Client, r.logger)
}

func (r *ConfigMapResource) update(
	ctx context.Context,
	current *corev1.ConfigMap,
	modified *corev1.ConfigMap,
	c k8sclient.Client,
	logger logr.Logger,
) error {
	// Do not touch existing last-applied-configuration (it's not reconciled in the main loop)
	if val, ok := current.Annotations[LastAppliedConfigurationAnnotationKey]; ok {
		if modified.Annotations == nil {
			modified.Annotations = make(map[string]string)
		}
		modified.Annotations[LastAppliedConfigurationAnnotationKey] = val
	}

	if err := r.markConfigurationConditionChanged(ctx, current, modified); err != nil {
		return err
	}

	_, err := Update(ctx, current, modified, c, logger)
	return err
}

// markConfigurationConditionChanged verifies and marks the cluster as needing synchronization (using the ClusterConfigured condition).
// The condition is changed so that the configuration controller can later restore it back to normal after interacting with the cluster.
func (r *ConfigMapResource) markConfigurationConditionChanged(
	ctx context.Context, current *corev1.ConfigMap, modified *corev1.ConfigMap,
) error {
	if !featuregates.CentralizedConfiguration(r.pandaCluster.Spec.Version) {
		return nil
	}

	status := r.pandaCluster.Status.GetConditionStatus(redpandav1alpha1.ClusterConfiguredConditionType)
	if status == corev1.ConditionFalse {
		// Condition already indicates a change
		return nil
	}

	// If the condition is not present, or it does not currently indicate a change, we check it again
	if !r.globalConfigurationChanged(current, modified) {
		return nil
	}

	r.logger.Info("Detected configuration change in the cluster")

	// We need to mark the cluster as changed to trigger the configuration workflow
	r.pandaCluster.Status.SetCondition(
		redpandav1alpha1.ClusterConfiguredConditionType,
		corev1.ConditionFalse,
		redpandav1alpha1.ClusterConfiguredReasonUpdating,
		"Detected cluster configuration change that needs to be applied to the cluster",
	)
	return r.Status().Update(ctx, r.pandaCluster)
}

// obj returns resource managed client.Object
func (r *ConfigMapResource) obj(ctx context.Context) (k8sclient.Object, error) {
	conf, err := r.CreateConfiguration(ctx)
	if err != nil {
		return nil, err
	}

	cfgSerialized, err := conf.Serialize()
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
		Data: map[string]string{},
	}

	if cfgSerialized.RedpandaFile != nil {
		cm.Data[configKey] = string(cfgSerialized.RedpandaFile)
	}
	if cfgSerialized.BootstrapFile != nil {
		cm.Data[bootstrapConfigFile] = string(cfgSerialized.BootstrapFile)
	}

	err = controllerutil.SetControllerReference(r.pandaCluster, cm, r.scheme)
	if err != nil {
		return nil, err
	}

	return cm, nil
}

// CreateConfiguration creates a global configuration for the current cluster
// nolint:funlen // let's keep the configuration in one function for now and refactor later
func (r *ConfigMapResource) CreateConfiguration(
	ctx context.Context,
) (*configuration.GlobalConfiguration, error) {
	cfg := configuration.For(r.pandaCluster.Spec.Version)
	cfg.NodeConfiguration = *config.Default()
	mountPoints := GetTLSMountPoints()

	c := r.pandaCluster.Spec.Configuration
	cr := &cfg.NodeConfiguration.Redpanda

	internalListener := r.pandaCluster.InternalListener()
	cr.KafkaAPI = []config.NamedSocketAddress{} // we don't want to inherit default kafka port
	cr.KafkaAPI = append(cr.KafkaAPI, config.NamedSocketAddress{
		SocketAddress: config.SocketAddress{
			Address: "0.0.0.0",
			Port:    internalListener.Port,
		},
		Name: InternalListenerName,
	})

	if r.pandaCluster.ExternalListener() != nil {
		cr.KafkaAPI = append(cr.KafkaAPI, config.NamedSocketAddress{
			SocketAddress: config.SocketAddress{
				Address: "0.0.0.0",
				Port:    calculateExternalPort(internalListener.Port, r.pandaCluster.ExternalListener().Port),
			},
			Name: ExternalListenerName,
		})
	}

	cr.RPCServer.Port = clusterCRPortOrRPKDefault(c.RPCServer.Port, cr.RPCServer.Port)
	cr.AdvertisedRPCAPI = &config.SocketAddress{
		Address: "0.0.0.0",
		Port:    clusterCRPortOrRPKDefault(c.RPCServer.Port, cr.RPCServer.Port),
	}

	cr.AdminAPI[0].Port = clusterCRPortOrRPKDefault(r.pandaCluster.AdminAPIInternal().Port, cr.AdminAPI[0].Port)
	cr.AdminAPI[0].Name = AdminPortName
	if r.pandaCluster.AdminAPIExternal() != nil {
		externalAdminAPI := config.NamedSocketAddress{
			SocketAddress: config.SocketAddress{
				Address: cr.AdminAPI[0].Address,
				Port:    cr.AdminAPI[0].Port + 1,
			},
			Name: AdminPortExternalName,
		}
		cr.AdminAPI = append(cr.AdminAPI, externalAdminAPI)
	}

	cr.DeveloperMode = c.DeveloperMode
	cr.Directory = dataDirectory
	for _, tl := range r.pandaCluster.KafkaTLSListeners() {
		tls := config.ServerTLS{
			Name:              tl.Name,
			KeyFile:           fmt.Sprintf("%s/%s", mountPoints.KafkaAPI.NodeCertMountDir, corev1.TLSPrivateKeyKey), // tls.key
			CertFile:          fmt.Sprintf("%s/%s", mountPoints.KafkaAPI.NodeCertMountDir, corev1.TLSCertKey),       // tls.crt
			Enabled:           true,
			RequireClientAuth: tl.TLS.RequireClientAuth,
		}
		if tl.TLS.RequireClientAuth {
			tls.TruststoreFile = fmt.Sprintf("%s/%s", mountPoints.KafkaAPI.ClientCAMountDir, cmetav1.TLSCAKey)
		}
		cr.KafkaAPITLS = append(cr.KafkaAPITLS, tls)
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
			KeyFile:           fmt.Sprintf("%s/%s", mountPoints.AdminAPI.NodeCertMountDir, corev1.TLSPrivateKeyKey),
			CertFile:          fmt.Sprintf("%s/%s", mountPoints.AdminAPI.NodeCertMountDir, corev1.TLSCertKey),
			Enabled:           true,
			RequireClientAuth: adminAPITLSListener.TLS.RequireClientAuth,
		}
		if adminAPITLSListener.TLS.RequireClientAuth {
			adminTLS.TruststoreFile = fmt.Sprintf("%s/%s", mountPoints.AdminAPI.ClientCAMountDir, cmetav1.TLSCAKey)
		}
		cr.AdminAPITLS = append(cr.AdminAPITLS, adminTLS)
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
		r.prepareCloudStorage(cfg, secretKeyStr)
	}

	for _, user := range r.pandaCluster.Spec.Superusers {
		if err := cfg.AppendToAdditionalRedpandaProperty(superusersConfigurationKey, user.Username); err != nil {
			return nil, err
		}
	}

	if r.pandaCluster.Spec.EnableSASL {
		cfg.SetAdditionalRedpandaProperty("enable_sasl", true)
	}

	partitions := r.pandaCluster.Spec.Configuration.GroupTopicPartitions
	if partitions != 0 {
		cfg.SetAdditionalRedpandaProperty("group_topic_partitions", partitions)
	}

	cfg.SetAdditionalRedpandaProperty("auto_create_topics_enabled", r.pandaCluster.Spec.Configuration.AutoCreateTopics)

	if featuregates.ShadowIndex(r.pandaCluster.Spec.Version) {
		intervalSec := 60 * 30 // 60s * 30 = 30 minutes
		cfg.SetAdditionalRedpandaProperty("cloud_storage_segment_max_upload_interval_sec", intervalSec)
	}

	cfg.SetAdditionalRedpandaProperty("log_segment_size", logSegmentSize)

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

	r.preparePandaproxy(&cfg.NodeConfiguration)
	r.preparePandaproxyTLS(&cfg.NodeConfiguration, mountPoints)
	err := r.preparePandaproxyClient(ctx, cfg)
	if err != nil {
		return nil, err
	}

	if sr := r.pandaCluster.Spec.Configuration.SchemaRegistry; sr != nil {
		cfg.NodeConfiguration.SchemaRegistry.SchemaRegistryAPI = []config.NamedSocketAddress{
			{
				SocketAddress: config.SocketAddress{
					Address: "0.0.0.0",
					Port:    sr.Port,
				},
				Name: SchemaRegistryPortName,
			},
		}
	}
	r.prepareSchemaRegistryTLS(&cfg.NodeConfiguration, mountPoints)
	err = r.prepareSchemaRegistryClient(ctx, cfg)
	if err != nil {
		return nil, err
	}

	if err := cfg.SetAdditionalFlatProperties(r.pandaCluster.Spec.AdditionalConfiguration); err != nil {
		return nil, err
	}

	return cfg, nil
}

// calculateExternalPort can calculate external Kafka API port based on the internal Kafka API port
func calculateExternalPort(kafkaInternalPort, specifiedExternalPort int) int {
	if kafkaInternalPort < 0 || kafkaInternalPort > 65535 {
		return 0
	}
	if specifiedExternalPort != 0 {
		return specifiedExternalPort
	}
	return kafkaInternalPort + 1
}

func (r *ConfigMapResource) prepareCloudStorage(
	cfg *configuration.GlobalConfiguration, secretKeyStr string,
) {
	cfg.SetAdditionalRedpandaProperty("cloud_storage_enabled", r.pandaCluster.Spec.CloudStorage.Enabled)
	cfg.SetAdditionalRedpandaProperty("cloud_storage_access_key", r.pandaCluster.Spec.CloudStorage.AccessKey)
	cfg.SetAdditionalRedpandaProperty("cloud_storage_region", r.pandaCluster.Spec.CloudStorage.Region)
	cfg.SetAdditionalRedpandaProperty("cloud_storage_bucket", r.pandaCluster.Spec.CloudStorage.Bucket)
	cfg.SetAdditionalRedpandaProperty("cloud_storage_secret_key", secretKeyStr)
	cfg.SetAdditionalRedpandaProperty("cloud_storage_disable_tls", r.pandaCluster.Spec.CloudStorage.DisableTLS)

	interval := r.pandaCluster.Spec.CloudStorage.ReconcilicationIntervalMs
	if interval != 0 {
		cfg.SetAdditionalRedpandaProperty("cloud_storage_reconciliation_interval_ms", interval)
	}
	maxCon := r.pandaCluster.Spec.CloudStorage.MaxConnections
	if maxCon != 0 {
		cfg.SetAdditionalRedpandaProperty("cloud_storage_max_connections", maxCon)
	}
	apiEndpoint := r.pandaCluster.Spec.CloudStorage.APIEndpoint
	if apiEndpoint != "" {
		cfg.SetAdditionalRedpandaProperty("cloud_storage_api_endpoint", apiEndpoint)
	}
	endpointPort := r.pandaCluster.Spec.CloudStorage.APIEndpointPort
	if endpointPort != 0 {
		cfg.SetAdditionalRedpandaProperty("cloud_storage_api_endpoint_port", endpointPort)
	}
	trustfile := r.pandaCluster.Spec.CloudStorage.Trustfile
	if trustfile != "" {
		cfg.SetAdditionalRedpandaProperty("cloud_storage_trust_file", trustfile)
	}

	if featuregates.ShadowIndex(r.pandaCluster.Spec.Version) {
		cfg.NodeConfiguration.Redpanda.CloudStorageCacheDirectory = archivalCacheIndexDirectory

		if r.pandaCluster.Spec.CloudStorage.CacheStorage != nil && r.pandaCluster.Spec.CloudStorage.CacheStorage.Capacity.Value() > 0 {
			size := strconv.FormatInt(r.pandaCluster.Spec.CloudStorage.CacheStorage.Capacity.Value(), 10)
			cfg.SetAdditionalRedpandaProperty("cloud_storage_cache_size", size)
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
					Port:    calculateExternalPort(internal.Port, 0),
				},
				Name: PandaproxyPortExternalName,
			})
	}
}

func (r *ConfigMapResource) preparePandaproxyClient(
	ctx context.Context, cfg *configuration.GlobalConfiguration,
) error {
	if internal := r.pandaCluster.PandaproxyAPIInternal(); internal == nil {
		return nil
	}

	replicas := *r.pandaCluster.Spec.Replicas
	cfg.NodeConfiguration.PandaproxyClient = &config.KafkaClient{}
	for i := int32(0); i < replicas; i++ {
		cfg.NodeConfiguration.PandaproxyClient.Brokers = append(cfg.NodeConfiguration.PandaproxyClient.Brokers, config.SocketAddress{
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
	cfg.NodeConfiguration.PandaproxyClient.SCRAMUsername = &username
	cfg.NodeConfiguration.PandaproxyClient.SCRAMPassword = &password
	cfg.NodeConfiguration.PandaproxyClient.SASLMechanism = &mechanism

	// Add username as superuser
	return cfg.AppendToAdditionalRedpandaProperty(superusersConfigurationKey, username)
}

func (r *ConfigMapResource) prepareSchemaRegistryClient(
	ctx context.Context, cfg *configuration.GlobalConfiguration,
) error {
	if r.pandaCluster.Spec.Configuration.SchemaRegistry == nil {
		return nil
	}

	replicas := *r.pandaCluster.Spec.Replicas
	cfg.NodeConfiguration.SchemaRegistryClient = &config.KafkaClient{}
	for i := int32(0); i < replicas; i++ {
		cfg.NodeConfiguration.SchemaRegistryClient.Brokers = append(cfg.NodeConfiguration.SchemaRegistryClient.Brokers, config.SocketAddress{
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
	cfg.NodeConfiguration.SchemaRegistryClient.SCRAMUsername = &username
	cfg.NodeConfiguration.SchemaRegistryClient.SCRAMPassword = &password
	cfg.NodeConfiguration.SchemaRegistryClient.SASLMechanism = &mechanism

	// Add username as superuser
	return cfg.AppendToAdditionalRedpandaProperty(superusersConfigurationKey, username)
}

func (r *ConfigMapResource) preparePandaproxyTLS(
	cfgRpk *config.Config, mountPoints *TLSMountPoints,
) {
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
			KeyFile:           fmt.Sprintf("%s/%s", mountPoints.PandaProxyAPI.NodeCertMountDir, corev1.TLSPrivateKeyKey), // tls.key
			CertFile:          fmt.Sprintf("%s/%s", mountPoints.PandaProxyAPI.NodeCertMountDir, corev1.TLSCertKey),       // tls.crt
			Enabled:           true,
			RequireClientAuth: tlsListener.TLS.RequireClientAuth,
		}
		if tlsListener.TLS.RequireClientAuth {
			tls.TruststoreFile = fmt.Sprintf("%s/%s", mountPoints.PandaProxyAPI.ClientCAMountDir, cmetav1.TLSCAKey)
		}
		cfgRpk.Pandaproxy.PandaproxyAPITLS = []config.ServerTLS{tls}
	}
}

func (r *ConfigMapResource) prepareSchemaRegistryTLS(
	cfgRpk *config.Config, mountPoints *TLSMountPoints,
) {
	if r.pandaCluster.Spec.Configuration.SchemaRegistry != nil &&
		r.pandaCluster.Spec.Configuration.SchemaRegistry.TLS != nil {
		name := SchemaRegistryPortName

		tls := config.ServerTLS{
			Name:              name,
			KeyFile:           fmt.Sprintf("%s/%s", mountPoints.SchemaRegistryAPI.NodeCertMountDir, corev1.TLSPrivateKeyKey), // tls.key
			CertFile:          fmt.Sprintf("%s/%s", mountPoints.SchemaRegistryAPI.NodeCertMountDir, corev1.TLSCertKey),       // tls.crt
			Enabled:           true,
			RequireClientAuth: r.pandaCluster.Spec.Configuration.SchemaRegistry.TLS.RequireClientAuth,
		}
		if r.pandaCluster.Spec.Configuration.SchemaRegistry.TLS.RequireClientAuth {
			tls.TruststoreFile = fmt.Sprintf("%s/%s", mountPoints.SchemaRegistryAPI.ClientCAMountDir, cmetav1.TLSCAKey)
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
	pwdBytes := make([]byte, length)

	if _, err := rand.Read(pwdBytes); err != nil {
		return "", err
	}

	for i, b := range pwdBytes {
		pwdBytes[i] = letters[b%byte(len(letters))]
	}

	return string(pwdBytes), nil
}

// GetNodeConfigHash returns md5 hash of the configuration.
// For clusters without centralized configuration, it computes a hash of the plain "redpanda.yaml" file.
// When using centralized configuration, it only takes into account node properties.
func (r *ConfigMapResource) GetNodeConfigHash(
	ctx context.Context,
) (string, error) {
	cfg, err := r.CreateConfiguration(ctx)
	if err != nil {
		return "", err
	}
	if featuregates.CentralizedConfiguration(r.pandaCluster.Spec.Version) {
		return cfg.GetNodeConfigurationHash()
	}

	// Previous behavior for v21.x
	return cfg.GetAllConfigurationHash()
}

// globalConfigurationChanged verifies if the new global configuration
// is different from the one in the previous version of the ConfigMap
func (r *ConfigMapResource) globalConfigurationChanged(
	current *corev1.ConfigMap, modified *corev1.ConfigMap,
) bool {
	if !featuregates.CentralizedConfiguration(r.pandaCluster.Spec.Version) {
		return false
	}

	oldConfigNode := current.Data[configKey]
	oldConfigBootstrap := current.Data[bootstrapConfigFile]

	newConfigNode := modified.Data[configKey]
	newConfigBootstrap := modified.Data[bootstrapConfigFile]

	return newConfigNode != oldConfigNode || newConfigBootstrap != oldConfigBootstrap
}

// GetLastAppliedConfigurationFromCluster returns the last applied configuration from the configmap,
// together with information about the presence of the configmap itself.
func (r *ConfigMapResource) GetLastAppliedConfigurationFromCluster(
	ctx context.Context,
) (lastConfig map[string]interface{}, configmapExists bool, err error) {
	existing := corev1.ConfigMap{}
	if err := r.Client.Get(ctx, r.Key(), &existing); err != nil {
		if apierrors.IsNotFound(err) {
			// No keys have been used previously
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("could not load configmap for reading last applied configuration: %w", err)
	}
	if ann, ok := existing.Annotations[LastAppliedConfigurationAnnotationKey]; ok {
		var cnf map[string]interface{}
		decoder := json.NewDecoder(bytes.NewReader([]byte(ann)))
		decoder.UseNumber()
		if err := decoder.Decode(&cnf); err != nil {
			return nil, true, fmt.Errorf("could not unmarshal last applied configuration from configmap annotation %q: %w", LastAppliedConfigurationAnnotationKey, err)
		}
		cleanupJSONNumbers(cnf)
		return cnf, true, nil
	}
	return nil, true, nil
}

// cleanupJSONNumbers translates json Number objects into int64 or float64, otherwise yaml.v3
// will convert them into strings when marshaling
func cleanupJSONNumbers(cnf map[string]interface{}) {
	var replace map[string]interface{}
	for k, v := range cnf {
		switch d := v.(type) {
		case json.Number:
			if replace == nil {
				replace = make(map[string]interface{})
			}
			if conv, err := d.Int64(); err == nil {
				replace[k] = conv
			} else if conv, err := d.Float64(); err == nil {
				replace[k] = conv
			}
		case map[string]interface{}:
			cleanupJSONNumbers(d)
		}
	}
	for k, v := range replace {
		cnf[k] = v
	}
}

// SetLastAppliedConfigurationInCluster saves the last applied configuration in the configmap
func (r *ConfigMapResource) SetLastAppliedConfigurationInCluster(
	ctx context.Context, cfg map[string]interface{},
) error {
	existing := corev1.ConfigMap{}
	if err := r.Client.Get(ctx, r.Key(), &existing); err != nil {
		return fmt.Errorf("could not load configmap for storing last applied configuration: %w", err)
	}
	if cfg == nil {
		// Save an empty map instead of "null"
		cfg = make(map[string]interface{})
	}
	ser, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("could not marhsal configuration: %w", err)
	}
	newAnnotation := string(ser)
	if existing.Annotations[LastAppliedConfigurationAnnotationKey] != newAnnotation {
		if existing.Annotations == nil {
			existing.Annotations = make(map[string]string)
		}
		existing.Annotations[LastAppliedConfigurationAnnotationKey] = string(ser)
		return r.Update(ctx, &existing)
	}
	return nil
}
