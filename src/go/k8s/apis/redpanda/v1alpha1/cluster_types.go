// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package v1alpha1

import (
	"fmt"

	cmmeta "github.com/jetstack/cert-manager/pkg/apis/meta/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterSpec defines the desired state of Cluster
type ClusterSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// If specified, Redpanda Pod annotations
	Annotations map[string]string `json:"annotations,omitempty"`
	// Image is the fully qualified name of the Redpanda container
	Image string `json:"image,omitempty"`
	// Version is the Redpanda container tag
	Version string `json:"version,omitempty"`
	// Replicas determine how big the cluster will be.
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`
	// Resources used by redpanda process running in container. Beware that
	// there are multiple containers running in the redpanda pod and these can
	// be enabled/disabled and configured from the `sidecars` field. These
	// containers have separate resources settings and the amount of resources
	// assigned to these containers will be required on the cluster on top of
	// the resources defined here
	Resources corev1.ResourceRequirements `json:"resources"`
	// Sidecars is list of sidecars run alongside redpanda container
	Sidecars Sidecars `json:"sidecars,omitempty"`
	// Configuration represent redpanda specific configuration
	Configuration RedpandaConfig `json:"configuration,omitempty"`
	// If specified, Redpanda Pod tolerations
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
	// If specified, Redpanda Pod node selectors. For reference please visit
	// https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// Storage spec for cluster
	Storage StorageSpec `json:"storage,omitempty"`
	// Cloud storage configuration for cluster
	CloudStorage CloudStorageConfig `json:"cloudStorage,omitempty"`
	// List of superusers
	Superusers []Superuser `json:"superUsers,omitempty"`
	// SASL enablement flag
	EnableSASL bool `json:"enableSasl,omitempty"`
	// For configuration parameters not exposed, a map can be provided for string values.
	// Such values are passed transparently to Redpanda. The key format is "<subsystem>.field", e.g.,
	//
	// additionalConfiguration:
	//   redpanda.enable_idempotence: "true"
	//   redpanda.default_topic_partitions: "3"
	//   pandaproxy_client.produce_batch_size_bytes: "2097152"
	//
	// Notes:
	// 1. versioning is not supported for map keys
	// 2. key names not supported by Redpanda will lead to failure on start up
	// 3. updating this map requires a manual restart of the Redpanda pods. Please be aware of
	// sync period when one Redpandais POD is restarted
	// 4. cannot have keys that conflict with existing struct fields - it leads to panic
	//
	// By default if Replicas is 3 or more and redpanda.default_topic_partitions is not set
	// default webhook is setting redpanda.default_topic_partitions to 3.
	AdditionalConfiguration map[string]string `json:"additionalConfiguration,omitempty"`
	// DNSTrailingDotDisabled gives ability to turn off the fully-qualified
	// DNS name.
	// http://www.dns-sd.org/trailingdotsindomainnames.html
	DNSTrailingDotDisabled bool `json:"dnsTrailingDotDisabled,omitempty"`
	// Wether a Startup probe pointing to /v1/status/started is added to each Redpanda node or not.
	// Enabled by default.
	StartupProbeEnabled *bool `json:"startupProbeEnabled,omitempty"`
	// Used to set the max amount of retries for the StartupProbe before failing and therefore making the node restart.
	// The StartupProbe sends a request to /v1/status/started every 1s until it returns 200 or the max amount of retries
	// is reached.
	// The default value is 3000 which gives the node around 1 hour to boot.
	// i.e. 3000 retries * 1s period * ~200ms response time ~= 1h.
	//
	// The reason behind this default is that for clusters with large amounts of data, the startup of a new node can
	// take 10s of minutes because the `started` endpoint succeeds only until there are no partitions under-replicated.
	//
	// The disadvantage of this default value is that it will take around ~1h to notice a failure unrelated to startup.
	// This parameter can be tuned to accomodate smaller clusters.
	// e.g. Assuming that every retry roughly takes 1200ms, if the desired max timeout is 10 min
	// a value of 500 can be used as 10 min/1200 ms = 500
	StartupProbeMaxRetries *int32 `json:"startupProbeMaxRetries,omitempty"`
}

// Sidecars is definition of sidecars running alongside redpanda process
type Sidecars struct {
	// RpkStatus is sidecar running rpk status collecting status information
	// from the running node
	RpkStatus *Sidecar `json:"rpkStatus,omitempty"`
}

// Sidecar is a container running alongside redpanda, there's couple of them
// added by default via defaulting webhook
type Sidecar struct {
	// Enabled if false, the sidecar won't be added to the pod running redpanda node
	Enabled bool `json:"enabled,omitempty"`
	// Resources are resource requirements and limits for the container running
	// this sidecar. For the default sidecars this is defaulted
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// Superuser has full access to the Redpanda cluster
type Superuser struct {
	Username string `json:"username"`
}

// CloudStorageConfig configures the Data Archiving feature in Redpanda
// https://vectorized.io/docs/data-archiving
type CloudStorageConfig struct {
	// Enables data archiving feature
	Enabled bool `json:"enabled"`
	// Cloud storage access key
	AccessKey string `json:"accessKey,omitempty"`
	// Reference to (Kubernetes) Secret containing the cloud storage secret key.
	// SecretKeyRef must contain the name and namespace of the Secret.
	// The Secret must contain a data entry of the form:
	// data[<SecretKeyRef.Name>] = <secret key>
	SecretKeyRef corev1.ObjectReference `json:"secretKeyRef,omitempty"`
	// Cloud storage region
	Region string `json:"region,omitempty"`
	// Cloud storage bucket
	Bucket string `json:"bucket,omitempty"`
	// Reconciliation period (default - 10s)
	ReconcilicationIntervalMs int `json:"reconciliationIntervalMs,omitempty"`
	// Number of simultaneous uploads per shard (default - 20)
	MaxConnections int `json:"maxConnections,omitempty"`
	// Disable TLS (can be used in tests)
	DisableTLS bool `json:"disableTLS,omitempty"`
	// Path to certificate that should be used to validate server certificate
	Trustfile string `json:"trustfile,omitempty"`
	// API endpoint for data storage
	APIEndpoint string `json:"apiEndpoint,omitempty"`
	// Used to override TLS port (443)
	APIEndpointPort int `json:"apiEndpointPort,omitempty"`
}

// StorageSpec defines the storage specification of the Cluster
type StorageSpec struct {
	// Storage capacity requested
	Capacity resource.Quantity `json:"capacity,omitempty"`
	// Storage class name - https://kubernetes.io/docs/concepts/storage/storage-classes/
	StorageClassName string `json:"storageClassName,omitempty"`
}

// ExternalConnectivityConfig adds listener that can be reached outside
// of a kubernetes cluster. The Service type NodePort will be used
// to create unique ports on each Kubernetes nodes. Those nodes
// need to be reachable from the client perspective. Setting up
// any additional resources in cloud or premise is the responsibility
// of the Redpanda operator user e.g. allow to reach the nodes by
// creating new rule in AWS security group.
// Inside the container the Configuration.KafkaAPI.Port + 1 will be
// used as a external listener. This port is tight to the autogenerated
// host port. The collision between Kafka external, Kafka internal,
// Admin, Pandaproxy, Schema Registry and RPC port is checked in the webhook.
type ExternalConnectivityConfig struct {
	// Enabled enables the external connectivity feature
	Enabled bool `json:"enabled,omitempty"`
	// Subdomain can be used to change the behavior of an advertised
	// KafkaAPI. Each broker advertises Kafka API as follows
	// BROKER_ID.SUBDOMAIN:EXTERNAL_KAFKA_API_PORT.
	// If Subdomain is empty then each broker advertises Kafka
	// API as PUBLIC_NODE_IP:EXTERNAL_KAFKA_API_PORT.
	// If TLS is enabled then this subdomain will be requested
	// as a subject alternative name.
	Subdomain string `json:"subdomain,omitempty"`
}

// ClusterStatus defines the observed state of Cluster
type ClusterStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Replicas show how many nodes are working in the cluster
	// +optional
	Replicas int32 `json:"replicas"`
	// Nodes of the provisioned redpanda nodes
	// +optional
	Nodes NodesList `json:"nodes,omitempty"`
	// Indicates cluster is upgrading
	// +optional
	Upgrading bool `json:"upgrading"`
}

// NodesList shows where client of Cluster custom resource can reach
// various listeners of Redpanda cluster
type NodesList struct {
	Internal           []string              `json:"internal,omitempty"`
	External           []string              `json:"external,omitempty"`
	ExternalAdmin      []string              `json:"externalAdmin,omitempty"`
	ExternalPandaproxy []string              `json:"externalPandaproxy,omitempty"`
	PandaproxyIngress  *string               `json:"pandaproxyIngress,omitempty"`
	SchemaRegistry     *SchemaRegistryStatus `json:"schemaRegistry,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// Cluster is the Schema for the clusters API
type Cluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterSpec   `json:"spec,omitempty"`
	Status ClusterStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// ClusterList contains a list of Cluster
type ClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Cluster `json:"items"`
}

// RedpandaConfig is the definition of the main configuration
type RedpandaConfig struct {
	RPCServer      SocketAddress      `json:"rpcServer,omitempty"`
	KafkaAPI       []KafkaAPI         `json:"kafkaApi,omitempty"`
	AdminAPI       []AdminAPI         `json:"adminApi,omitempty"`
	PandaproxyAPI  []PandaproxyAPI    `json:"pandaproxyApi,omitempty"`
	SchemaRegistry *SchemaRegistryAPI `json:"schemaRegistry,omitempty"`
	DeveloperMode  bool               `json:"developerMode,omitempty"`
	// Number of partitions in the internal group membership topic
	GroupTopicPartitions int `json:"groupTopicPartitions,omitempty"`
	// Enable auto-creation of topics. Reference https://kafka.apache.org/documentation/#brokerconfigs_auto.create.topics.enable
	AutoCreateTopics bool `json:"autoCreateTopics,omitempty"`
}

// AdminAPI configures listener for the Redpanda Admin API
type AdminAPI struct {
	Port int `json:"port,omitempty"`
	// External enables user to expose Redpanda
	// admin API outside of a Kubernetes cluster. For more
	// information please go to ExternalConnectivityConfig
	External ExternalConnectivityConfig `json:"external,omitempty"`
	// Configuration of TLS for Admin API
	TLS AdminAPITLS `json:"tls,omitempty"`
}

// KafkaAPI configures listener for the Kafka API
type KafkaAPI struct {
	Port int `json:"port,omitempty"`
	// External enables user to expose Redpanda
	// nodes outside of a Kubernetes cluster. For more
	// information please go to ExternalConnectivityConfig
	External ExternalConnectivityConfig `json:"external,omitempty"`
	// Configuration of TLS for Kafka API
	TLS KafkaAPITLS `json:"tls,omitempty"`
}

// PandaproxyAPI configures listener for the Pandaproxy API
type PandaproxyAPI struct {
	Port int `json:"port,omitempty"`
	// External enables user to expose Redpanda
	// nodes outside of a Kubernetes cluster. For more
	// information please go to ExternalConnectivityConfig
	External ExternalConnectivityConfig `json:"external,omitempty"`
	// Configuration of TLS for Pandaproxy API
	TLS PandaproxyAPITLS `json:"tls,omitempty"`
}

// SchemaRegistryAPI configures the schema registry API
type SchemaRegistryAPI struct {
	// Port will set the schema registry listener port in Redpanda
	// configuration.
	// If not set the default will be 8081
	Port int `json:"port"`
	// External enables user to expose Redpanda
	// nodes outside of a Kubernetes cluster. For more
	// information please go to ExternalConnectivityConfig
	External *ExternalConnectivityConfig `json:"external,omitempty"`
	// TLS is the configuration for schema registry
	TLS *SchemaRegistryAPITLS `json:"tls,omitempty"`
}

// SchemaRegistryStatus reports addresses where schema registry
// can be reached
type SchemaRegistryStatus struct {
	Internal string `json:"internal,omitempty"`
	// External address should be registered in DNS provider using
	// all public IP of a nodes that Redpanda is scheduled on.
	//
	// The External is empty when subdomain is not provided.
	External string `json:"external,omitempty"`
	// ExternalNodeIPs is only filled when the Schema Registry
	// external connectivity feature flag is enabled, but the subdomain is
	// empty. This gives user ability to register all addresses individually
	// in DNS provider of choice.
	ExternalNodeIPs []string `json:"externalNodeIPs,omitempty"`
}

// KafkaAPITLS configures TLS for redpanda Kafka API
//
// If Enabled is set to true, one-way TLS verification is enabled.
// In that case, a key pair ('tls.crt', 'tls.key') and CA certificate 'ca.crt'
// are generated and stored in a Secret with the same name and namespace as the
// Redpanda cluster. 'ca.crt', must be used by a client as a trustore when
// communicating with Redpanda.
//
// If RequireClientAuth is set to true, two-way TLS verification is enabled.
// In that case, a node and three client certificates are created.
// The node certificate is used by redpanda nodes.
//
// The three client certificates are the following: 1. operator client
// certificate is for internal use of this kubernetes operator 2. admin client
// certificate is meant to be used by your internal infrastructure, other than
// operator. It's possible that you might not need this client certificate in
// your setup. The client certificate can be retrieved from the Secret named
// '<redpanda-cluster-name>-admin-client'. 3. user client certificate is
// available for Redpanda users to call KafkaAPI. The client certificate can be
// retrieved from the Secret named '<redpanda-cluster-name>-user-client'.
//
// All TLS secrets are stored in the same namespace as the Redpanda cluster.
//
// Additionally all mentioned certificates beside PEM version will have JKS
// and PKCS#12 certificate. Both stores are protected with the password that
// is the same as the name of the Cluster custom resource.
type KafkaAPITLS struct {
	Enabled bool `json:"enabled,omitempty"`
	// References cert-manager Issuer or ClusterIssuer. When provided, this
	// issuer will be used to issue node certificates.
	// Typically you want to provide the issuer when a generated self-signed one
	// is not enough and you need to have a verifiable chain with a proper CA
	// certificate.
	IssuerRef *cmmeta.ObjectReference `json:"issuerRef,omitempty"`
	// If provided, operator uses certificate in this secret instead of
	// issuing its own node certificate. The secret is expected to provide
	// the following keys: 'ca.crt', 'tls.key' and 'tls.crt'
	// If NodeSecretRef points to secret in different namespace, operator will
	// duplicate the secret to the same namespace as redpanda CRD to be able to
	// mount it to the nodes
	NodeSecretRef *corev1.ObjectReference `json:"nodeSecretRef,omitempty"`
	// Enables two-way verification on the server side. If enabled, all Kafka
	// API clients are required to have a valid client certificate.
	RequireClientAuth bool `json:"requireClientAuth,omitempty"`
}

// AdminAPITLS configures TLS for Redpanda Admin API
//
// If Enabled is set to true, one-way TLS verification is enabled.
// In that case, a key pair ('tls.crt', 'tls.key') and CA certificate 'ca.crt'
// are generated and stored in a Secret with the same name and namespace as the
// Redpanda cluster. 'ca.crt' must be used by a client as a truststore when
// communicating with Redpanda.
//
// If RequireClientAuth is set to true, two-way TLS verification is enabled.
// In that case, a client certificate is generated, which can be retrieved from
// the Secret named '<redpanda-cluster-name>-admin-api-client'.
//
// All TLS secrets are stored in the same namespace as the Redpanda cluster.
//
// Additionally all mentioned certificates beside PEM version will have JKS
// and PKCS#12 certificate. Both stores are protected with the password that
// is the same as the name of the Cluster custom resource.
type AdminAPITLS struct {
	Enabled           bool `json:"enabled,omitempty"`
	RequireClientAuth bool `json:"requireClientAuth,omitempty"`
}

// PandaproxyAPITLS configures the TLS of the Pandaproxy API
//
// If Enabled is set to true, one-way TLS verification is enabled.
// In that case, a key pair ('tls.crt', 'tls.key') and CA certificate 'ca.crt'
// are generated and stored in a Secret named '<redpanda-cluster-name>-proxy-api-node'
// and namespace as the Redpanda cluster. 'ca.crt' must be used by a client as a
// truststore when communicating with Redpanda.
//
// If RequireClientAuth is set to true, two-way TLS verification is enabled.
// In that case, a client certificate is generated, which can be retrieved from
// the Secret named '<redpanda-cluster-name>-proxy-api-client'.
//
// All TLS secrets are stored in the same namespace as the Redpanda cluster.
//
// Additionally all mentioned certificates beside PEM version will have JKS
// and PKCS#12 certificate. Both stores are protected with the password that
// is the same as the name of the Cluster custom resource.
type PandaproxyAPITLS struct {
	Enabled           bool `json:"enabled,omitempty"`
	RequireClientAuth bool `json:"requireClientAuth,omitempty"`
}

// SchemaRegistryAPITLS configures the TLS of the Pandaproxy API
//
// If Enabled is set to true, one-way TLS verification is enabled.
// In that case, a key pair ('tls.crt', 'tls.key') and CA certificate 'ca.crt'
// are generated and stored in a Secret named '<redpanda-cluster-name>-schema-registry-node'
// and namespace as the Redpanda cluster. 'ca.crt' must be used by a client as a
// truststore when communicating with Schema registry.
//
// If RequireClientAuth is set to true, two-way TLS verification is enabled.
// In that case, a client certificate is generated, which can be retrieved from
// the Secret named '<redpanda-cluster-name>-schema-registry-client'.
//
// All TLS secrets are stored in the same namespace as the Redpanda cluster.
//
// Additionally all mentioned certificates beside PEM version will have JKS
// and PKCS#12 certificate. Both stores are protected with the password that
// is the same as the name of the Cluster custom resource.
type SchemaRegistryAPITLS struct {
	Enabled           bool `json:"enabled,omitempty"`
	RequireClientAuth bool `json:"requireClientAuth,omitempty"`
}

// SocketAddress provide the way to configure the port
type SocketAddress struct {
	Port int `json:"port,omitempty"`
}

const (
	// MinimumMemoryPerCore the minimum amount of memory needed per core
	MinimumMemoryPerCore = 2 * gb
)

func init() {
	SchemeBuilder.Register(&Cluster{}, &ClusterList{})
}

// FullImageName returns image name including version
func (r *Cluster) FullImageName() string {
	return fmt.Sprintf("%s:%s", r.Spec.Image, r.Spec.Version)
}

// ExternalListener returns external listener if found in configuration. Returns
// nil if no external listener is configured. Right now we support only one
// external listener which is enforced by webhook
func (r *Cluster) ExternalListener() *KafkaAPI {
	for _, el := range r.Spec.Configuration.KafkaAPI {
		if el.External.Enabled {
			return &el
		}
	}
	return nil
}

// InternalListener returns internal listener.
func (r *Cluster) InternalListener() *KafkaAPI {
	for _, el := range r.Spec.Configuration.KafkaAPI {
		if !el.External.Enabled {
			return &el
		}
	}
	return nil
}

// KafkaTLSListener returns kafka listener that has tls enabled. Returns nil if
// no tls is configured. Until v1alpha1 API is deprecated, we support only
// single listener with TLS
func (r *Cluster) KafkaTLSListener() *KafkaAPI {
	for i, el := range r.Spec.Configuration.KafkaAPI {
		if el.TLS.Enabled {
			return &r.Spec.Configuration.KafkaAPI[i]
		}
	}
	return nil
}

// AdminAPIInternal returns internal admin listener
func (r *Cluster) AdminAPIInternal() *AdminAPI {
	for _, el := range r.Spec.Configuration.AdminAPI {
		if !el.External.Enabled {
			return &el
		}
	}
	return nil
}

// AdminAPIExternal returns external admin listener
func (r *Cluster) AdminAPIExternal() *AdminAPI {
	for _, el := range r.Spec.Configuration.AdminAPI {
		if el.External.Enabled {
			return &el
		}
	}
	return nil
}

// AdminAPITLS returns admin api listener that has tls enabled or nil if there's
// none
func (r *Cluster) AdminAPITLS() *AdminAPI {
	for i, el := range r.Spec.Configuration.AdminAPI {
		if el.TLS.Enabled {
			return &r.Spec.Configuration.AdminAPI[i]
		}
	}
	return nil
}

// PandaproxyAPIInternal returns internal pandaproxy listener
func (r *Cluster) PandaproxyAPIInternal() *PandaproxyAPI {
	for _, el := range r.Spec.Configuration.PandaproxyAPI {
		if !el.External.Enabled {
			return &el
		}
	}
	return nil
}

// PandaproxyAPIExternal returns the external pandaproxy listener
func (r *Cluster) PandaproxyAPIExternal() *PandaproxyAPI {
	for _, el := range r.Spec.Configuration.PandaproxyAPI {
		if el.External.Enabled {
			return &el
		}
	}
	return nil
}

// PandaproxyAPITLS returns a Pandaproxy listener that has TLS enabled.
// It returns nil if no TLS is configured.
func (r *Cluster) PandaproxyAPITLS() *PandaproxyAPI {
	for i, el := range r.Spec.Configuration.PandaproxyAPI {
		if el.TLS.Enabled {
			return &r.Spec.Configuration.PandaproxyAPI[i]
		}
	}
	return nil
}

// IsSchemaRegistryExternallyAvailable returns true if schema registry
// is enabled with external connectivity
func (r *Cluster) IsSchemaRegistryExternallyAvailable() bool {
	return r.Spec.Configuration.SchemaRegistry != nil &&
		r.Spec.Configuration.SchemaRegistry.External != nil &&
		r.Spec.Configuration.SchemaRegistry.External.Enabled
}
