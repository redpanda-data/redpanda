package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// RedpandaClusterSpec defines the desired state of Redpanda Cluster
type RedpandaClusterSpec struct {
	// NameOverride is the override to give your redpanda release
	NameOverride string `json:"nameOverride,omitempty"`
	// FullNameOverride is the override to give your redpanda release
	FullNameOverride string `json:"fullNameOverride,omitempty"`
	// ClusterDomain is the override to give your redpanda release
	ClusterDomain string `json:"clusterDomain,omitempty"`
	// CommonLabels is the override to give your redpanda release
	CommonLabels map[string]string `json:"commonLabels,omitempty"`
	// NodeSelector is the override to give your redpanda release
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// Tolerations is the override to give your redpanda release
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// Image defines the container image to use for the redpanda cluster
	Image *RedpandaImage `json:"image,omitempty"`

	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// Deprecated: Use `Enterprise.LicenseKey` instead.
	LicenseKey *string `json:"license_key,omitempty"`
	// Deprecated: Use `Enterprise.LicenseSecretRef` instead.
	LicenseSecretRef *LicenseSecretRef `json:"license_secret_ref,omitempty"`
	Enterprise       *Enterprise       `json:"enterprise,omitempty"`

	RackAwareness *RackAwareness `json:"rackAwareness,omitempty"`

	Console *RedpandaConsole `json:"console,omitempty"`

	Auth *Auth `json:"auth,omitempty"`

	TLS *TLS `json:"tls,omitempty"`

	External *External `json:"external,omitempty"`

	Logging *Logging `json:"logging,omitempty"`

	Resources *Resources `json:"resources,omitempty"`

	Storage *Storage `json:"storage,omitempty"`

	PostInstallJob *PostInstallJob `json:"post_install_job,omitempty"`

	PostUpgradeJob *PostUpgradeJob `json:"post_upgrade_job,omitempty"`

	Statefulset *Statefulset `json:"statefulset,omitempty"`

	Tuning *Tuning `json:"tuning,omitempty"`

	Listeners *Listeners `json:"listeners,omitempty"`

	Config *Config `json:"config,omitempty"`

	RBAC *RBAC `json:"rbac,omitempty"`

	ServiceAccount *ServiceAccount `json:"serviceAccount,omitempty"`

	Monitoring *Monitoring `json:"monitoring,omitempty"`
}

type ConfigWatcher struct {
	Enabled           *bool                        `json:"enabled,omitempty"`
	ExtraVolumeMounts string                       `json:"extraVolumeMounts,omitempty"`
	Resources         *corev1.ResourceRequirements `json:"resources,omitempty"`
	SecurityContext   *corev1.SecurityContext      `json:"SecurityContext,omitempty"`
}

// RedpandaImage is a top-level field of the values file
type RedpandaImage struct {
	Repository *string `json:"repository,omitempty"`
	Tag        *string `json:"tag,omitempty"`
	PullPolicy *string `json:"pullPolicy,omitempty"`
}

// LicenseSecretRef is a top-level field of the values file
type LicenseSecretRef struct {
	SecretKey  *string `json:"secret_key,omitempty"`
	SecretName *string `json:"secret_name,omitempty"`
}

// RackAwareness is a top-level field of the values file
type RackAwareness struct {
	Enabled        bool    `json:"enabled"`
	NodeAnnotation *string `json:"nodeAnnotation,omitempty"`
}

type RedpandaConsole struct {
	Enabled *bool `json:"enabled,omitempty"`

	ReplicaCount      *int    `json:"replicaCount,omitempty"`
	NameOverride      *string `json:"nameOverride,omitempty"`
	FullNameOverride  *string `json:"fullnameOverride,omitempty"`
	PriorityClassName *string `json:"priorityClassName,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	Image *runtime.RawExtension `json:"image,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	ImagePullSecrets *runtime.RawExtension `json:"imagePullSecrets,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	ServiceAccount *runtime.RawExtension `json:"serviceAccount,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	Annotations *runtime.RawExtension `json:"annotations,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	PodAnnotations *runtime.RawExtension `json:"podAnnotations,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	PodLabels *runtime.RawExtension `json:"podLabels,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	PodSecurityContext *runtime.RawExtension `json:"podSecurityContext,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	SecurityContext *runtime.RawExtension `json:"securityContext,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	Service *runtime.RawExtension `json:"service,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	Ingress *runtime.RawExtension `json:"ingress,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	Resources *runtime.RawExtension `json:"resources,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	Autoscaling *runtime.RawExtension `json:"autoscaling,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	NodeSelector *runtime.RawExtension `json:"nodeSelector,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	Tolerations *runtime.RawExtension `json:"tolerations,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	Affinity *runtime.RawExtension `json:"affinity,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	TopologySpreadConstraints *runtime.RawExtension `json:"topologySpreadConstraints,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	ExtraEnv *runtime.RawExtension `json:"extraEnv,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	ExtraEnvFrom *runtime.RawExtension `json:"extraEnvFrom,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	ExtraVolumes *runtime.RawExtension `json:"extraVolumes,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	ExtraVolumeMounts *runtime.RawExtension `json:"extraVolumeMounts,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	ExtraContainers *runtime.RawExtension `json:"extraContainers,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	InitContainers *runtime.RawExtension `json:"initContainers,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	SecretMounts *runtime.RawExtension `json:"secretMounts,omitempty"`

	ConfigMap  *ConsoleCreateObj `json:"configMap,omitempty"`
	Secret     *ConsoleCreateObj `json:"secret,omitempty"`
	Deployment *ConsoleCreateObj `json:"deployment,omitempty"`

	// +kubebuilder:pruning:PreserveUnknownFields
	Console *runtime.RawExtension `json:"console,omitempty"`
}

type ConsoleCreateObj struct {
	Create bool `json:"create"`
}

// Auth is a top-level field of the values file
type Auth struct {
	SASL *SASL `json:"sasl"`
}

// SASL is a top-level field of the values file
type SASL struct {
	Enabled   bool    `json:"enabled"`
	Mechanism *string `json:"mechanism,omitempty"`
	SecretRef *string `json:"secretRef,omitempty"`
	// DO NOT SET Omitempty, as empty list is a valid entry.
	Users []UsersItems `json:"users"`
}

// UsersItems is a top-level field of the values file
type UsersItems struct {
	Mechanism *string `json:"mechanism,omitempty"`
	Name      *string `json:"name,omitempty"`
	Password  *string `json:"password,omitempty"`
}

// TLS is a top-level field of the values file
type TLS struct {
	Certs   map[string]*Certificate `json:"certs,omitempty"`
	Enabled *bool                   `json:"enabled,omitempty"`
}

type Certificate struct {
	IssuerRef *IssuerRef       `json:"issuerRef,omitempty"`
	SecretRef *SecretRef       `json:"secretRef,omitempty"`
	Duration  *metav1.Duration `json:"duration,omitempty"`
	CAEnabled bool             `json:"caEnabled"`
}

type IssuerRef struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

type SecretRef struct {
	Name string `json:"name"`
}

// ListenerTLS is a top-level field of the values file
type ListenerTLS struct {
	Cert              *string `json:"cert,omitempty"`
	Enabled           *bool   `json:"enabled,omitempty"`
	SecretRef         *string `json:"secretRef,omitempty"`
	RequireClientAuth *bool   `json:"requireClientAuth,omitempty"`
}

// ExternalService allows you to enable or disable creation of external service type
type ExternalService struct {
	Enabled bool `json:"enabled,omitempty"`
}

// External is a top-level field of the values file
type External struct {
	Addresses      []string          `json:"addresses,omitempty"`
	Annotations    map[string]string `json:"annotations,omitempty"`
	Domain         *string           `json:"domain,omitempty"`
	Enabled        *bool             `json:"enabled,omitempty"`
	Service        *ExternalService  `json:"service,omitempty"`
	SourceRanges   []string          `json:"sourceRanges,omitempty"`
	Type           *string           `json:"type,omitempty"`
	ExternalDNS    *ExternalDNS      `json:"externalDns,omitempty"`
	PrefixTemplate *string           `json:"prefixTemplate,omitempty"`
}

// Logging is a top-level field of the values file
type Logging struct {
	LogLevel   string     `json:"logLevel"`
	UsageStats UsageStats `json:"usageStats"`
}

type UsageStats struct {
	Enabled      bool    `json:"enabled"`
	Organization *string `json:"organization,omitempty"`
	ClusterID    *string `json:"clusterId,omitempty"`
}

type Resources struct {
	CPU    *CPU    `json:"cpu,omitempty"`
	Memory *Memory `json:"memory,omitempty"`
}

// Limits is a top-level field of the values file
type Limits struct {
	CPU    *int    `json:"cpu,omitempty"`
	Memory *string `json:"memory,omitempty"`
}

// Requests is a top-level field of the values file
type Requests struct {
	CPU    *int    `json:"cpu,omitempty"`
	Memory *string `json:"memory,omitempty"`
}

// Storage is a top-level field of the values file
type Storage struct {
	HostPath         *string           `json:"hostPath,omitempty"`
	PersistentVolume *PersistentVolume `json:"persistentVolume,omitempty"`
	Tiered           *Tiered           `json:"tiered,omitempty"`
}

type Tiered struct {
	MountType                     *string                        `json:"mountType,omitempty"`
	HostPath                      *string                        `json:"hostPath,omitempty"`
	TieredStoragePersistentVolume *TieredStoragePersistentVolume `json:"persistentVolume,omitempty"`
	Config                        *TieredConfig                  `json:"config,omitempty"`
}

// TieredConfig is a top-level field of the values file
type TieredConfig struct {
	CloudStorageEnabled                     *string `json:"cloud_storage_enabled,omitempty"`
	CloudStorageAPIEndpoint                 *string `json:"cloud_storage_api_endpoint,omitempty"`
	CloudStorageAPIEndpointPort             *int    `json:"cloud_storage_api_endpoint_port,omitempty"`
	CloudStorageBucket                      *string `json:"cloud_storage_bucket"`
	CloudStorageCacheCheckInterval          *int    `json:"cloud_storage_cache_check_interval,omitempty"`
	CloudStorageCacheDirectory              *string `json:"cloud_storage_cache_directory,omitempty"`
	CloudStorageCacheSize                   *string `json:"cloud_storage_cache_size,omitempty"`
	CloudStorageCredentialsSource           *string `json:"cloud_storage_credentials_source,omitempty"`
	CloudStorageDisableTLS                  *bool   `json:"cloud_storage_disable_tls,omitempty"`
	CloudStorageEnableRemoteRead            *bool   `json:"cloud_storage_enable_remote_read,omitempty"`
	CloudStorageEnableRemoteWrite           *bool   `json:"cloud_storage_enable_remote_write,omitempty"`
	CloudStorageInitialBackoffMs            *int    `json:"cloud_storage_initial_backoff_ms,omitempty"`
	CloudStorageManifestUploadTimeoutMs     *int    `json:"cloud_storage_manifest_upload_timeout_ms,omitempty"`
	CloudStorageMaxConnectionIdleTimeMs     *int    `json:"cloud_storage_max_connection_idle_time_ms,omitempty"`
	CloudStorageMaxConnections              *int    `json:"cloud_storage_max_connections,omitempty"`
	CloudStorageReconciliationIntervalMs    *int    `json:"cloud_storage_reconciliation_interval_ms,omitempty"`
	CloudStorageRegion                      *string `json:"cloud_storage_region"`
	CloudStorageSegmentMaxUploadIntervalSec *int    `json:"cloud_storage_segment_max_upload_interval_sec,omitempty"`
	CloudStorageSegmentUploadTimeoutMs      *int    `json:"cloud_storage_segment_upload_timeout_ms,omitempty"`
	CloudStorageTrustFile                   *string `json:"cloud_storage_trust_file,omitempty"`
	CloudStorageUploadCtrlDCoeff            *int    `json:"cloud_storage_upload_ctrl_d_coeff,omitempty"`
	CloudStorageUploadCtrlMaxShares         *int    `json:"cloud_storage_upload_ctrl_max_shares,omitempty"`
	CloudStorageUploadCtrlMinShares         *int    `json:"cloud_storage_upload_ctrl_min_shares,omitempty"`
	CloudStorageUploadCtrlPCoeff            *int    `json:"cloud_storage_upload_ctrl_p_coeff,omitempty"`
	CloudStorageUploadCtrlUpdateIntervalMs  *int    `json:"cloud_storage_upload_ctrl_update_interval_ms,omitempty"`
}

// TieredStoragePersistentVolume is a top-level field of the values file
type TieredStoragePersistentVolume struct {
	Annotations  map[string]string `json:"annotations,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
	StorageClass *string           `json:"storageClass,omitempty"`
}

// PersistentVolume is a top-level field of the values file
type PersistentVolume struct {
	Annotations  map[string]string `json:"annotations,omitempty"`
	Enabled      *bool             `json:"enabled,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
	Size         *string           `json:"size,omitempty"`
	StorageClass *string           `json:"storageClass,omitempty"`
}

// PostInstallJob is a top-level field of the values file
type PostInstallJob struct {
	Resources   *corev1.ResourceRequirements `json:"resources,omitempty"`
	Annotations map[string]string            `json:"annotations,omitempty"`
	Enabled     bool                         `json:"enabled"`
	Labels      map[string]string            `json:"labels,omitempty"`
}

// PostUpgradeJob is a top-level field of the values file
type PostUpgradeJob struct {
	Annotations map[string]string `json:"annotations,omitempty"`
	Enabled     bool              `json:"enabled"`
	Labels      map[string]string `json:"labels,omitempty"`
	// +patchMergeKey=name
	// +patchStrategy=merge
	ExtraEnv     []corev1.EnvVar              `json:"extraEnv,omitempty" patchStrategy:"merge" patchMergeKey:"name"`
	ExtraEnvFrom []corev1.EnvFromSource       `json:"extraEnvFrom,omitempty"`
	Resources    *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// Statefulset is a top-level field of the values file
type Statefulset struct {
	AdditionalRedpandaCmdFlags    []string                   `json:"additionalRedpandaCmdFlags,omitempty"`
	Annotations                   map[string]string          `json:"annotations,omitempty"`
	Budget                        *Budget                    `json:"budget,omitempty"`
	ExtraVolumeMounts             string                     `json:"extraVolumeMounts,omitempty"`
	ExtraVolumes                  string                     `json:"extraVolumes,omitempty"`
	InitContainerImage            *InitContainerImage        `json:"initContainerImage,omitempty"`
	InitContainers                *InitContainers            `json:"initContainers,omitempty"`
	LivenessProbe                 *LivenessProbe             `json:"livenessProbe,omitempty"`
	NodeSelector                  map[string]string          `json:"nodeSelector,omitempty"`
	PodAffinity                   *corev1.PodAffinity        `json:"podAffinity,omitempty"`
	PodAntiAffinity               *corev1.PodAntiAffinity    `json:"podAntiAffinity,omitempty"`
	PriorityClassName             *string                    `json:"priorityClassName,omitempty"`
	ReadinessProbe                *ReadinessProbe            `json:"readinessProbe,omitempty"`
	Replicas                      *int                       `json:"replicas,omitempty"`
	SecurityContext               *corev1.SecurityContext    `json:"securityContext,omitempty"`
	SideCars                      *SideCars                  `json:"sideCars,omitempty"`
	SkipChown                     *bool                      `json:"skipChown,omitempty"`
	StartupProbe                  *StartupProbe              `json:"startupProbe,omitempty"`
	Tolerations                   []corev1.Toleration        `json:"tolerations,omitempty"`
	TopologySpreadConstraints     *TopologySpreadConstraints `json:"topologySpreadConstraints,omitempty"`
	UpdateStrategy                *UpdateStrategy            `json:"updateStrategy,omitempty"`
	TerminationGracePeriodSeconds *int                       `json:"terminationGracePeriodSeconds,omitempty"`
}

// Budget is a top-level field of the values file
type Budget struct {
	MaxUnavailable int `json:"maxUnavailable"`
}

// LivenessProbe is a top-level field of the values file
type LivenessProbe struct {
	FailureThreshold    int `json:"failureThreshold"`
	InitialDelaySeconds int `json:"initialDelaySeconds"`
	PeriodSeconds       int `json:"periodSeconds"`
}

// ReadinessProbe is a top-level field of the values file
type ReadinessProbe struct {
	FailureThreshold    int `json:"failureThreshold"`
	InitialDelaySeconds int `json:"initialDelaySeconds"`
	PeriodSeconds       int `json:"periodSeconds"`
}

// StartupProbe is a top-level field of the values file
type StartupProbe struct {
	FailureThreshold    int `json:"failureThreshold"`
	InitialDelaySeconds int `json:"initialDelaySeconds"`
	PeriodSeconds       int `json:"periodSeconds"`
}

// PodAntiAffinity is a top-level field of the values file
type PodAntiAffinity struct {
	TopologyKey string `json:"topologyKey"`
	Type        string `json:"type"`
	Weight      int    `json:"weight"`
	// +kubebuilder:pruning:PreserveUnknownFields
	Custom *runtime.RawExtension `json:"custom,omitempty"`
}

// TopologySpreadConstraints is a top-level field of the values file
type TopologySpreadConstraints struct {
	MaxSkew           int    `json:"maxSkew"`
	TopologyKey       string `json:"topologyKey"`
	WhenUnsatisfiable string `json:"whenUnsatisfiable"`
}

// UpdateStrategy is a top-level field of the values file
type UpdateStrategy struct {
	Type string `json:"type"`
}

// Tuning is a top-level field of the values file
type Tuning struct {
	ExtraVolumeMounts string                       `json:"extraVolumeMounts,omitempty"`
	Resources         *corev1.ResourceRequirements `json:"resources,omitempty"`
	BallastFilePath   *string                      `json:"ballast_file_path,omitempty"`
	BallastFileSize   *string                      `json:"ballast_file_size,omitempty"`
	TuneAioEvents     *bool                        `json:"tune_aio_events,omitempty"`
	TuneBallastFile   *bool                        `json:"tune_ballast_file,omitempty"`
	TuneClockSource   *bool                        `json:"tune_clocksource,omitempty"`
	WellKnownIo       *string                      `json:"well_known_io,omitempty"`
}

// Listeners is a top-level field of the values file
type Listeners struct {
	Admin          *Admin          `json:"admin,omitempty"`
	HTTP           *HTTP           `json:"http,omitempty"`
	Kafka          *Kafka          `json:"kafka,omitempty"`
	RPC            *RPC            `json:"rpc,omitempty"`
	SchemaRegistry *SchemaRegistry `json:"schemaRegistry,omitempty"`
}

type ExternalListener struct {
	AuthenticationMethod *string      `json:"authenticationMethod,omitempty"`
	Port                 *int         `json:"port,omitempty"`
	TLS                  *ListenerTLS `json:"tls,omitempty"`
	AdvertisedPorts      []int        `json:"advertisedPorts,omitempty"`
}

// Admin is a top-level field of the values file
type Admin struct {
	External map[string]*ExternalListener `json:"external,omitempty"`
	Port     *int                         `json:"port,omitempty"`
	TLS      *ListenerTLS                 `json:"tls,omitempty"`
}

// HTTP is a top-level field of the values file`
type HTTP struct {
	AuthenticationMethod *string                      `json:"authenticationMethod,omitempty"`
	Enabled              *bool                        `json:"enabled,omitempty"`
	External             map[string]*ExternalListener `json:"external,omitempty"`
	KafkaEndpoint        *string                      `json:"kafkaEndpoint,omitempty"`
	Port                 *int                         `json:"port,omitempty"`
	TLS                  *ListenerTLS                 `json:"tls,omitempty"`
	PrefixTemplate       *string                      `json:"prefixTemplate,omitempty"`
}

// Kafka is a top-level field of the values file
type Kafka struct {
	AuthenticationMethod *string                      `json:"authenticationMethod,omitempty"`
	External             map[string]*ExternalListener `json:"external,omitempty"`
	Port                 *int                         `json:"port,omitempty"`
	TLS                  *ListenerTLS                 `json:"tls,omitempty"`
	PrefixTemplate       *string                      `json:"prefixTemplate,omitempty"`
}

// RPC is a top-level field of the values file
type RPC struct {
	Port *int         `json:"port,omitempty"`
	TLS  *ListenerTLS `json:"tls,omitempty"`
}

// SchemaRegistry is a top-level field of the values file
type SchemaRegistry struct {
	AuthenticationMethod *string                      `json:"authenticationMethod,omitempty"`
	Enabled              *bool                        `json:"enabled,omitempty"`
	External             map[string]*ExternalListener `json:"external,omitempty"`
	KafkaEndpoint        *string                      `json:"kafkaEndpoint,omitempty"`
	Port                 *int                         `json:"port,omitempty"`
	TLS                  *ListenerTLS                 `json:"tls,omitempty"`
}

// Config is a top-level field of the values file
type Config struct {
	// +kubebuilder:pruning:PreserveUnknownFields
	Cluster *runtime.RawExtension `json:"cluster,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	Node *runtime.RawExtension `json:"node,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	Tunable *runtime.RawExtension `json:"tunable,omitempty"`
}

// SideCars is a field that stores sidecars in the statefulset
type SideCars struct {
	ConfigWatcher *ConfigWatcher `json:"configWatcher,omitempty"`
	RpkStatus     *SideCarObj    `json:"rpkStatus,omitempty"`
}

type TopologySpreadConstraintsItems struct {
	MaxSkew           int     `json:"maxSkew,omitempty"`
	TopologyKey       *string `json:"topologyKey,omitempty"`
	WhenUnsatisfiable *string `json:"whenUnsatisfiable,omitempty"`
}

type CPU struct {
	Cores           *resource.Quantity `json:"cores,omitempty"`
	Overprovisioned *bool              `json:"overprovisioned,omitempty"`
}

type Container struct {
	Max *resource.Quantity `json:"max,omitempty"`
	Min *resource.Quantity `json:"min,omitempty"`
}

type Memory struct {
	Container           *Container      `json:"container"`
	EnableMemoryLocking *bool           `json:"enable_memory_locking,omitempty"`
	Redpanda            *RedpandaMemory `json:"redpanda,omitempty"`
}

type RedpandaMemory struct {
	Memory        *resource.Quantity `json:"memory"`
	ReserveMemory *resource.Quantity `json:"reserveMemory"`
}

type RBAC struct {
	Annotations map[string]string `json:"annotations,omitempty"`
	Enabled     bool              `json:"enabled"`
}

type ServiceAccount struct {
	Annotations map[string]string `json:"annotations,omitempty"`
	Create      bool              `json:"create"`
	Name        *string           `json:"name,omitempty"`
}

type SetDataDirOwnership struct {
	Enabled           *bool                        `json:"enabled,omitempty"`
	ExtraVolumeMounts string                       `json:"extraVolumeMounts,omitempty"`
	Resources         *corev1.ResourceRequirements `json:"resources,omitempty"`
}

type InitContainerImage struct {
	Repository *string `json:"repository,omitempty"`
	Tag        *string `json:"tag,omitempty"`
}

type InitContainers struct {
	Configurator                      *Configurator                      `json:"configurator,omitempty"`
	ExtraInitContainers               string                             `json:"extraInitContainers,omitempty"`
	SetDataDirOwnership               *SetDataDirOwnership               `json:"setDataDirOwnership,omitempty"`
	SetTieredStorageCacheDirOwnership *SetTieredStorageCacheDirOwnership `json:"setTieredStorageCacheDirOwnership,omitempty"`
	Tuning                            *Tuning                            `json:"tuning,omitempty"`
}

type Configurator struct {
	ExtraVolumeMounts string                       `json:"extraVolumeMounts,omitempty"`
	Resources         *corev1.ResourceRequirements `json:"resources,omitempty"`
}

type SetTieredStorageCacheDirOwnership struct {
	ExtraVolumeMounts string                       `json:"extraVolumeMounts,omitempty"`
	Resources         *corev1.ResourceRequirements `json:"resources,omitempty"`
}

type Monitoring struct {
	Enabled        bool              `json:"enabled"`
	Labels         map[string]string `json:"commonLabels,omitempty"`
	ScrapeInterval *string           `json:"scrapeInterval,omitempty"`
}

type ExternalDNS struct {
	Enabled bool `json:"enabled"`
}

// SideCarObj represents generic sidecar object. This is a placeholder for now as it may
// each sidecar entry may require more specific impl.
type SideCarObj struct {
	Enabled         bool                         `json:"enabled,omitempty"`
	Resources       *corev1.ResourceRequirements `json:"resources,omitempty"`
	SecurityContext *corev1.SecurityContext      `json:"SecurityContext,omitempty"`
}

// EnterpriseLicenseSecretRef is the secret used in the Enterprise struct
type EnterpriseLicenseSecretRef struct {
	Key  string `json:"key,omitempty"`
	Name string `json:"name,omitempty"`
}

// Enterprise represents license data
type Enterprise struct {
	License          *string                     `json:"license,omitempty"`
	LicenseSecretRef *EnterpriseLicenseSecretRef `json:"licenseSecretRef,omitempty"`
}
