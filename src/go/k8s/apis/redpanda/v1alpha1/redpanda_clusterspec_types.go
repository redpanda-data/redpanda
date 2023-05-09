package v1alpha1

import (
	"encoding/json"
)

// RedpandaClusterSpec defines the desired state of Redpanda Cluster
type RedpandaClusterSpec struct {
	// NameOverride is the override to give your redpanda release
	NameOverride string `json:"nameOverride,omitempty"`
	// FullnameOverride is the override to give your redpanda release
	FullnameOverride string `json:"fullNameOverride,omitempty"`
	// ClusterDomain is the override to give your redpanda release
	ClusterDomain string `json:"clusterDomain,omitempty"`
	// CommonLabels is the override to give your redpanda release
	CommonLabels map[string]string `json:"commonLabels,omitempty"`
	// NodeSelector is the override to give your redpanda release
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// Tolerations is the override to give your redpanda release
	Tolerations []string `json:"tolerations,omitempty"`

	// Image defines the container image to use for the redpanda cluster
	Image *RedpandaImage `json:"image,omitempty"`

	LicenseKey       string            `json:"license_key,omitempty"`
	LicenseSecretRef *LicenseSecretRef `json:"license_secret_ref,omitempty"`

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
}

// RedpandaImage is a top level field of the values file
type RedpandaImage struct {
	Repository string `json:"repository,omitempty"`
	Tag        string `json:"tag,omitempty"`
	PullPolicy string `json:"pullPolicy,omitempty"`
}

// LicenseSecretRef is a top level field of the values file
type LicenseSecretRef struct {
	SecretKey  string `json:"secret_key,omitempty"`
	SecretName string `json:"secret_name,omitempty"`
}

// RackAwareness is a top level field of the values file
type RackAwareness struct {
	Enabled        bool   `json:"enabled"`
	NodeAnnotation string `json:"nodeAnnotation"`
}

type RedpandaConsole struct {
	// to be filled in
}

// Auth is a top level field of the values file
type Auth struct {
	SASL *SASL `json:"sasl"`
}

// SASL is a top level field of the values file
type SASL struct {
	Enabled   bool          `json:"enabled"`
	Mechanism string        `json:"mechanism,omitempty"`
	SecretRef string        `json:"secretRef,omitempty"`
	Users     []*UsersItems `json:"users,omitempty"`
}

// UsersItems is a top level field of the values file
type UsersItems struct {
	Mechanism string `json:"mechanism,omitempty"`
	Name      string `json:"name,omitempty"`
	Password  string `json:"password,omitempty"`
}

// TLS is a top level field of the values file
type TLS struct {
	Certs   *Certs `json:"certs,omitempty"`
	Enabled bool   `json:"enabled"`
}

// Certs is a top level field of the values file
type Certs struct{}

// External is a top level field of the values file
type External struct {
	Addresses json.RawMessage `json:"addresses,omitempty"`
	Domain    string          `json:"domain,omitempty"`
	Enabled   bool            `json:"enabled"`
	Type      string          `json:"type,omitempty"`
}

// Logging is a top level field of the values file
type Logging struct{}

// Resources is a top level field of the values file
type Resources struct {
	Limits   *Limits   `json:"limits,omitempty"`
	Requests *Requests `json:"requests,omitempty"`
}

// Limits is a top level field of the values file
type Limits struct {
	CPU    int    `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
}

// Requests is a top level field of the values file
type Requests struct {
	CPU    int    `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
}

// Storage is a top level field of the values file
type Storage struct {
	HostPath                      string                         `json:"hostPath"`
	PersistentVolume              *PersistentVolume              `json:"persistentVolume"`
	TieredConfig                  *TieredConfig                  `json:"tieredConfig,omitempty"`
	TieredStorageHostPath         string                         `json:"tieredStorageHostPath,omitempty"`
	TieredStoragePersistentVolume *TieredStoragePersistentVolume `json:"tieredStoragePersistentVolume,omitempty"`
}

// TieredConfig is a top level field of the values file
type TieredConfig struct {
	CloudStorageAPIEndpoint                 string `json:"cloud_storage_api_endpoint,omitempty"`
	CloudStorageAPIEndpointPort             int    `json:"cloud_storage_api_endpoint_port,omitempty"`
	CloudStorageBucket                      string `json:"cloud_storage_bucket"`
	CloudStorageCacheCheckInterval          int    `json:"cloud_storage_cache_check_interval,omitempty"`
	CloudStorageCacheDirectory              string `json:"cloud_storage_cache_directory,omitempty"`
	CloudStorageCacheSize                   int    `json:"cloud_storage_cache_size,omitempty"`
	CloudStorageCredentialsSource           string `json:"cloud_storage_credentials_source,omitempty"`
	CloudStorageDisableTLS                  bool   `json:"cloud_storage_disable_tls,omitempty"`
	CloudStorageEnableRemoteRead            bool   `json:"cloud_storage_enable_remote_read,omitempty"`
	CloudStorageEnableRemoteWrite           bool   `json:"cloud_storage_enable_remote_write,omitempty"`
	CloudStorageInitialBackoffMs            int    `json:"cloud_storage_initial_backoff_ms,omitempty"`
	CloudStorageManifestUploadTimeoutMs     int    `json:"cloud_storage_manifest_upload_timeout_ms,omitempty"`
	CloudStorageMaxConnectionIdleTimeMs     int    `json:"cloud_storage_max_connection_idle_time_ms,omitempty"`
	CloudStorageMaxConnections              int    `json:"cloud_storage_max_connections,omitempty"`
	CloudStorageReconciliationIntervalMs    int    `json:"cloud_storage_reconciliation_interval_ms,omitempty"`
	CloudStorageRegion                      string `json:"cloud_storage_region"`
	CloudStorageSegmentMaxUploadIntervalSec int    `json:"cloud_storage_segment_max_upload_interval_sec,omitempty"`
	CloudStorageSegmentUploadTimeoutMs      int    `json:"cloud_storage_segment_upload_timeout_ms,omitempty"`
	CloudStorageTrustFile                   string `json:"cloud_storage_trust_file,omitempty"`
	CloudStorageUploadCtrlDCoeff            int    `json:"cloud_storage_upload_ctrl_d_coeff,omitempty"`
	CloudStorageUploadCtrlMaxShares         int    `json:"cloud_storage_upload_ctrl_max_shares,omitempty"`
	CloudStorageUploadCtrlMinShares         int    `json:"cloud_storage_upload_ctrl_min_shares,omitempty"`
	CloudStorageUploadCtrlPCoeff            int    `json:"cloud_storage_upload_ctrl_p_coeff,omitempty"`
	CloudStorageUploadCtrlUpdateIntervalMs  int    `json:"cloud_storage_upload_ctrl_update_interval_ms,omitempty"`
}

// TieredStoragePersistentVolume is a top level field of the values file
type TieredStoragePersistentVolume struct {
	Annotations  map[string]string `json:"annotations"`
	Enabled      bool              `json:"enabled"`
	Labels       map[string]string `json:"labels"`
	StorageClass string            `json:"storageClass"`
}

// PersistentVolume is a top level field of the values file
type PersistentVolume struct {
	Annotations  map[string]string `json:"annotations,omitempty"`
	Enabled      bool              `json:"enabled"`
	Labels       map[string]string `json:"labels,omitempty"`
	Size         string            `json:"size,omitempty"`
	StorageClass string            `json:"storageClass,omitempty"`
}

// PostInstallJob is a top level field of the values file
type PostInstallJob struct {
	Enabled   bool       `json:"enabled"`
	Resources *Resources `json:"resources,omitempty"`
}

// PostUpgradeJob is a top level field of the values file
type PostUpgradeJob struct {
	Enabled      bool            `json:"enabled"`
	ExtraEnv     json.RawMessage `json:"extraEnv,omitempty"`
	ExtraEnvFrom json.RawMessage `json:"extraEnvFrom,omitempty"`
	Resources    *Resources      `json:"resources,omitempty"`
}

// Statefulset is a top level field of the values file
type Statefulset struct {
	Annotations               map[string]string          `json:"annotations,omitempty"`
	Budget                    *Budget                    `json:"budget,omitempty"`
	InitContainer             string                     `json:"initContainer,omitempty"`
	LivenessProbe             *LivenessProbe             `json:"livenessProbe,omitempty"`
	NodeSelector              map[string]string          `json:"nodeSelector,omitempty"`
	PodAffinity               json.RawMessage            `json:"podAffinity,omitempty"`
	PodAntiAffinity           *PodAntiAffinity           `json:"podAntiAffinity,omitempty"`
	PriorityClassName         string                     `json:"priorityClassName,omitempty"`
	ReadinessProbe            *ReadinessProbe            `json:"readinessProbe,omitempty"`
	Replicas                  int                        `json:"replicas,omitempty"`
	SecurityContext           *SecurityContext           `json:"securityContext,omitempty"`
	SkipChown                 bool                       `json:"skipChown,omitempty"`
	StartupProbe              *StartupProbe              `json:"startupProbe,omitempty"`
	Tolerations               []string                   `json:"tolerations,omitempty"`
	TopologySpreadConstraints *TopologySpreadConstraints `json:"topologySpreadConstraints,omitempty"`
	UpdateStrategy            *UpdateStrategy            `json:"updateStrategy,omitempty"`
}

// Budget is a top level field of the values file
type Budget struct {
	MaxUnavailable int `json:"maxUnavailable"`
}

// LivenessProbe is a top level field of the values file
type LivenessProbe struct {
	FailureThreshold    int `json:"failureThreshold"`
	InitialDelaySeconds int `json:"initialDelaySeconds"`
	PeriodSeconds       int `json:"periodSeconds"`
}

// ReadinessProbe is a top level field of the values file
type ReadinessProbe struct {
	FailureThreshold    int `json:"failureThreshold"`
	InitialDelaySeconds int `json:"initialDelaySeconds"`
	PeriodSeconds       int `json:"periodSeconds"`
}

// SecurityContext is a top level field of the values file
type SecurityContext struct {
	FsGroup   int `json:"fsGroup"`
	RunAsUser int `json:"runAsUser"`
}

// StartupProbe is a top level field of the values file
type StartupProbe struct {
	FailureThreshold    int `json:"failureThreshold"`
	InitialDelaySeconds int `json:"initialDelaySeconds"`
	PeriodSeconds       int `json:"periodSeconds"`
}

// PodAntiAffinity is a top level field of the values file
type PodAntiAffinity struct {
	TopologyKey string          `json:"topologyKey"`
	Type        string          `json:"type"`
	Weight      int             `json:"weight"`
	Custom      json.RawMessage `json:"custom,omitempty"`
}

// TopologySpreadConstraints is a top level field of the values file
type TopologySpreadConstraints struct {
	MaxSkew           int    `json:"maxSkew"`
	TopologyKey       string `json:"topologyKey"`
	WhenUnsatisfiable string `json:"whenUnsatisfiable"`
}

// UpdateStrategy is a top level field of the values file
type UpdateStrategy struct {
	Type string `json:"type"`
}

// Tuning is a top level field of the values file
type Tuning struct {
	BallastFilePath string `json:"ballast_file_path,omitempty"`
	BallastFileSize string `json:"ballast_file_size,omitempty"`
	TuneAioEvents   bool   `json:"tune_aio_events,omitempty"`
	TuneBallastFile bool   `json:"tune_ballast_file,omitempty"`
	TuneClockSource bool   `json:"tune_clocksource,omitempty"`
	WellKnownIo     string `json:"well_known_io,omitempty"`
}

// Listeners is a top level field of the values file
type Listeners struct {
	Admin          *Admin          `json:"admin,omitempty"`
	HTTP           *HTTP           `json:"http,omitempty"`
	Kafka          *Kafka          `json:"kafka,omitempty"`
	RPC            *RPC            `json:"rpc,omitempty"`
	SchemaRegistry *SchemaRegistry `json:"schemaRegistry,omitempty"`
}

// Admin is a top level field of the values file
type Admin struct {
	External *External `json:"external"`
	Port     int       `json:"port"`
	TLS      *TLS      `json:"tls"`
}

// HTTP is a top level field of the values file
type HTTP struct {
	Enabled       bool      `json:"enabled"`
	External      *External `json:"external"`
	KafkaEndpoint string    `json:"kafkaEndpoint"`
	Port          int       `json:"port"`
	TLS           *TLS      `json:"tls"`
}

// Kafka is a top level field of the values file
type Kafka struct {
	External *External `json:"external"`
	Port     int       `json:"port"`
	TLS      *TLS      `json:"tls"`
}

// RPC is a top level field of the values file
type RPC struct {
	Port int  `json:"port"`
	TLS  *TLS `json:"tls"`
}

// SchemaRegistry is a top level field of the values file
type SchemaRegistry struct {
	Enabled       bool      `json:"enabled"`
	External      *External `json:"external"`
	KafkaEndpoint string    `json:"kafkaEndpoint"`
	Port          int       `json:"port"`
	TLS           *TLS      `json:"tls"`
}

// Config is a top level field of the values file
type Config struct {
	Cluster json.RawMessage `json:"cluster,omitempty"`
	Node    json.RawMessage `json:"node,omitempty"`
	Tunable json.RawMessage `json:"tunable,omitempty"`
}
