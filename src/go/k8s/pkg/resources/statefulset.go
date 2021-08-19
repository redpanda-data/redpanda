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
	"path/filepath"
	"strconv"

	"github.com/go-logr/logr"
	cmetav1 "github.com/jetstack/cert-manager/pkg/apis/meta/v1"
	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/labels"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/pointer"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

var _ Resource = &StatefulSetResource{}

var errNodePortMissing = errors.New("the node port is missing from the service")

const (
	redpandaContainerName     = "redpanda"
	configuratorContainerName = "redpanda-configurator"
	rpkStatusContainerName    = "rpk-status"

	userID  = 101
	groupID = 101
	fsGroup = 101

	configDestinationDir = "/etc/redpanda"
	configSourceDir      = "/mnt/operator"
	configFile           = "redpanda.yaml"

	datadirName            = "datadir"
	defaultDatadirCapacity = "100Gi"
)

// ConfiguratorSettings holds settings related to configurator container and deployment
// strategy
type ConfiguratorSettings struct {
	ConfiguratorBaseImage string
	ConfiguratorTag       string
	ImagePullPolicy       corev1.PullPolicy
}

// StatefulSetResource is part of the reconciliation of redpanda.vectorized.io CRD
// focusing on the management of redpanda cluster
type StatefulSetResource struct {
	k8sclient.Client
	scheme                             *runtime.Scheme
	pandaCluster                       *redpandav1alpha1.Cluster
	serviceFQDN                        string
	serviceName                        string
	nodePortName                       types.NamespacedName
	nodePortSvc                        corev1.Service
	redpandaCertSecretKey              types.NamespacedName
	internalClientCertSecretKey        types.NamespacedName
	adminCertSecretKey                 types.NamespacedName // TODO this is unused, can be removed
	adminAPINodeCertSecretKey          types.NamespacedName
	adminAPIClientCertSecretKey        types.NamespacedName // TODO this is unused, can be removed
	pandaproxyAPINodeCertSecretKey     types.NamespacedName
	schemaRegistryAPINodeCertSecretKey types.NamespacedName
	serviceAccountName                 string
	configuratorSettings               ConfiguratorSettings
	logger                             logr.Logger

	LastObservedState *appsv1.StatefulSet
}

// NewStatefulSet creates StatefulSetResource
func NewStatefulSet(
	client k8sclient.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
	serviceFQDN string,
	serviceName string,
	nodePortName types.NamespacedName,
	redpandaCertSecretKey types.NamespacedName,
	internalClientCertSecretKey types.NamespacedName,
	adminCertSecretKey types.NamespacedName,
	adminAPINodeCertSecretKey types.NamespacedName,
	adminAPIClientCertSecretKey types.NamespacedName,
	pandaproxyAPINodeCertSecretKey types.NamespacedName,
	schemaRegistryAPINodeCertSecretKey types.NamespacedName,
	serviceAccountName string,
	configuratorSettings ConfiguratorSettings,
	logger logr.Logger,
) *StatefulSetResource {
	return &StatefulSetResource{
		client,
		scheme,
		pandaCluster,
		serviceFQDN,
		serviceName,
		nodePortName,
		corev1.Service{},
		redpandaCertSecretKey,
		internalClientCertSecretKey,
		adminCertSecretKey,
		adminAPINodeCertSecretKey,
		adminAPIClientCertSecretKey,
		pandaproxyAPINodeCertSecretKey,
		schemaRegistryAPINodeCertSecretKey,
		serviceAccountName,
		configuratorSettings,
		logger.WithValues("Kind", statefulSetKind()),
		nil,
	}
}

// Ensure will manage kubernetes v1.StatefulSet for redpanda.vectorized.io custom resource
func (r *StatefulSetResource) Ensure(ctx context.Context) error {
	var sts appsv1.StatefulSet

	if r.pandaCluster.ExternalListener() != nil {
		err := r.Get(ctx, r.nodePortName, &r.nodePortSvc)
		if err != nil {
			return fmt.Errorf("failed to retrieve node port service %s: %w", r.nodePortName, err)
		}

		for _, port := range r.nodePortSvc.Spec.Ports {
			if port.NodePort == 0 {
				return fmt.Errorf("node port service %s, port %s is 0: %w", r.nodePortName, port.Name, errNodePortMissing)
			}
		}
	}

	obj, err := r.obj()
	if err != nil {
		return fmt.Errorf("unable to construct StatefulSet object: %w", err)
	}
	created, err := CreateIfNotExists(ctx, r, obj, r.logger)
	if err != nil {
		return err
	}
	if created {
		r.LastObservedState = obj.(*appsv1.StatefulSet)
		return nil
	}

	err = r.Get(ctx, r.Key(), &sts)
	if err != nil {
		return fmt.Errorf("error while fetching StatefulSet resource: %w", err)
	}
	r.LastObservedState = &sts

	partitioned, err := r.shouldUsePartitionedUpdate(&sts)
	if err != nil {
		return err
	}
	if partitioned {
		r.logger.Info(fmt.Sprintf("Going to run partitioned update on resource %s", r.Key().Name))
		if err := r.runPartitionedUpdate(ctx, &sts); err != nil {
			return fmt.Errorf("failed to run partitioned update: %w", err)
		}
	} else {
		modified, err := r.obj()
		if err != nil {
			return err
		}
		err = Update(ctx, &sts, modified, r.Client, r.logger)
		if err != nil {
			return err
		}
	}

	return nil
}

func preparePVCResource(
	name, namespace string,
	storage redpandav1alpha1.StorageSpec,
	clusterLabels labels.CommonLabels,
) corev1.PersistentVolumeClaim {
	fileSystemMode := corev1.PersistentVolumeFilesystem

	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Labels:    clusterLabels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(defaultDatadirCapacity),
				},
			},
			VolumeMode: &fileSystemMode,
		},
	}

	if storage.Capacity.Value() != 0 {
		pvc.Spec.Resources.Requests[corev1.ResourceStorage] = storage.Capacity
	}

	if len(storage.StorageClassName) > 0 {
		pvc.Spec.StorageClassName = &storage.StorageClassName
	}
	return pvc
}

// obj returns resource managed client.Object
// nolint:funlen // The complexity of obj function will be address in the next version TODO
func (r *StatefulSetResource) obj() (k8sclient.Object, error) {
	var clusterLabels = labels.ForCluster(r.pandaCluster)

	pvc := preparePVCResource(datadirName, r.pandaCluster.Namespace, r.pandaCluster.Spec.Storage, clusterLabels)
	annotations := r.pandaCluster.Spec.Annotations
	tolerations := r.pandaCluster.Spec.Tolerations
	nodeSelector := r.pandaCluster.Spec.NodeSelector

	if len(r.pandaCluster.Spec.Configuration.KafkaAPI) == 0 {
		// TODO
		return nil, nil
	}

	externalListener := r.pandaCluster.ExternalListener()
	externalSubdomain := ""
	if externalListener != nil {
		externalSubdomain = externalListener.External.Subdomain
	}

	ss := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.Key().Namespace,
			Name:      r.Key().Name,
			Labels:    clusterLabels,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "StatefulSet",
			APIVersion: "apps/v1",
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:            r.pandaCluster.Spec.Replicas,
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Selector:            clusterLabels.AsAPISelector(),
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
			},
			ServiceName: r.pandaCluster.Name,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:        r.pandaCluster.Name,
					Namespace:   r.pandaCluster.Namespace,
					Labels:      clusterLabels.AsAPISelector().MatchLabels,
					Annotations: annotations,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: r.getServiceAccountName(),
					SecurityContext: &corev1.PodSecurityContext{
						FSGroup: pointer.Int64Ptr(fsGroup),
					},
					Volumes: append([]corev1.Volume{
						{
							Name: datadirName,
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: datadirName,
								},
							},
						},
						{
							Name: "configmap-dir",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: ConfigMapKey(r.pandaCluster).Name,
									},
								},
							},
						},
						{
							Name: "config-dir",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					}, r.secretVolumes()...),
					InitContainers: []corev1.Container{
						{
							Name:            configuratorContainerName,
							Image:           r.fullConfiguratorImage(),
							ImagePullPolicy: r.configuratorSettings.ImagePullPolicy,
							Env: append([]corev1.EnvVar{
								{
									Name:  "SERVICE_FQDN",
									Value: r.serviceFQDN,
								},
								{
									Name:  "CONFIG_SOURCE_DIR",
									Value: configSourceDir,
								},
								{
									Name:  "CONFIG_DESTINATION",
									Value: filepath.Join(configDestinationDir, configFile),
								},
								{
									Name:  "REDPANDA_RPC_PORT",
									Value: strconv.Itoa(r.pandaCluster.Spec.Configuration.RPCServer.Port),
								},
								{
									Name: "NODE_NAME",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											APIVersion: "v1",
											FieldPath:  "spec.nodeName",
										},
									},
								},
								{
									Name:  "EXTERNAL_CONNECTIVITY",
									Value: strconv.FormatBool(externalListener != nil),
								},
								{
									Name:  "EXTERNAL_CONNECTIVITY_SUBDOMAIN",
									Value: externalSubdomain,
								},
								{
									Name:  "HOST_PORT",
									Value: r.getNodePort(ExternalListenerName),
								},
							}, r.pandaproxyEnvVars()...),
							SecurityContext: &corev1.SecurityContext{
								RunAsUser:  pointer.Int64Ptr(userID),
								RunAsGroup: pointer.Int64Ptr(groupID),
							},
							Resources: corev1.ResourceRequirements{
								Limits:   r.pandaCluster.Spec.Resources.Limits,
								Requests: r.pandaCluster.Spec.Resources.Requests,
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "config-dir",
									MountPath: configDestinationDir,
								},
								{
									Name:      "configmap-dir",
									MountPath: configSourceDir,
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:    redpandaContainerName,
							Image:   r.pandaCluster.FullImageName(),
							Command: []string{"/usr/bin/rpk"},
							Args: append([]string{
								"redpanda",
								"start",
								"--check=false",
								r.portsConfiguration(),
							}, prepareAdditionalArguments(
								r.pandaCluster.Spec.Configuration.DeveloperMode,
								r.pandaCluster.Spec.Resources.Requests)...),
							Env: []corev1.EnvVar{
								{
									Name:  "REDPANDA_ENVIRONMENT",
									Value: "kubernetes",
								},
								{
									Name: "POD_NAME",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											APIVersion: "v1",
											FieldPath:  "metadata.name",
										},
									},
								},
								{
									Name: "POD_NAMESPACE",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											APIVersion: "v1",
											FieldPath:  "metadata.namespace",
										},
									},
								},
								{
									Name: "POD_IP",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											APIVersion: "v1",
											FieldPath:  "status.podIP",
										},
									},
								},
							},
							Ports: append([]corev1.ContainerPort{
								{
									Name:          "rpc",
									ContainerPort: int32(r.pandaCluster.Spec.Configuration.RPCServer.Port),
								},
							}, r.getPorts()...),
							StartupProbe: r.getStartupProbe(),
							Resources: corev1.ResourceRequirements{
								Limits:   r.pandaCluster.Spec.Resources.Limits,
								Requests: r.pandaCluster.Spec.Resources.Requests,
							},
							VolumeMounts: append([]corev1.VolumeMount{
								{
									Name:      datadirName,
									MountPath: dataDirectory,
								},
								{
									Name:      "config-dir",
									MountPath: configDestinationDir,
								},
							}, r.secretVolumeMounts()...),
						},
					},
					Tolerations:  tolerations,
					NodeSelector: nodeSelector,
					Affinity: &corev1.Affinity{
						PodAntiAffinity: &corev1.PodAntiAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
								{
									LabelSelector: clusterLabels.AsAPISelector(),
									Namespaces:    []string{r.pandaCluster.Namespace},
									TopologyKey:   corev1.LabelHostname},
							},
							PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
								{
									Weight: 100,
									PodAffinityTerm: corev1.PodAffinityTerm{
										LabelSelector: clusterLabels.AsAPISelector(),
										Namespaces:    []string{r.pandaCluster.Namespace},
										TopologyKey:   corev1.LabelHostname,
									},
								},
							},
						},
					},
					TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
						{
							MaxSkew:           1,
							TopologyKey:       corev1.LabelZoneFailureDomainStable,
							WhenUnsatisfiable: corev1.ScheduleAnyway,
							LabelSelector:     clusterLabels.AsAPISelector(),
						},
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				pvc,
			},
		},
	}

	rpkStatusContainer := r.rpkStatusContainer()
	if rpkStatusContainer != nil {
		ss.Spec.Template.Spec.Containers = append(ss.Spec.Template.Spec.Containers, *rpkStatusContainer)
	}

	err := controllerutil.SetControllerReference(r.pandaCluster, ss, r.scheme)
	if err != nil {
		return nil, err
	}

	return ss, nil
}
func (r *StatefulSetResource) rpkStatusContainer() *corev1.Container {
	if r.pandaCluster.Spec.Sidecars.RpkStatus == nil {
		r.logger.Info("BUG! No resources found for rpk status - this should never happen with defaulting webhook enabled - please consider enabling the webhook")
		r.pandaCluster.Spec.Sidecars.RpkStatus = &redpandav1alpha1.Sidecar{
			Enabled:   true,
			Resources: redpandav1alpha1.DefaultRpkStatusResources,
		}
	}
	if !r.pandaCluster.Spec.Sidecars.RpkStatus.Enabled {
		return nil
	}
	return &corev1.Container{
		Name:    rpkStatusContainerName,
		Image:   r.pandaCluster.FullImageName(),
		Command: []string{"/usr/local/bin/rpk-status.sh"},
		Env: []corev1.EnvVar{
			{
				Name:  "REDPANDA_ENVIRONMENT",
				Value: "kubernetes",
			},
		},
		Resources: *r.pandaCluster.Spec.Sidecars.RpkStatus.Resources,
		VolumeMounts: append([]corev1.VolumeMount{
			{
				Name:      "config-dir",
				MountPath: configDestinationDir,
			},
		}, r.secretVolumeMounts()...),
	}
}

func prepareAdditionalArguments(
	developerMode bool, originalRequests corev1.ResourceList,
) []string {
	requests := originalRequests.DeepCopy()

	requests.Cpu().RoundUp(0)
	requestedCores := requests.Cpu().Value()
	requestedMemory := requests.Memory().Value()

	if developerMode {
		args := []string{
			"--overprovisioned",
			"--kernel-page-cache=true",
			"--default-log-level=debug",
		}
		// When smp is not set, all cores are used
		if requestedCores > 0 {
			args = append(args, "--smp="+strconv.FormatInt(requestedCores, 10))
		}
		return args
	}

	args := []string{
		"--default-log-level=info",
		"--reserve-memory=0M",
	}

	/*
	 * Example:
	 *    in: minimum requirement per core, 2GB
	 *    in: requestedMemory, 16GB
	 *    => maxAllowedCores = 8
	 *    if requestedCores == 8, set smp = 8 (with 2GB per core)
	 *    if requestedCores == 4, set smp = 4 (with 4GB per core)
	 */

	// The webhook ensures that the requested memory is >= the per-core requirement
	maxAllowedCores := requestedMemory / redpandav1alpha1.MinimumMemoryPerCore
	var smp int64 = maxAllowedCores
	if requestedCores != 0 && requestedCores < smp {
		smp = requestedCores
	}
	args = append(args, "--smp="+strconv.FormatInt(smp, 10),
		"--memory="+strconv.FormatInt(requestedMemory, 10))

	return args
}

func (r *StatefulSetResource) pandaproxyEnvVars() []corev1.EnvVar {
	var envs []corev1.EnvVar
	listener := r.pandaCluster.PandaproxyAPIExternal()
	if listener != nil {
		envs = append(envs, corev1.EnvVar{
			Name:  "PROXY_HOST_PORT",
			Value: r.getNodePort(PandaproxyPortExternalName),
		})
	}
	return envs
}

func (r *StatefulSetResource) secretVolumeMounts() []corev1.VolumeMount {
	var mounts []corev1.VolumeMount
	tlsListener := r.pandaCluster.KafkaTLSListener()
	if tlsListener != nil {
		mounts = append(mounts, corev1.VolumeMount{
			Name:      "tlscert",
			MountPath: tlsDir,
		})
	}
	if tlsListener != nil && tlsListener.TLS.RequireClientAuth {
		mounts = append(mounts, corev1.VolumeMount{
			Name:      "tlsca",
			MountPath: tlsDirCA,
		})
	}
	if r.pandaCluster.AdminAPITLS() != nil {
		mounts = append(mounts, corev1.VolumeMount{
			Name:      "tlsadmincert",
			MountPath: tlsAdminDir,
		})
	}
	if r.pandaCluster.PandaproxyAPITLS() != nil {
		mounts = append(mounts, corev1.VolumeMount{
			Name:      "tlspandaproxycert",
			MountPath: tlsPandaproxyDir,
		})
	}
	if r.pandaCluster.Spec.Configuration.SchemaRegistry != nil &&
		r.pandaCluster.Spec.Configuration.SchemaRegistry.TLS != nil {
		mounts = append(mounts, corev1.VolumeMount{
			Name:      "tlsschemaregistrycert",
			MountPath: tlsSchemaRegistryDir,
		})
	}
	return mounts
}

// nolint:funlen // The controller should have more focused feature
// oriented functions. E.g. each TLS volume could be managed by
// one function along side with root certificate, issuer, certificate and
// volumesMount in statefulset.
func (r *StatefulSetResource) secretVolumes() []corev1.Volume {
	var vols []corev1.Volume

	// When TLS is enabled, Redpanda needs a keypair certificate.
	tlsListener := r.pandaCluster.KafkaTLSListener()
	if tlsListener != nil {
		vols = append(vols, corev1.Volume{
			Name: "tlscert",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: r.redpandaCertSecretKey.Name,
					Items: []corev1.KeyToPath{
						{
							Key:  corev1.TLSPrivateKeyKey,
							Path: corev1.TLSPrivateKeyKey,
						},
						{
							Key:  corev1.TLSCertKey,
							Path: corev1.TLSCertKey,
						},
					},
				},
			},
		})
	}

	// When TLS client authentication is enabled, Redpanda needs the client's CA certificate.
	if tlsListener != nil && tlsListener.TLS.RequireClientAuth {
		vols = append(vols, corev1.Volume{
			Name: "tlsca",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: r.internalClientCertSecretKey.Name,
					Items: []corev1.KeyToPath{
						{
							Key:  cmetav1.TLSCAKey,
							Path: cmetav1.TLSCAKey,
						},
					},
				},
			},
		})
	}

	// When Admin TLS is enabled, Redpanda needs a keypair certificate.
	if r.pandaCluster.AdminAPITLS() != nil {
		vols = append(vols, corev1.Volume{
			Name: "tlsadmincert",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: r.adminAPINodeCertSecretKey.Name,
					Items: []corev1.KeyToPath{
						{
							Key:  corev1.TLSPrivateKeyKey,
							Path: corev1.TLSPrivateKeyKey,
						},
						{
							Key:  corev1.TLSCertKey,
							Path: corev1.TLSCertKey,
						},
						{
							Key:  cmetav1.TLSCAKey,
							Path: cmetav1.TLSCAKey,
						},
					},
				},
			},
		})
	}

	// When Pandaproxy TLS is enabled, Redpanda needs a keypair certificate.
	if r.pandaCluster.PandaproxyAPITLS() != nil {
		vols = append(vols, corev1.Volume{
			Name: "tlspandaproxycert",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: r.pandaproxyAPINodeCertSecretKey.Name,
					Items: []corev1.KeyToPath{
						{
							Key:  corev1.TLSPrivateKeyKey,
							Path: corev1.TLSPrivateKeyKey,
						},
						{
							Key:  corev1.TLSCertKey,
							Path: corev1.TLSCertKey,
						},
						{
							Key:  cmetav1.TLSCAKey,
							Path: cmetav1.TLSCAKey,
						},
					},
				},
			},
		})
	}

	if r.pandaCluster.Spec.Configuration.SchemaRegistry != nil &&
		r.pandaCluster.Spec.Configuration.SchemaRegistry.TLS != nil {
		vols = append(vols, corev1.Volume{
			Name: "tlsschemaregistrycert",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: r.schemaRegistryAPINodeCertSecretKey.Name,
					Items: []corev1.KeyToPath{
						{
							Key:  corev1.TLSPrivateKeyKey,
							Path: corev1.TLSPrivateKeyKey,
						},
						{
							Key:  corev1.TLSCertKey,
							Path: corev1.TLSCertKey,
						},
						{
							Key:  cmetav1.TLSCAKey,
							Path: cmetav1.TLSCAKey,
						},
					},
				},
			},
		})
	}

	return vols
}

func (r *StatefulSetResource) getNodePort(name string) string {
	for _, port := range r.nodePortSvc.Spec.Ports {
		if port.Name == name {
			return strconv.FormatInt(int64(port.NodePort), 10)
		}
	}
	return ""
}

func (r *StatefulSetResource) getServiceAccountName() string {
	if r.pandaCluster.ExternalListener() != nil {
		return r.serviceAccountName
	}
	return ""
}

// Key returns namespace/name object that is used to identify object.
// For reference please visit types.NamespacedName docs in k8s.io/apimachinery
func (r *StatefulSetResource) Key() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name, Namespace: r.pandaCluster.Namespace}
}

func (r *StatefulSetResource) portsConfiguration() string {
	rpcAPIPort := r.pandaCluster.Spec.Configuration.RPCServer.Port
	serviceFQDN := r.serviceFQDN

	return fmt.Sprintf("--advertise-rpc-addr=$(POD_NAME).%s:%d", serviceFQDN, rpcAPIPort)
}

func (r *StatefulSetResource) getPorts() []corev1.ContainerPort {
	ports := []corev1.ContainerPort{}

	if adminAPIInternal := r.pandaCluster.AdminAPIInternal(); adminAPIInternal != nil {
		ports = append(ports, corev1.ContainerPort{
			Name:          AdminPortName,
			ContainerPort: int32(adminAPIInternal.Port),
		})
	}

	if internalListener := r.pandaCluster.InternalListener(); internalListener != nil {
		ports = append(ports, corev1.ContainerPort{
			Name:          InternalListenerName,
			ContainerPort: int32(internalListener.Port),
		})
	}

	if internalProxy := r.pandaCluster.PandaproxyAPIInternal(); internalProxy != nil {
		ports = append(ports, corev1.ContainerPort{
			Name:          PandaproxyPortInternalName,
			ContainerPort: int32(internalProxy.Port),
		})
	}

	if r.pandaCluster.Spec.Configuration.SchemaRegistry != nil &&
		r.pandaCluster.Spec.Configuration.SchemaRegistry.External != nil &&
		!r.pandaCluster.Spec.Configuration.SchemaRegistry.External.Enabled {
		ports = append(ports, corev1.ContainerPort{
			Name:          SchemaRegistryPortName,
			ContainerPort: int32(r.pandaCluster.Spec.Configuration.SchemaRegistry.Port),
		})
	}

	if len(r.nodePortSvc.Spec.Ports) > 0 {
		for _, port := range r.nodePortSvc.Spec.Ports {
			ports = append(ports, corev1.ContainerPort{
				Name: port.Name,
				// To distinguish external from internal clients the new listener
				// and port is exposed for Redpanda clients. The port is chosen
				// arbitrary to the KafkaAPI + 1, because user can not reach this
				// port. The routing in the Kubernetes will forward all traffic from
				// HostPort to the ContainerPort.
				ContainerPort: port.TargetPort.IntVal,
				// The host port is set to the service node port that doesn't have
				// any endpoints.
				HostPort: port.NodePort,
			})
		}
		return ports
	}

	return ports
}

func (r *StatefulSetResource) getStartupProbe() *corev1.Probe {
	adminApi := r.pandaCluster.AdminAPIInternal()
	if adminApi == nil {
		r.logger.Info("BUG! an internal listener for admin API is required")
		return nil
	}
	// TODO: add support for installations with TLS on internal admin API
	if adminApi.TLS.Enabled || adminApi.TLS.RequireClientAuth {
		r.logger.Info("WARNING! with TLS enabled for the internal adminAPI, the operator won't add a Startup probe to Redpanda nodes which might be problematic during upgrades or when adding nodes to the cluster")
		return nil
	}

	baseProbe := corev1.Probe{
		Handler: corev1.Handler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "v1/status/started",
				Port:   intstr.FromInt(adminApi.Port),
				Scheme: "HTTP",
			},
		},
		// TODO: make max retries configurable
		//
		// For clusters with large amounts of data, the startup of a new node can take 10s of minutes.
		// As a measure of precaution, we're setting the min time for the probe to fail to ~1h.
		InitialDelaySeconds: 1,
		TimeoutSeconds:      1,
		PeriodSeconds:       1,
		SuccessThreshold:    1, // Once the node is in "started" state, it won't go back to "booting" state
		FailureThreshold:    3000,
	}

	return &baseProbe
}

func statefulSetKind() string {
	var statefulSet appsv1.StatefulSet
	return statefulSet.Kind
}

func (r *StatefulSetResource) fullConfiguratorImage() string {
	return fmt.Sprintf("%s:%s", r.configuratorSettings.ConfiguratorBaseImage, r.configuratorSettings.ConfiguratorTag)
}
