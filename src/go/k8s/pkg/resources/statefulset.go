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
	"context"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"strconv"

	"github.com/go-logr/logr"
	cmetav1 "github.com/jetstack/cert-manager/pkg/apis/meta/v1"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/featuregates"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

	datadirName                  = "datadir"
	archivalCacheIndexAnchorName = "shadow-index-cache"
	defaultDatadirCapacity       = "100Gi"

	redpandaCertVolName       = "tlscert"
	redpandaCAVolName         = "tlsca"
	adminAPICertVolName       = "tlsadmincert"
	adminAPICAVolName         = "tlsadminca"
	pandaProxyCertVolName     = "tlspandaproxycert"
	pandaProxyCAVolName       = "tlspandaproxyca"
	schemaRegistryCertVolName = "tlsschemaregistrycert"
	schemaRegistryCAVolName   = "tlsschemaregistryca"
)

var (
	// ConfigMapHashAnnotationKey contains the hash of the node local properties of the cluster
	ConfigMapHashAnnotationKey = redpandav1alpha1.GroupVersion.Group + "/configmap-hash"
	// CentralizedConfigurationHashAnnotationKey contains the hash of the centralized configuration properties that require a restart when changed
	CentralizedConfigurationHashAnnotationKey = redpandav1alpha1.GroupVersion.Group + "/centralized-configuration-hash"
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
	adminCertSecretKey                 types.NamespacedName
	adminAPINodeCertSecretKey          types.NamespacedName
	adminAPIClientCertSecretKey        types.NamespacedName
	pandaproxyAPINodeCertSecretKey     types.NamespacedName
	pandaproxyClientCertSecretKey      types.NamespacedName
	schemaRegistryAPINodeCertSecretKey types.NamespacedName
	schemaRegistryClientCertSecretKey  types.NamespacedName
	serviceAccountName                 string
	configuratorSettings               ConfiguratorSettings
	// hash of configmap containing configuration for redpanda (node config only), it's injected to
	// annotation to ensure the pods get restarted when configuration changes
	// this has to be retrieved lazily to achieve the correct order of resources
	// being applied
	nodeConfigMapHashGetter func(context.Context) (string, error)
	logger                  logr.Logger

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
	pandaproxyClientCertSecretKey types.NamespacedName,
	schemaRegistryAPINodeCertSecretKey types.NamespacedName,
	schemaRegistryClientCertSecretKey types.NamespacedName,
	serviceAccountName string,
	configuratorSettings ConfiguratorSettings,
	nodeConfigMapHashGetter func(context.Context) (string, error),
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
		pandaproxyClientCertSecretKey,
		schemaRegistryAPINodeCertSecretKey,
		schemaRegistryClientCertSecretKey,
		serviceAccountName,
		configuratorSettings,
		nodeConfigMapHashGetter,
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

	obj, err := r.obj(ctx)
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
	r.logger.Info("Running update", "resource name", r.Key().Name)
	return r.runUpdate(ctx, &sts, obj.(*appsv1.StatefulSet))
}

// GetCentralizedConfigurationHashFromCluster retrieves the current centralized configuratino hash from the statefulset
func (r *StatefulSetResource) GetCentralizedConfigurationHashFromCluster(
	ctx context.Context,
) (string, error) {
	existing := appsv1.StatefulSet{}
	if err := r.Client.Get(ctx, r.Key(), &existing); err != nil {
		return "", fmt.Errorf("could not load statefulset for reading the centralized configuration hash: %w", err)
	}
	if hash, ok := existing.Spec.Template.Annotations[CentralizedConfigurationHashAnnotationKey]; ok {
		return hash, nil
	}
	return "", nil
}

// SetCentralizedConfigurationHashInCluster saves the given centralized configuration hash in the statefulset
func (r *StatefulSetResource) SetCentralizedConfigurationHashInCluster(
	ctx context.Context, hash string,
) error {
	existing := appsv1.StatefulSet{}
	if err := r.Client.Get(ctx, r.Key(), &existing); err != nil {
		if apierrors.IsNotFound(err) {
			// No place where to store it
			return nil
		}
		return fmt.Errorf("could not load statefulset for storing the centralized configuration hash: %w", err)
	}
	if existing.Spec.Template.Annotations == nil {
		existing.Spec.Template.Annotations = make(map[string]string)
	}
	existing.Spec.Template.Annotations[CentralizedConfigurationHashAnnotationKey] = hash
	return r.Update(ctx, &existing)
}

func preparePVCResource(
	name, namespace string,
	storage redpandav1alpha1.StorageSpec,
	clusterLabels map[string]string,
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
func (r *StatefulSetResource) obj(
	ctx context.Context,
) (k8sclient.Object, error) {
	var clusterLabels = labels.ForCluster(r.pandaCluster)

	annotations := r.pandaCluster.Spec.Annotations
	if annotations == nil {
		annotations = map[string]string{}
	}
	configMapHash, err := r.nodeConfigMapHashGetter(ctx)
	if err != nil {
		return nil, err
	}
	annotations[ConfigMapHashAnnotationKey] = configMapHash
	tolerations := r.pandaCluster.Spec.Tolerations
	nodeSelector := r.pandaCluster.Spec.NodeSelector

	if len(r.pandaCluster.Spec.Configuration.KafkaAPI) == 0 {
		// TODO
		return nil, nil
	}

	externalListener := r.pandaCluster.ExternalListener()
	externalSubdomain := ""
	externalAddressType := ""
	if externalListener != nil {
		externalSubdomain = externalListener.External.Subdomain
		externalAddressType = externalListener.External.PreferredAddressType
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
				Type: appsv1.OnDeleteStatefulSetStrategyType,
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
									Name:  "EXTERNAL_CONNECTIVITY_ADDRESS_TYPE",
									Value: externalAddressType,
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
								r.pandaCluster.Spec.Resources)...),
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
							Resources: corev1.ResourceRequirements{
								Limits:   r.pandaCluster.Spec.Resources.Limits,
								Requests: r.pandaCluster.Spec.Resources.Requests,
							},
							VolumeMounts: append([]corev1.VolumeMount{
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
		},
	}

	if featuregates.CentralizedConfiguration(r.pandaCluster.Spec.Version) {
		ss.Spec.Template.Spec.Containers[0].VolumeMounts = append(ss.Spec.Template.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
			Name:      "configmap-dir",
			MountPath: path.Join(configDestinationDir, bootstrapConfigFile),
			SubPath:   bootstrapConfigFile,
		})
	}

	setCloudStorage(ss, r.pandaCluster)

	rpkStatusContainer := r.rpkStatusContainer()
	if rpkStatusContainer != nil {
		ss.Spec.Template.Spec.Containers = append(ss.Spec.Template.Spec.Containers, *rpkStatusContainer)
	}

	err = controllerutil.SetControllerReference(r.pandaCluster, ss, r.scheme)
	if err != nil {
		return nil, err
	}

	return ss, nil
}

// setCloudStorage manipulates v1.StatefulSet object in order to add cloud storage specific
// properties to Redpanda pod.
func setCloudStorage(
	ss *appsv1.StatefulSet, cluster *redpandav1alpha1.Cluster,
) {
	pvcDataDir := preparePVCResource(datadirName, cluster.Namespace, cluster.Spec.Storage, ss.Labels)
	ss.Spec.VolumeClaimTemplates = append(ss.Spec.VolumeClaimTemplates, pvcDataDir)
	vol := corev1.Volume{
		Name: datadirName,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: datadirName,
			},
		},
	}
	ss.Spec.Template.Spec.Volumes = append(ss.Spec.Template.Spec.Volumes, vol)

	containers := ss.Spec.Template.Spec.Containers
	for i := range containers {
		if containers[i].Name == redpandaContainerName {
			volMount := corev1.VolumeMount{
				Name:      datadirName,
				MountPath: dataDirectory,
			}
			containers[i].VolumeMounts = append(containers[i].VolumeMounts, volMount)
		}
	}

	if cluster.Spec.CloudStorage.Enabled && featuregates.ShadowIndex(cluster.Spec.Version) && cluster.Spec.CloudStorage.CacheStorage != nil {
		pvcArchivalDir := preparePVCResource(archivalCacheIndexAnchorName, cluster.Namespace, *cluster.Spec.CloudStorage.CacheStorage, ss.Labels)
		ss.Spec.VolumeClaimTemplates = append(ss.Spec.VolumeClaimTemplates, pvcArchivalDir)
		archivalVol := corev1.Volume{
			Name: archivalCacheIndexAnchorName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: archivalCacheIndexAnchorName,
				},
			},
		}
		ss.Spec.Template.Spec.Volumes = append(ss.Spec.Template.Spec.Volumes, archivalVol)

		for i := range containers {
			if containers[i].Name == redpandaContainerName {
				archivalVolMount := corev1.VolumeMount{
					Name:      archivalCacheIndexAnchorName,
					MountPath: archivalCacheIndexDirectory,
				}
				containers[i].VolumeMounts = append(containers[i].VolumeMounts, archivalVolMount)
			}
		}
	}
}

func (r *StatefulSetResource) rpkStatusContainer() *corev1.Container {
	if r.pandaCluster.Spec.Sidecars.RpkStatus == nil || !r.pandaCluster.Spec.Sidecars.RpkStatus.Enabled {
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
	developerMode bool,
	originalRequests redpandav1alpha1.RedpandaResourceRequirements,
) []string {
	requests := originalRequests.DeepCopy()

	requestedCores := requests.RedpandaCPU().Value()
	requestedMemory := requests.RedpandaMemory().Value()

	args := []string{}
	if developerMode {
		args = append(args,
			"--overprovisioned",
			"--kernel-page-cache=true",
			"--default-log-level=debug",
		)
	} else {
		args = append(args, "--default-log-level=info")
	}

	// When cpu is not set, all cores are used
	if requestedCores > 0 {
		args = append(args, "--smp="+strconv.FormatInt(requestedCores, 10))
	}

	// When memory is not set, all of the host memory is used minus max(1.5Gi, 7%)
	if requestedMemory > 0 {
		args = append(args,
			// Both of these flags shouldn't be set at the same time:
			// https://github.com/scylladb/seastar/issues/375
			//
			// However, this allows explicitly setting the amount of memory to
			// the required value, and the code in seastar hasn't changed in
			// years.
			//
			// The correct way to do it is to set just --reserve-memory
			// taking into account:
			// * Seastar sees the total host memory
			// * k8s has an allocatable amount of memory
			// * DefaultRequestBaseMemory reservation
			// * Memory buffer for the cgroup
			//
			// All of which doesn't feel much less fragile or intuitive.
			"--memory="+strconv.FormatInt(requestedMemory, 10),
			"--reserve-memory=0M",
		)
	}

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
	if kafkaListener := r.pandaCluster.KafkaTLSListener(); kafkaListener != nil {
		mounts = append(mounts, r.secretVolumeMountForTLS(kafkaListener.GetTLS(), redpandaCertVolName, tlsKafkaAPIDir, redpandaCAVolName, tlsKafkaAPIDirCA)...)
	}
	if adminAPI := r.pandaCluster.AdminAPITLS(); adminAPI != nil {
		mounts = append(mounts, r.secretVolumeMountForTLS(adminAPI.GetTLS(), adminAPICertVolName, tlsAdminAPIDir, adminAPICAVolName, tlsAdminAPIDirCA)...)
	}
	if pandaProxy := r.pandaCluster.PandaproxyAPITLS(); pandaProxy != nil {
		mounts = append(mounts, r.secretVolumeMountForTLS(pandaProxy.GetTLS(), pandaProxyCertVolName, tlsPandaproxyAPIDir, pandaProxyCAVolName, tlsPandaproxyAPIDirCA)...)
	}
	if schemaRegistry := r.pandaCluster.SchemaRegistryAPITLS(); schemaRegistry != nil {
		mounts = append(mounts, r.secretVolumeMountForTLS(schemaRegistry.GetTLS(), schemaRegistryCertVolName, tlsSchemaRegistryDir, schemaRegistryCAVolName, tlsSchemaRegistryDirCA)...)
	}

	return mounts
}

func (r *StatefulSetResource) secretVolumeMountForTLS(
	tlsConfig *redpandav1alpha1.TLSConfig,
	tlsVolName, tlsMoundDir, caVolName, caMountDir string,
) []corev1.VolumeMount {
	var mounts []corev1.VolumeMount
	if tlsConfig == nil || !tlsConfig.Enabled {
		return mounts
	}
	mounts = append(mounts, corev1.VolumeMount{
		Name:      tlsVolName,
		MountPath: tlsMoundDir,
	})

	if tlsConfig.RequireClientAuth {
		mounts = append(mounts, corev1.VolumeMount{
			Name:      caVolName,
			MountPath: caMountDir,
		})
	}

	return mounts
}

// The controller should have more focused feature
// oriented functions. E.g. each TLS volume could be managed by
// one function along side with root certificate, issuer, certificate and
// volumesMount in statefulset.
func (r *StatefulSetResource) secretVolumes() []corev1.Volume {
	var vols []corev1.Volume
	if kafkaListener := r.pandaCluster.KafkaTLSListener(); kafkaListener != nil {
		vols = append(vols, r.secretVolumesForTLS(kafkaListener.GetTLS(), redpandaCertVolName, r.redpandaCertSecretKey, redpandaCAVolName, r.internalClientCertSecretKey)...)
	}
	if adminAPI := r.pandaCluster.AdminAPITLS(); adminAPI != nil {
		vols = append(vols, r.secretVolumesForTLS(adminAPI.GetTLS(), adminAPICertVolName, r.adminAPINodeCertSecretKey, adminAPICAVolName, r.adminAPIClientCertSecretKey)...)
	}
	if pandaProxy := r.pandaCluster.PandaproxyAPITLS(); pandaProxy != nil {
		vols = append(vols, r.secretVolumesForTLS(pandaProxy.GetTLS(), pandaProxyCertVolName, r.pandaproxyAPINodeCertSecretKey, pandaProxyCAVolName, r.pandaproxyClientCertSecretKey)...)
	}
	if schemaRegistry := r.pandaCluster.SchemaRegistryAPITLS(); schemaRegistry != nil {
		vols = append(vols, r.secretVolumesForTLS(schemaRegistry.GetTLS(), schemaRegistryCertVolName, r.schemaRegistryAPINodeCertSecretKey, schemaRegistryCAVolName, r.schemaRegistryClientCertSecretKey)...)
	}

	return vols
}

func (r *StatefulSetResource) secretVolumesForTLS(
	tlsConfig *redpandav1alpha1.TLSConfig,
	tlsVolName string,
	tlsSecretRef types.NamespacedName,
	caVolName string,
	mutualTLSSecretRef types.NamespacedName,
) []corev1.Volume {
	var vols []corev1.Volume
	if tlsConfig == nil || !tlsConfig.Enabled {
		return vols
	}

	vols = append(vols, corev1.Volume{
		Name: tlsVolName,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: tlsSecretRef.Name,
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

	if tlsConfig.RequireClientAuth {
		vols = append(vols, corev1.Volume{
			Name: caVolName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: mutualTLSSecretRef.Name,
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
	ports := []corev1.ContainerPort{{
		Name:          AdminPortName,
		ContainerPort: int32(r.pandaCluster.AdminAPIInternal().Port),
	}}
	internalListener := r.pandaCluster.InternalListener()
	ports = append(ports, corev1.ContainerPort{
		Name:          InternalListenerName,
		ContainerPort: int32(internalListener.Port),
	})
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

func statefulSetKind() string {
	var statefulSet appsv1.StatefulSet
	return statefulSet.Kind
}

func (r *StatefulSetResource) fullConfiguratorImage() string {
	return fmt.Sprintf("%s:%s", r.configuratorSettings.ConfiguratorBaseImage, r.configuratorSettings.ConfiguratorTag)
}
