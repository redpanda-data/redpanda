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
	"reflect"
	"strconv"

	"github.com/go-logr/logr"
	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/labels"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
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
	redpandaContainerName      = "redpanda"
	configuratorContainerName  = "redpanda-configurator"
	configuratorContainerImage = "vectorized/configurator"

	userID  = 101
	groupID = 101
	fsGroup = 101

	configDestinationDir = "/etc/redpanda"
	configSourceDir      = "/mnt/operator"
	configFile           = "redpanda.yaml"
)

// StatefulSetResource is part of the reconciliation of redpanda.vectorized.io CRD
// focusing on the management of redpanda cluster
type StatefulSetResource struct {
	k8sclient.Client
	scheme                      *runtime.Scheme
	pandaCluster                *redpandav1alpha1.Cluster
	serviceFQDN                 string
	serviceName                 string
	nodePortName                types.NamespacedName
	nodePortSvc                 corev1.Service
	redpandaCertSecretKey       types.NamespacedName
	internalClientCertSecretKey *types.NamespacedName
	serviceAccountName          string
	configuratorTag             string
	logger                      logr.Logger

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
	internalClientCertSecretKey *types.NamespacedName,
	serviceAccountName string,
	configuratorTag string,
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
		serviceAccountName,
		configuratorTag,
		logger.WithValues("Kind", statefulSetKind()),
		nil,
	}
}

// Ensure will manage kubernetes v1.StatefulSet for redpanda.vectorized.io custom resource
func (r *StatefulSetResource) Ensure(ctx context.Context) error {
	var sts appsv1.StatefulSet

	if r.pandaCluster.Spec.ExternalConnectivity {
		err := r.Get(ctx, r.nodePortName, &r.nodePortSvc)
		if err != nil {
			return fmt.Errorf("failed to retrieve node port service %s: %w", r.nodePortName, err)
		}

		if len(r.nodePortSvc.Spec.Ports) != 1 || r.nodePortSvc.Spec.Ports[0].NodePort == 0 {
			return fmt.Errorf("node port service %s: %w", r.nodePortName, errNodePortMissing)
		}
	}

	err := r.Get(ctx, r.Key(), &sts)
	if err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("error while fetching StatefulSet resource: %w", err)
	}

	if k8serrors.IsNotFound(err) {
		r.logger.Info(fmt.Sprintf("StatefulSet %s does not exist, going to create one", r.Key().Name))

		obj, err := r.obj()
		if err != nil {
			return fmt.Errorf("unable to construct StatefulSet object: %w", err)
		}

		if err = r.Create(ctx, obj); err != nil {
			return fmt.Errorf("unable to create StatefulSet resource: %w", err)
		}
		r.LastObservedState = obj.(*appsv1.StatefulSet)

		return nil
	}

	r.LastObservedState = &sts

	updated := update(&sts, r.pandaCluster, r.logger)
	if updated {
		if err := r.Update(ctx, &sts); err != nil {
			return fmt.Errorf("failed to update StatefulSet replicas or resources: %w", err)
		}
	}

	if err := r.updateStsImage(ctx, &sts); err != nil {
		return fmt.Errorf("failed to update StatefulSet image: %w", err)
	}

	return nil
}

// update ensures StatefulSet #replicas and resources equals cluster requirements.
func update(
	sts *appsv1.StatefulSet,
	pandaCluster *redpandav1alpha1.Cluster,
	logger logr.Logger,
) (updated bool) {
	return updateReplicasIfNeeded(sts, pandaCluster, logger) || updateResourcesIfNeeded(sts, pandaCluster, logger)
}

func updateResourcesIfNeeded(
	sts *appsv1.StatefulSet,
	pandaCluster *redpandav1alpha1.Cluster,
	logger logr.Logger,
) (updated bool) {
	if !reflect.DeepEqual(sts.Spec.Template.Spec.Containers[0].Resources, pandaCluster.Spec.Resources) {
		logger.Info(fmt.Sprintf("StatefulSet %s resources will be updated to %v", sts.Name, pandaCluster.Spec.Resources))
		sts.Spec.Template.Spec.Containers[0].Resources = pandaCluster.Spec.Resources

		return true
	}

	return false
}

func updateReplicasIfNeeded(
	sts *appsv1.StatefulSet,
	pandaCluster *redpandav1alpha1.Cluster,
	logger logr.Logger,
) (updated bool) {
	if sts.Spec.Replicas != nil && pandaCluster.Spec.Replicas != nil && *sts.Spec.Replicas != *pandaCluster.Spec.Replicas {
		logger.Info(fmt.Sprintf("StatefulSet %s has replicas set to %d but need %d. Going to update", sts.Name, *sts.Spec.Replicas, *pandaCluster.Spec.Replicas))
		sts.Spec.Replicas = pandaCluster.Spec.Replicas

		return true
	}

	return false
}

// obj returns resource managed client.Object
// nolint:funlen // The complexity of obj function will be address in the next version TODO
func (r *StatefulSetResource) obj() (k8sclient.Object, error) {
	var configMapDefaultMode int32 = 0754

	var clusterLabels = labels.ForCluster(r.pandaCluster)

	ss := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.Key().Namespace,
			Name:      r.Key().Name,
			Labels:    clusterLabels,
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
					Name:      r.pandaCluster.Name,
					Namespace: r.pandaCluster.Namespace,
					Labels:    clusterLabels.AsAPISelector().MatchLabels,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: r.getServiceAccountName(),
					SecurityContext: &corev1.PodSecurityContext{
						FSGroup: pointer.Int64Ptr(fsGroup),
					},
					Volumes: []corev1.Volume{
						{
							Name: "datadir",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "datadir",
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
									DefaultMode: &configMapDefaultMode,
								},
							},
						},
						{
							Name: "config-dir",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
					InitContainers: []corev1.Container{
						{
							Name:            configuratorContainerName,
							Image:           configuratorContainerImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Env: []corev1.EnvVar{
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
									Name:  "KAFKA_API_PORT",
									Value: strconv.Itoa(r.pandaCluster.Spec.Configuration.KafkaAPI.Port),
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
									Value: strconv.FormatBool(r.pandaCluster.Spec.ExternalConnectivity),
								},
								{
									Name:  "HOST_PORT",
									Value: r.getNodePort(),
								},
							},
							SecurityContext: &corev1.SecurityContext{
								RunAsUser:  pointer.Int64Ptr(userID),
								RunAsGroup: pointer.Int64Ptr(groupID),
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
							Name:  redpandaContainerName,
							Image: r.pandaCluster.FullImageName(),
							Args: []string{
								"redpanda",
								"start",
								"--check=false",
								"--smp 1",
								// sometimes a little bit of memory is consumed by other processes than seastar
								"--reserve-memory " + redpandav1alpha1.ReserveMemoryString,
								r.portsConfiguration(),
								"--",
								"--default-log-level=debug",
							},
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
									Name:          "admin",
									ContainerPort: int32(r.pandaCluster.Spec.Configuration.AdminAPI.Port),
								},
								{
									Name:          "rpc",
									ContainerPort: int32(r.pandaCluster.Spec.Configuration.RPCServer.Port),
								},
							}, r.getPorts()...),
							Resources: corev1.ResourceRequirements{
								Limits:   r.pandaCluster.Spec.Resources.Limits,
								Requests: r.pandaCluster.Spec.Resources.Requests,
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "datadir",
									MountPath: dataDirectory,
								},
								{
									Name:      "config-dir",
									MountPath: configDestinationDir,
								},
							},
						},
					},
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
				{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: r.pandaCluster.Namespace,
						Name:      "datadir",
						Labels:    clusterLabels,
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								"storage": resource.MustParse("100Gi"),
							},
						},
					},
				},
			},
		},
	}

	if r.pandaCluster.Spec.Configuration.TLS.KafkaAPIEnabled {
		ss.Spec.Template.Spec.Containers[0].VolumeMounts = append(ss.Spec.Template.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
			Name:      "tlscert",
			MountPath: tlsDir,
		})
		ss.Spec.Template.Spec.Volumes = append(ss.Spec.Template.Spec.Volumes, corev1.Volume{
			Name: "tlscert",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: r.redpandaCertSecretKey.Name,
				},
			},
		})
	}

	err := controllerutil.SetControllerReference(r.pandaCluster, ss, r.scheme)
	if err != nil {
		return nil, err
	}

	return ss, nil
}

func (r *StatefulSetResource) getNodePort() string {
	if r.pandaCluster.Spec.ExternalConnectivity {
		return strconv.FormatInt(int64(r.nodePortSvc.Spec.Ports[0].NodePort), 10)
	}
	return ""
}

func (r *StatefulSetResource) getServiceAccountName() string {
	if r.pandaCluster.Spec.ExternalConnectivity {
		return r.serviceAccountName
	}
	return ""
}

// Key returns namespace/name object that is used to identify object.
// For reference please visit types.NamespacedName docs in k8s.io/apimachinery
func (r *StatefulSetResource) Key() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name, Namespace: r.pandaCluster.Namespace}
}

// Kind returns v1.StatefulSet kind
func (r *StatefulSetResource) Kind() string {
	return statefulSetKind()
}

func (r *StatefulSetResource) portsConfiguration() string {
	rpcAPIPort := r.pandaCluster.Spec.Configuration.RPCServer.Port
	svcName := r.serviceName

	// In every dns name there is trailing dot to query absolute path
	// For trailing dot explanation please visit http://www.dns-sd.org/trailingdotsindomainnames.html
	return fmt.Sprintf("--advertise-rpc-addr=$(POD_NAME).%s.$(POD_NAMESPACE).svc.cluster.local.:%d", svcName, rpcAPIPort)
}

func (r *StatefulSetResource) getPorts() []corev1.ContainerPort {
	if r.pandaCluster.Spec.ExternalConnectivity &&
		len(r.nodePortSvc.Spec.Ports) > 0 {
		return []corev1.ContainerPort{
			{
				Name:          "kafka-internal",
				ContainerPort: int32(r.pandaCluster.Spec.Configuration.KafkaAPI.Port),
			},
			{
				Name: "kafka-external",
				// To distinguish external from internal clients the new listener
				// and port is exposed for Redpanda clients. The port is chosen
				// arbitrary to the KafkaAPI + 1, because user can not reach this
				// port. The routing in the Kubernetes will forward all traffic from
				// HostPort to the ContainerPort.
				ContainerPort: r.nodePortSvc.Spec.Ports[0].TargetPort.IntVal,
				// The host port is set to the service node port that doesn't have
				// any endpoints.
				HostPort: r.nodePortSvc.Spec.Ports[0].NodePort,
			},
		}
	}

	return []corev1.ContainerPort{
		{
			Name:          "kafka",
			ContainerPort: int32(r.pandaCluster.Spec.Configuration.KafkaAPI.Port),
		},
	}
}

func statefulSetKind() string {
	var statefulSet appsv1.StatefulSet
	return statefulSet.Kind
}
