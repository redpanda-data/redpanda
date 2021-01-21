// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package redpanda contains reconciliation logic for redpanda.vectorized.io CRD
package redpanda

import (
	"context"
	"reflect"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	seedSuffix	= "-seed"
	dataDirectory	= "/var/lib/redpanda/data"
	fsGroup		= 101

	debugLevel	= 2
)

// ClusterReconciler reconciles a Cluster object
type ClusterReconciler struct {
	client.Client
	Log	logr.Logger
	Scheme	*runtime.Scheme
}

//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=clusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=clusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=clusters/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;
//+kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;
//+kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// the Cluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.7.0/pkg/reconcile
// nolint:funlen // The complexity of Reconcile function will be address in the next version
func (r *ClusterReconciler) Reconcile(
	ctx context.Context, req ctrl.Request,
) (ctrl.Result, error) {
	log := r.Log.WithValues("redpandacluster", req.NamespacedName)

	var redpandaCluster redpandav1alpha1.Cluster
	if err := r.Get(ctx, req.NamespacedName, &redpandaCluster); err != nil {
		log.Error(err, "Unable to fetch RedpandaCluster")
		// we'll ignore not-found errors, since they can't be fixed by an immediate
		// requeue (we'll need to wait for a new notification), and we can get them
		// on deleted requests.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	var svc corev1.Service

	err := r.Get(ctx, types.NamespacedName{Name: redpandaCluster.Name, Namespace: redpandaCluster.Namespace}, &svc)
	if err != nil && !errors.IsNotFound(err) {
		log.V(debugLevel).Info("Unable to fetch Service resource")
		return ctrl.Result{}, err
	}

	if errors.IsNotFound(err) {
		log.V(debugLevel).Info("Creating headless service")

		if err = r.createHeadlessService(ctx, &redpandaCluster, r.Scheme); err != nil {
			log.Error(err, "Failed to create new service",
				"Service.Namespace", redpandaCluster.Namespace,
				"Service.Name", redpandaCluster.Name)

			return ctrl.Result{}, err
		}
	}

	var seedConfigMap corev1.ConfigMap

	err = r.Get(ctx, types.NamespacedName{Name: redpandaCluster.Name + seedSuffix, Namespace: redpandaCluster.Namespace}, &seedConfigMap)
	if err != nil && !errors.IsNotFound(err) {
		log.V(debugLevel).Info("Unable to fetch seed redpanda ConfigMap resource")

		return ctrl.Result{}, err
	}

	if errors.IsNotFound(err) {
		log.V(debugLevel).Info("Creating seed redpanda ConfigMap")

		if err = r.createBootstrapConfigMap(ctx, &redpandaCluster, r.Scheme); err != nil {
			log.Error(err, "Failed to create new seed redpanda ConfigMap",
				"Configmap.Namespace", redpandaCluster.Namespace,
				"Configmap.Name", redpandaCluster.Name+seedSuffix)

			return ctrl.Result{}, err
		}
	}

	var sts appsv1.StatefulSet

	err = r.Get(ctx, types.NamespacedName{Name: redpandaCluster.Name + seedSuffix, Namespace: redpandaCluster.Namespace}, &sts)
	if err != nil && !errors.IsNotFound(err) {
		log.V(debugLevel).Info("Unable to fetch StatefulSet resource")

		return ctrl.Result{}, err
	}

	if errors.IsNotFound(err) {
		log.V(debugLevel).Info("Creating bootstrap StatefulSet")

		if err = r.createBootstrapStatefulSet(ctx, &redpandaCluster, r.Scheme); err != nil {
			log.Error(err, "Failed to create new bootstrap StatefulSet",
				"Configmap.Namespace", redpandaCluster.Namespace, "StatefulSet.Name", redpandaCluster.Name+seedSuffix)

			return ctrl.Result{}, err
		}
	}

	var observedPods corev1.PodList

	err = r.List(ctx, &observedPods, &client.ListOptions{
		LabelSelector:	labels.SelectorFromSet(redpandaCluster.Labels),
		Namespace:	redpandaCluster.Namespace,
	})
	if err != nil {
		log.Error(err, "Unable to fetch PodList resource")

		return ctrl.Result{}, err
	}

	observedNodes := make([]string, len(observedPods.Items))
	// nolint:gocritic // the copies are necessary for further redpandacluster updates
	for _, item := range observedPods.Items {
		observedNodes = append(observedNodes, item.Name)
	}

	if !reflect.DeepEqual(observedNodes, redpandaCluster.Status.Nodes) {
		redpandaCluster.Status.Nodes = observedNodes
		if err := r.Status().Update(ctx, &redpandaCluster); err != nil {
			log.Error(err, "Failed to update RedpandaClusterStatus")

			return ctrl.Result{}, err
		}
	}

	if !reflect.DeepEqual(sts.Status.ReadyReplicas, redpandaCluster.Status.Replicas) {
		redpandaCluster.Status.Replicas = sts.Status.ReadyReplicas
		if err := r.Status().Update(ctx, &redpandaCluster); err != nil {
			log.Error(err, "Failed to update RedpandaClusterStatus")

			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *ClusterReconciler) createHeadlessService(
	ctx context.Context,
	clusterSpec *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:	clusterSpec.Namespace,
			Name:		clusterSpec.Name,
			Labels:		clusterSpec.Labels,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:	corev1.ClusterIPNone,
			Ports: []corev1.ServicePort{
				{
					Name:		"kafka-tcp",
					Protocol:	corev1.ProtocolTCP,
					Port:		int32(clusterSpec.Spec.Configuration.KafkaAPI.Port),
					TargetPort:	intstr.FromInt(clusterSpec.Spec.Configuration.KafkaAPI.Port),
				},
			},
			Selector:	clusterSpec.Labels,
		},
	}
	err := controllerutil.SetControllerReference(clusterSpec, svc, scheme)
	if err != nil {
		return err
	}
	return r.Create(ctx, svc)
}

func (r *ClusterReconciler) createBootstrapConfigMap(
	ctx context.Context,
	cluster *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
) error {
	cfg := config.Default()
	cfg.Redpanda = copyConfig(&cluster.Spec.Configuration, &cfg.Redpanda)
	cfg.Redpanda.Id = 0
	cfg.Redpanda.AdvertisedKafkaApi.Address = cluster.Name + seedSuffix + "-0" + "." +
		cluster.Name + seedSuffix + "." +
		cluster.Namespace +
		".svc.cluster.local"
	cfg.Redpanda.AdvertisedRPCAPI.Address = cluster.Name + seedSuffix + "-0" + "." +
		cluster.Name + seedSuffix + "." +
		cluster.Namespace +
		".svc.cluster.local"
	cfg.Redpanda.Directory = dataDirectory

	cfgBytes, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:	cluster.Namespace,
			Name:		cluster.Name + seedSuffix,
			Labels:		cluster.Labels,
		},
		Data: map[string]string{
			"redpanda.yaml": string(cfgBytes),
		},
	}
	err = controllerutil.SetControllerReference(cluster, cm, scheme)
	if err != nil {
		return err
	}

	return r.Create(ctx, cm)
}

func copyConfig(
	c *redpandav1alpha1.RedpandaConfig, cfgDefaults *config.RedpandaConfig,
) config.RedpandaConfig {
	rpcServerPort := c.RPCServer.Port
	if c.RPCServer.Port == 0 {
		rpcServerPort = cfgDefaults.RPCServer.Port
	}

	kafkaAPIPort := c.KafkaAPI.Port
	if c.KafkaAPI.Port == 0 {
		kafkaAPIPort = cfgDefaults.KafkaApi.Port
	}

	AdminAPIPort := c.AdminAPI.Port
	if c.AdminAPI.Port == 0 {
		AdminAPIPort = cfgDefaults.AdminApi.Port
	}

	return config.RedpandaConfig{
		RPCServer: config.SocketAddress{
			Address:	"0.0.0.0",
			Port:		rpcServerPort,
		},
		AdvertisedRPCAPI:	&config.SocketAddress{},
		KafkaApi: config.SocketAddress{
			Address:	"0.0.0.0",
			Port:		kafkaAPIPort,
		},
		AdvertisedKafkaApi:	&config.SocketAddress{},
		AdminApi: config.SocketAddress{
			Address:	"0.0.0.0",
			Port:		AdminAPIPort,
		},
		DeveloperMode:	c.DeveloperMode,
	}
}

// nolint:funlen // The definition needs further refinement
func (r *ClusterReconciler) createBootstrapStatefulSet(
	ctx context.Context,
	cluster *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
) error {
	memory, exist := cluster.Spec.Resources.Limits["memory"]
	if !exist {
		memory = resource.MustParse("2Gi")
	}

	ss := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:	cluster.Namespace,
			Name:		cluster.Name + seedSuffix,
			Labels:		cluster.Labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:		pointer.Int32Ptr(1),
			PodManagementPolicy:	appsv1.ParallelPodManagement,
			Selector:		metav1.SetAsLabelSelector(cluster.Labels),
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:		cluster.Name + seedSuffix,
					Namespace:	cluster.Namespace,
					Labels:		cluster.Labels,
				},
				Spec: corev1.PodSpec{
					SecurityContext: &corev1.PodSecurityContext{
						FSGroup: pointer.Int64Ptr(fsGroup),
					},
					Volumes: []corev1.Volume{
						{
							Name:	"datadir",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "datadir",
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:	"redpanda",
							Image:	cluster.Spec.Image + ":" + cluster.Spec.Version,
							Args: []string{
								"--advertise-kafka-addr $HOSTNAME." + cluster.Namespace + ".redpanda.svc.cluster.local:" + strconv.Itoa(cluster.Spec.Configuration.AdvertisedKafkaAPI.Port),
								"--advertise-rpc-addr $HOSTNAME." + cluster.Namespace + ".redpanda.svc.cluster.local:" + strconv.Itoa(cluster.Spec.Configuration.AdvertisedRPCAPI.Port),
								"--check=false",
								"--smp 1",
								"--memory " + strings.ReplaceAll(memory.String(), "Gi", "G"),
								"start",
								"--",
								"--default-log-level=info",
								"--reserve-memory 0M",
							},
							Ports: []corev1.ContainerPort{
								{
									Name:		"admin",
									ContainerPort:	int32(cluster.Spec.Configuration.AdminAPI.Port),
								},
								{
									Name:		"kafka",
									ContainerPort:	int32(cluster.Spec.Configuration.KafkaAPI.Port),
								},
								{
									Name:		"rpc",
									ContainerPort:	int32(cluster.Spec.Configuration.RPCServer.Port),
								},
							},
							Resources: corev1.ResourceRequirements{
								Limits:		cluster.Spec.Resources.Limits,
								Requests:	cluster.Spec.Resources.Requests,
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:		"datadir",
									MountPath:	dataDirectory,
								},
							},
						},
					},
					Affinity: &corev1.Affinity{
						PodAntiAffinity: &corev1.PodAntiAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
								{
									LabelSelector:	metav1.SetAsLabelSelector(cluster.Labels),
									Namespaces:	[]string{cluster.Namespace},
									TopologyKey:	corev1.LabelHostname},
							},
							PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
								{
									Weight:	100,
									PodAffinityTerm: corev1.PodAffinityTerm{
										LabelSelector:	metav1.SetAsLabelSelector(cluster.Labels),
										Namespaces:	[]string{cluster.Namespace},
										TopologyKey:	corev1.LabelHostname,
									},
								},
							},
						},
					},
					TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
						{
							MaxSkew:		1,
							TopologyKey:		corev1.LabelZoneFailureDomainStable,
							WhenUnsatisfiable:	corev1.ScheduleAnyway,
							LabelSelector:		metav1.SetAsLabelSelector(cluster.Labels),
						},
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Namespace:	cluster.Namespace,
						Name:		"datadir",
						Labels:		cluster.Labels,
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes:	[]corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
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

	err := controllerutil.SetControllerReference(cluster, ss, scheme)
	if err != nil {
		return err
	}

	return r.Create(ctx, ss)
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&redpandav1alpha1.Cluster{}).
		Owns(&appsv1.StatefulSet{}).
		Complete(r)
}
