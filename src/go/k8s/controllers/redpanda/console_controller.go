// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package redpanda

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/redpanda-data/console/backend/pkg/api"
	"github.com/redpanda-data/console/backend/pkg/kafka"
	"github.com/redpanda-data/console/backend/pkg/schema"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	"gopkg.in/yaml.v3"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	kafkaTLSKeyFilepath  = "/redpanda/schema-registry/key.pem"
	kafkaTLSCertFilepath = "/redpanda/schema-registry/crt.pem"
	kafkaTLSCAFilepath   = "/redpanda/schema-registry/ca.pem"

	schemaRegistryTLSKeyFilepath  = "/redpanda/schema-registry/key.pem"
	schemaRegistryTLSCertFilepath = "/redpanda/schema-registry/crt.pem"
	schemaRegistryTLSCAFilepath   = "/redpanda/schema-registry/ca.pem"

	configMount = "config"
	configPath  = "/etc/console/configs/config.yaml"
)

// ConsoleReconciler reconciles a Console object
type ConsoleReconciler struct {
	client.Client
	Log           logr.Logger
	RuntimeScheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=consoles,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=consoles/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=consoles/finalizers,verbs=update

// Reconcile schedules and configures Redpanda console
func (r *ConsoleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("console/cluster", req.NamespacedName)

	log.V(debugLogLevel).Info("Console resource reconciliation started")
	defer log.V(debugLogLevel).Info("Console resource reconciliation finished")

	var console redpandav1alpha1.Console
	if err := r.Get(ctx, req.NamespacedName, &console); err != nil {
		return ctrl.Result{}, fmt.Errorf("retrieving Console customer resource: %w", err)
	}

	var cluster redpandav1alpha1.Cluster
	if err := r.Get(ctx, types.NamespacedName{
		Namespace: console.Spec.ClusterKeyRef.Namespace,
		Name:      console.Spec.ClusterKeyRef.Name,
	}, &cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("retrieving Cluster customer resource: %w", err)
	}

	if err := r.createConfigMap(ctx, &cluster, &console, log); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting Console configuration: %w", err)
	}

	if err := r.createServiceAccount(ctx, &console, log); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting Console service account: %w", err)
	}

	if err := r.createService(ctx, &console, log); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting Console service: %w", err)
	}

	if err := r.createIngress(ctx, &cluster, &console, log); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting Console ingress: %w", err)
	}

	if err := r.createDeployment(ctx, &cluster, &console, log); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting Console deployment: %w", err)
	}

	return ctrl.Result{}, nil
}

func (r *ConsoleReconciler) createIngress(ctx context.Context, cluster *redpandav1alpha1.Cluster, console *redpandav1alpha1.Console, log logr.Logger) error {
	return nil
}

func (r *ConsoleReconciler) createService(ctx context.Context, console *redpandav1alpha1.Console, log logr.Logger) error {
	objLabels := labels.ForConsole(console)
	httpAppProtocol := "http"
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: console.Namespace,
			Name:      console.Name,
			Labels:    objLabels,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{
					Name:        "http",
					Protocol:    "TCP",
					AppProtocol: &httpAppProtocol,
					Port:        int32(console.Spec.REST.HTTPListenPort),
					TargetPort: intstr.IntOrString{
						Type:   intstr.String,
						StrVal: "http",
					},
				},
			},
			Selector: objLabels.AsAPISelector().MatchLabels,
		},
	}

	err := controllerutil.SetControllerReference(console, svc, r.RuntimeScheme)
	if err != nil {
		return err
	}

	created, err := resources.CreateIfNotExists(ctx, r, svc, log)
	if err != nil || created {
		return fmt.Errorf("creating Console service account: %w", err)
	}

	var currentSVC corev1.Service
	err = r.Get(ctx, types.NamespacedName{Name: console.Name, Namespace: console.Namespace}, &currentSVC)
	if err != nil {
		return fmt.Errorf("fetching Console service account: %w", err)
	}

	_, err = resources.Update(ctx, &currentSVC, svc, r, log)
	if err != nil {
		return fmt.Errorf("updating Console service account: %w", err)
	}
	return nil
}

func (r *ConsoleReconciler) createServiceAccount(ctx context.Context, console *redpandav1alpha1.Console, log logr.Logger) error {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: console.Namespace,
			Name:      console.Name,
			Labels:    labels.ForConsole(console),
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "ServiceAccount",
			APIVersion: "v1",
		},
	}

	err := controllerutil.SetControllerReference(console, sa, r.RuntimeScheme)
	if err != nil {
		return err
	}

	created, err := resources.CreateIfNotExists(ctx, r, sa, log)
	if err != nil || created {
		return fmt.Errorf("creating Console service account: %w", err)
	}

	var currentSA corev1.ServiceAccount
	err = r.Get(ctx, types.NamespacedName{Name: console.Name, Namespace: console.Namespace}, &currentSA)
	if err != nil {
		return fmt.Errorf("fetching Console service account: %w", err)
	}

	_, err = resources.Update(ctx, &currentSA, sa, r, log)
	if err != nil {
		return fmt.Errorf("updating Console service account: %w", err)
	}
	return nil
}

func (r *ConsoleReconciler) createConfigMap(ctx context.Context, rpCluster *redpandav1alpha1.Cluster, console *redpandav1alpha1.Console, log logr.Logger) error {
	kafkaAPI := rpCluster.InternalListener()
	if kafkaAPI == nil {
		kafkaAPI = rpCluster.ExternalListener()
	}

	var brokers []string
	for _, b := range rpCluster.Status.Nodes.Internal {
		brokers = append(brokers, fmt.Sprintf("%s:%d", b, kafkaAPI.Port))
	}

	clientID := fmt.Sprintf("redpanda-console-%s-%s", console.Name, console.Namespace)
	if console.Spec.ClientID != "" {
		clientID = console.Spec.ClientID
	}

	kafkaConfig := kafka.Config{
		Brokers:  brokers,
		ClientID: clientID,
		// TODO Redpanda operator does not support rack awareness
		RackID: "",

		Schema:      getSchema(rpCluster),
		Protobuf:    console.Spec.Protobuf,
		MessagePack: console.Spec.MessagePack,
		TLS: kafka.TLSConfig{
			Enabled:      kafkaAPI.TLS.Enabled,
			CaFilepath:   kafkaTLSCAFilepath,
			CertFilepath: kafkaTLSCertFilepath,
			KeyFilepath:  kafkaTLSKeyFilepath,
			Passphrase:   "",
		},
		SASL: kafka.SASLConfig{
			Enabled:   rpCluster.Spec.EnableSASL,
			Username:  "",
			Password:  "",
			Mechanism: kafka.SASLMechanismScramSHA256,
		},
	}

	consoleCfg := api.Config{}

	consoleCfg.SetDefaults()

	consoleCfg.Console = console.Spec.Console
	consoleCfg.Connect = console.Spec.Connect
	consoleCfg.REST = console.Spec.REST
	consoleCfg.Kafka = kafkaConfig

	if console.Spec.Logger != nil {
		consoleCfg.Logger = *console.Spec.Logger
	}

	cfg, err := yaml.Marshal(consoleCfg)
	if err != nil {
		return fmt.Errorf("marshaling Console kafka configuration: %w", err)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: console.Namespace,
			Name:      console.Name,
			Labels:    labels.ForConsole(console),
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		Data: map[string]string{
			"config.yaml": string(cfg),
		},
	}

	err = controllerutil.SetControllerReference(console, cm, r.RuntimeScheme)
	if err != nil {
		return err
	}

	created, err := resources.CreateIfNotExists(ctx, r, cm, log)
	if err != nil || created {
		return fmt.Errorf("creating Console configuration: %w", err)
	}

	var currentCM corev1.ConfigMap
	err = r.Get(ctx, types.NamespacedName{Name: console.Name, Namespace: console.Namespace}, &currentCM)
	if err != nil {
		return fmt.Errorf("fetching Console configuration: %w", err)
	}

	_, err = resources.Update(ctx, &currentCM, cm, r, log)
	if err != nil {
		return fmt.Errorf("updating Console configuration: %w", err)
	}
	return nil
}

func getSchema(cluster *redpandav1alpha1.Cluster) schema.Config {
	if cluster.Spec.Configuration.SchemaRegistry == nil {
		return schema.Config{Enabled: false}
	}

	return schema.Config{
		Enabled: true,
		URLs:    []string{cluster.Status.Nodes.SchemaRegistry.Internal},
		TLS: schema.TLSConfig{
			Enabled:               cluster.Spec.Configuration.SchemaRegistry.TLS.Enabled,
			CaFilepath:            schemaRegistryTLSCAFilepath,
			CertFilepath:          schemaRegistryTLSCertFilepath,
			KeyFilepath:           schemaRegistryTLSKeyFilepath,
			InsecureSkipTLSVerify: false,
		},

		// TODO Redpanda does not support User, Password and Bearer token in Schema registry only mTLS
		Username:    "",
		Password:    "",
		BearerToken: "",
	}
}

func (r *ConsoleReconciler) createDeployment(ctx context.Context, cluster *redpandav1alpha1.Cluster, console *redpandav1alpha1.Console, log logr.Logger) error {
	consoleLabels := labels.ForConsole(console)
	gracePeriod := console.Spec.REST.ServerGracefulShutdownTimeout.Nanoseconds() / time.Second.Nanoseconds()
	deploy := &v1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: console.Namespace,
			Name:      console.Name,
			Labels:    consoleLabels,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "Deployment",
			APIVersion: "apps/v1",
		},
		Spec: v1.DeploymentSpec{
			Replicas: console.Spec.Deployment.Replicas,
			Selector: consoleLabels.AsAPISelector(),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: configMount,
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: console.Name,
									},
								},
							},
						},
					},
					Containers:                    getContainers(cluster, console),
					TerminationGracePeriodSeconds: &gracePeriod,
					DNSPolicy:                     getDNSPolicy(console),
					NodeSelector:                  console.Spec.Deployment.NodeSelector,
					ServiceAccountName:            console.Name,
					SecurityContext:               console.Spec.Deployment.SecurityContext,
					ImagePullSecrets:              console.Spec.Deployment.ImagePullSecrets,
					Affinity:                      console.Spec.Deployment.Affinity,
					SchedulerName:                 console.Spec.Deployment.SchedulerName,
					Tolerations:                   console.Spec.Deployment.Tolerations,
					PriorityClassName:             console.Spec.Deployment.PriorityClassName,
					Priority:                      console.Spec.Deployment.Priority,
					DNSConfig:                     console.Spec.Deployment.DNSConfig,
					RuntimeClassName:              console.Spec.Deployment.RuntimeClassName,
					Overhead:                      console.Spec.Deployment.Overhead,
					TopologySpreadConstraints:     console.Spec.Deployment.TopologySpreadConstraints,
					SetHostnameAsFQDN:             console.Spec.Deployment.SetHostnameAsFQDN,
				},
			},
			Strategy:                getDeploymentStrategy(console),
			ProgressDeadlineSeconds: console.Spec.Deployment.ProgressDeadlineSeconds,
		},
	}

	err := controllerutil.SetControllerReference(console, deploy, r.RuntimeScheme)
	if err != nil {
		return err
	}

	created, err := resources.CreateIfNotExists(ctx, r, deploy, log)
	if err != nil || created {
		return fmt.Errorf("creating Console deployment: %w", err)
	}

	var currentDeploy v1.Deployment
	err = r.Get(ctx, types.NamespacedName{Name: console.Name, Namespace: console.Namespace}, &currentDeploy)
	if err != nil {
		return fmt.Errorf("fetching Console deployment: %w", err)
	}

	_, err = resources.Update(ctx, &currentDeploy, deploy, r, log)
	if err != nil {
		return fmt.Errorf("updating Console deployment: %w", err)
	}
	return nil
}

func getContainers(cluster *redpandav1alpha1.Cluster, console *redpandav1alpha1.Console) []corev1.Container {
	var args []string
	if cluster.Spec.EnableSASL {
		args = append(args, "--kafka.sasl.password=$(KAFKA_SASL_PASSWORD)")
	}

	return []corev1.Container{
		{
			Name:    "console",
			Image:   fmt.Sprintf("%s:%s", console.Spec.Image, console.Spec.Version),
			Command: []string{"/app/console"},
			Args:    append(args, fmt.Sprintf("--config.filepath=%s", configPath)),
			Ports: []corev1.ContainerPort{
				{
					Name:          "http",
					ContainerPort: int32(console.Spec.REST.HTTPListenPort),
					Protocol:      "tcp",
				},
			},
			Env:       []corev1.EnvVar{},
			Resources: console.Spec.Deployment.ResourceRequirements,
			VolumeMounts: []corev1.VolumeMount{
				{
					Name:      configMount,
					ReadOnly:  false,
					MountPath: configPath,
				},
			},
			LivenessProbe:   console.Spec.Deployment.LivenessProbe,
			ReadinessProbe:  console.Spec.Deployment.ReadinessProbe,
			ImagePullPolicy: console.Spec.Deployment.ImagePullPolicy,
		},
	}
}

func getDNSPolicy(console *redpandav1alpha1.Console) corev1.DNSPolicy {
	if console.Spec.Deployment.DNSPolicy != nil {
		return *console.Spec.Deployment.DNSPolicy
	}
	return corev1.DNSDefault
}

func getDeploymentStrategy(console *redpandav1alpha1.Console) v1.DeploymentStrategy {
	if console.Spec.Deployment.DeploymentStrategy != nil {
		return *console.Spec.Deployment.DeploymentStrategy
	}
	return v1.DeploymentStrategy{
		Type: v1.RollingUpdateDeploymentStrategyType,
		RollingUpdate: &v1.RollingUpdateDeployment{
			MaxUnavailable: nil,
			MaxSurge: &intstr.IntOrString{
				Type:   intstr.String,
				StrVal: "50%",
			},
		},
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *ConsoleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&redpandav1alpha1.Console{}).
		Owns(&redpandav1alpha1.Cluster{}).
		Complete(r)
}

func (r *ConsoleReconciler) Scheme() *runtime.Scheme {
	return r.RuntimeScheme
}
