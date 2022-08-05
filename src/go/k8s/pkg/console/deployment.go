package console

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	labels "github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// Deployment is a Console resource
type Deployment struct {
	client.Client
	scheme     *runtime.Scheme
	consoleobj *redpandav1alpha1.Console
	clusterobj *redpandav1alpha1.Cluster
	store      *Store
	log        logr.Logger
}

// NewDeployment instantiates a new Deployment
func NewDeployment(cl client.Client, scheme *runtime.Scheme, consoleobj *redpandav1alpha1.Console, clusterobj *redpandav1alpha1.Cluster, store *Store, log logr.Logger) *Deployment {
	return &Deployment{
		Client:     cl,
		scheme:     scheme,
		consoleobj: consoleobj,
		clusterobj: clusterobj,
		store:      store,
		log:        log,
	}
}

// Ensure implements Resource interface
func (d *Deployment) Ensure(ctx context.Context) error {
	sa, err := d.ensureServiceAccount(ctx)
	if err != nil {
		return err
	}

	initContainers, err := d.initContainers()
	if err != nil {
		return err
	}

	objLabels := labels.ForConsole(d.consoleobj)
	obj := &v1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      d.consoleobj.GetName(),
			Namespace: d.consoleobj.GetNamespace(),
			Labels:    objLabels,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "Deployment",
			APIVersion: "apps/v1",
		},
		Spec: v1.DeploymentSpec{
			Replicas: &d.consoleobj.Spec.Deployment.Replicas,
			Selector: objLabels.AsAPISelector(),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: objLabels,
				},
				Spec: corev1.PodSpec{
					Volumes:                       d.getVolumes(),
					InitContainers:                initContainers,
					Containers:                    d.getContainers(),
					TerminationGracePeriodSeconds: getGracePeriod(d.consoleobj.Spec.Server.ServerGracefulShutdownTimeout),
					ServiceAccountName:            sa,
				},
			},
			Strategy: v1.DeploymentStrategy{
				Type: v1.RollingUpdateDeploymentStrategyType,
				RollingUpdate: &v1.RollingUpdateDeployment{
					MaxUnavailable: &intstr.IntOrString{
						Type:   intstr.Int,
						IntVal: d.consoleobj.Spec.Deployment.MaxUnavailable,
					},
					MaxSurge: &intstr.IntOrString{
						Type:   intstr.Int,
						IntVal: d.consoleobj.Spec.Deployment.MaxSurge,
					},
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(d.consoleobj, obj, d.scheme); err != nil {
		return err
	}

	created, err := resources.CreateIfNotExists(ctx, d.Client, obj, d.log)
	if err != nil {
		return fmt.Errorf("creating Console deployment: %w", err)
	}

	if !created {
		// Update resource if not created.
		var current v1.Deployment
		err = d.Get(ctx, types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}, &current)
		if err != nil {
			return fmt.Errorf("fetching Console deployment: %w", err)
		}
		_, err = resources.Update(ctx, &current, obj, d.Client, d.log)
		if err != nil {
			return fmt.Errorf("updating Console deployment: %w", err)
		}
	}

	return nil
}

// Key implements Resource interface
func (d *Deployment) Key() types.NamespacedName {
	return types.NamespacedName{Name: d.consoleobj.GetName(), Namespace: d.consoleobj.GetNamespace()}
}

// ensureServiceAccount gets or creates Service Account
// It's best practice to use a separate Service Account per app instead of using the default
func (d *Deployment) ensureServiceAccount(ctx context.Context) (string, error) {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      d.consoleobj.GetName(),
			Namespace: d.consoleobj.GetNamespace(),
			Labels:    labels.ForConsole(d.consoleobj),
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "ServiceAccount",
			APIVersion: "v1",
		},
	}

	err := controllerutil.SetControllerReference(d.consoleobj, sa, d.scheme)
	if err != nil {
		return "", err
	}

	created, err := resources.CreateIfNotExists(ctx, d.Client, sa, d.log)
	if err != nil {
		return "", fmt.Errorf("creating Console serviceaccount: %w", err)
	}

	if !created {
		var currentSA corev1.ServiceAccount
		err = d.Get(ctx, types.NamespacedName{Name: sa.GetName(), Namespace: sa.GetNamespace()}, &currentSA)
		if err != nil {
			return "", fmt.Errorf("fetching Console serviceaccount: %w", err)
		}
		_, err = resources.Update(ctx, &currentSA, sa, d.Client, d.log)
		if err != nil {
			return "", fmt.Errorf("updating Console serviceaccount: %w", err)
		}
	}

	return sa.GetName(), nil
}

func getGracePeriod(period string) *int64 {
	duration, err := time.ParseDuration(period)
	if err != nil {
		// Defaults to 30s
		return nil
	}
	gracePeriod := duration.Nanoseconds() / time.Second.Nanoseconds()
	return &gracePeriod
}

const (
	configMountName = "config"
	configMountPath = "/etc/console/configs"

	tlsSchemaRegistryMountName = "tls-schema-registry"
	tlsConnectMountName        = "tls-connect-%s"

	schemaRegistryClientCertSuffix = "schema-registry-client"
)

func (d *Deployment) getVolumes() []corev1.Volume {
	volumes := []corev1.Volume{
		{
			Name: configMountName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: d.consoleobj.GetName(),
					},
				},
			},
		},
	}

	if d.clusterobj.IsSchemaRegistryTLSEnabled() {
		volumes = append(volumes, corev1.Volume{
			Name: tlsSchemaRegistryMountName,
			// By default VolumeSource is EmptyDir
			// But we set anyway, otherwise resource.Update() will recognize a patch
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		})
	}

	// Each Connect cluster will have own Volume because they reference different Secret
	for _, c := range d.consoleobj.Spec.Connect.Clusters {
		if c.TLS == nil || !c.TLS.Enabled {
			continue
		}
		volumes = append(volumes, corev1.Volume{
			Name: fmt.Sprintf(tlsConnectMountName, c.Name),
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: c.TLS.SecretKeyRef.Name,
				},
			},
		})
	}

	return volumes
}

func (d *Deployment) getContainers() []corev1.Container {
	volumeMounts := []corev1.VolumeMount{
		{
			Name:      configMountName,
			ReadOnly:  true,
			MountPath: configMountPath,
		},
	}

	if d.clusterobj.IsSchemaRegistryTLSEnabled() {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      tlsSchemaRegistryMountName,
			ReadOnly:  true,
			MountPath: SchemaRegistryTLSDir,
		})
	}

	for _, c := range d.consoleobj.Spec.Connect.Clusters {
		if c.TLS == nil || !c.TLS.Enabled {
			continue
		}
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      fmt.Sprintf(tlsConnectMountName, c.Name),
			ReadOnly:  true,
			MountPath: fmt.Sprintf("%s/%s", ConnectTLSDir, c.Name),
		})
	}

	return []corev1.Container{
		{
			Name:  "console",
			Image: d.consoleobj.Spec.Deployment.Image,
			Args:  []string{fmt.Sprintf("--config.filepath=%s/%s", configMountPath, "config.yaml")},
			Ports: []corev1.ContainerPort{
				{
					Name:          "http",
					ContainerPort: int32(d.consoleobj.Spec.Server.HTTPListenPort),
					Protocol:      "TCP",
				},
			},
			VolumeMounts: volumeMounts,
		},
	}
}

func (d *Deployment) initContainers() ([]corev1.Container, error) {
	// Currently only copying Schema Registry certificates
	// Nothing to do if TLS is disabled
	if !d.clusterobj.IsSchemaRegistryTLSEnabled() {
		return nil, nil
	}

	clientCert, exists := d.store.GetSchemaRegistryClientCert(d.clusterobj)
	if !exists {
		return nil, fmt.Errorf("get schema registry client certificate: %s", "not found")
	}

	caCert, exists := d.store.GetSchemaRegistryNodeCert(d.clusterobj)
	if !exists {
		return nil, fmt.Errorf("get schema registry node certificate: %s", "not found")
	}

	var (
		cafile   = getOrEmpty("ca.crt", caCert.Data)
		certfile = getOrEmpty("tls.crt", clientCert.Data)
		keyfile  = getOrEmpty("tls.key", clientCert.Data)
	)

	return []corev1.Container{
		{
			Name:  "copy-schema-registry-tls",
			Image: "busybox",
			Command: []string{
				"/bin/sh", "-c",
			},
			WorkingDir: SchemaRegistryTLSDir,
			Args: []string{
				fmt.Sprintf(
					"echo '%s' > ca.crt && echo '%s' > tls.crt && echo '%s' > tls.key",
					cafile, certfile, keyfile,
				),
			},
			VolumeMounts: []corev1.VolumeMount{
				{
					Name:      tlsSchemaRegistryMountName,
					MountPath: SchemaRegistryTLSDir,
				},
			},
		},
	}, nil
}
