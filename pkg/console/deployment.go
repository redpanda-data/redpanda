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
func NewDeployment(
	cl client.Client,
	scheme *runtime.Scheme,
	consoleobj *redpandav1alpha1.Console,
	clusterobj *redpandav1alpha1.Cluster,
	store *Store,
	log logr.Logger,
) *Deployment {
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

	ss, err := d.ensureSyncedSecrets(ctx)
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
					Volumes:                       d.getVolumes(ss),
					Containers:                    d.getContainers(ss),
					TerminationGracePeriodSeconds: getGracePeriod(d.consoleobj.Spec.Server.ServerGracefulShutdownTimeout.Duration),
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

	err = controllerutil.SetControllerReference(d.consoleobj, obj, d.scheme)
	if err != nil {
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

// ensureSyncedSecrets ensures that Secrets required by Deployment are available
// These Secrets are synced across different namespace via the Store
func (d *Deployment) ensureSyncedSecrets(ctx context.Context) (string, error) {
	// Currently only copying Schema Registry certificates
	// Nothing to do if TLS is disabled
	if !d.clusterobj.IsSchemaRegistryTLSEnabled() {
		return "", nil
	}

	// Write the synced certs to secret
	data := map[string][]byte{}

	if d.clusterobj.IsSchemaRegistryMutualTLSEnabled() {
		clientCert, exists := d.store.GetSchemaRegistryClientCert(d.clusterobj)
		if !exists {
			return "", fmt.Errorf("get schema registry client certificate: %s", "not found") //nolint:goerr113 // no need to declare new error type
		}
		certfile := getOrEmpty("tls.crt", clientCert.Data)
		keyfile := getOrEmpty("tls.key", clientCert.Data)
		data["tls.crt"] = []byte(certfile)
		data["tls.key"] = []byte(keyfile)
	}

	// Only write CA cert if not using DefaultCaFilePath
	ca := &SchemaRegistryTLSCa{d.clusterobj.SchemaRegistryAPITLS().TLS.NodeSecretRef}
	if ca.useCaCert() {
		caCert, exists := d.store.GetSchemaRegistryNodeCert(d.clusterobj)
		if !exists {
			return "", fmt.Errorf("get schema registry node certificate: %s", "not found") //nolint:goerr113 // no need to declare new error type
		}
		cafile := getOrEmpty("ca.crt", caCert.Data)
		data["ca.crt"] = []byte(cafile)
	}

	// Nothing to do if synced certs is empty
	if len(data) == 0 {
		return "", nil
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s", d.consoleobj.GetName(), resources.SchemaRegistrySuffix),
			Namespace: d.consoleobj.GetNamespace(),
			Labels:    labels.ForConsole(d.consoleobj),
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: "v1",
		},
		Data: data,
	}

	err := controllerutil.SetControllerReference(d.consoleobj, secret, d.scheme)
	if err != nil {
		return "", err
	}

	created, err := resources.CreateIfNotExists(ctx, d.Client, secret, d.log)
	if err != nil {
		return "", fmt.Errorf("creating Console synced secret: %w", err)
	}

	if !created {
		var current corev1.Secret
		err = d.Get(ctx, types.NamespacedName{Name: secret.GetName(), Namespace: secret.GetNamespace()}, &current)
		if err != nil {
			return "", fmt.Errorf("fetching Console synced secret: %w", err)
		}
		_, err = resources.Update(ctx, &current, secret, d.Client, d.log)
		if err != nil {
			return "", fmt.Errorf("updating Console synced secret: %w", err)
		}
	}

	return secret.GetName(), nil
}

func getGracePeriod(period time.Duration) *int64 {
	gracePeriod := period.Nanoseconds() / time.Second.Nanoseconds()
	return &gracePeriod
}

const (
	configMountName = "config"
	configMountPath = "/etc/console/configs"

	tlsSchemaRegistryMountName = "tls-schema-registry"
	tlsConnectMountName        = "tls-connect-%s"

	schemaRegistryClientCertSuffix = "schema-registry-client"

	enterpriseRBACMountName     = "enterprise-rbac"
	enterpriseRBACMountPath     = "/etc/console/enterprise/rbac"
	enterpriseGoogleSAMountName = "enterprise-google-sa"
	enterpriseGoogleSAMountPath = "/etc/console/enterprise/google"

	prometheusBasicAuthPassowrdEnvVar = "CLOUD_PROMETHEUSENDPOINT_BASICAUTH_PASSWORD"
)

func (d *Deployment) getVolumes(ss string) []corev1.Volume {
	volumes := []corev1.Volume{
		{
			Name: configMountName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: d.consoleobj.Status.ConfigMapRef.Name,
					},
				},
			},
		},
	}

	if d.clusterobj.IsSchemaRegistryTLSEnabled() && ss != "" {
		volumes = append(volumes, corev1.Volume{
			Name: tlsSchemaRegistryMountName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: ss,
				},
			},
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

	if enterprise := d.consoleobj.Spec.Enterprise; enterprise != nil {
		volumes = append(volumes, corev1.Volume{
			Name: enterpriseRBACMountName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: enterprise.RBAC.RoleBindingsRef,
				},
			},
		})
	}

	if login := d.consoleobj.Spec.Login; login != nil && login.Google != nil && login.Google.Directory != nil {
		volumes = append(volumes, corev1.Volume{
			Name: enterpriseGoogleSAMountName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: login.Google.Directory.ServiceAccountRef,
				},
			},
		})
	}

	return volumes
}

// ConsoleContainerName is the Console container name
var ConsoleContainerName = "console"

func (d *Deployment) getContainers(ss string) []corev1.Container {
	volumeMounts := []corev1.VolumeMount{
		{
			Name:      configMountName,
			ReadOnly:  true,
			MountPath: configMountPath,
		},
	}

	if enterprise := d.consoleobj.Spec.Enterprise; enterprise != nil {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      enterpriseRBACMountName,
			ReadOnly:  true,
			MountPath: enterpriseRBACMountPath,
		})
	}

	if d.clusterobj.IsSchemaRegistryTLSEnabled() && ss != "" {
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

	if login := d.consoleobj.Spec.Login; login != nil && login.Google != nil && login.Google.Directory != nil {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      enterpriseGoogleSAMountName,
			ReadOnly:  true,
			MountPath: enterpriseGoogleSAMountPath,
		})
	}

	return []corev1.Container{
		{
			Name:  ConsoleContainerName,
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
			Env:          d.genEnvVars(),
		},
	}
}

func (d *Deployment) genEnvVars() []corev1.EnvVar {
	if d.consoleobj.Spec.Cloud == nil ||
		d.consoleobj.Spec.Cloud.PrometheusEndpoint == nil ||
		!d.consoleobj.Spec.Cloud.PrometheusEndpoint.Enabled {
		return []corev1.EnvVar{}
	}
	// the webhook enforces that the secret is in the same namespace as console
	passwordRef := d.consoleobj.Spec.Cloud.PrometheusEndpoint.BasicAuth.PasswordRef
	return []corev1.EnvVar{
		{
			Name: prometheusBasicAuthPassowrdEnvVar,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					Key: passwordRef.Key,
					LocalObjectReference: corev1.LocalObjectReference{
						Name: passwordRef.Name,
					},
				},
			},
		},
	}
}
