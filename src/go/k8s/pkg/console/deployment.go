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

	containers, err := d.getContainers(ctx, ss)
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
					Containers:                    containers,
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
			Name:        d.consoleobj.GetName(),
			Namespace:   d.consoleobj.GetNamespace(),
			Labels:      labels.ForConsole(d.consoleobj),
			Annotations: map[string]string{},
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "ServiceAccount",
			APIVersion: "v1",
		},
	}

	if ss := d.consoleobj.Spec.SecretStore; ss != nil && ss.GCPSecretManager != nil && ss.GCPSecretManager.ServiceAccountNameAnnotation != "" {
		sa.ObjectMeta.Annotations["iam.gke.io/gcp-service-account"] = ss.GCPSecretManager.ServiceAccountNameAnnotation
	}

	if ss := d.consoleobj.Spec.SecretStore; ss != nil && ss.AWSSecretManager != nil && ss.AWSSecretManager.ServiceAccountRoleARNAnnotation != "" {
		sa.ObjectMeta.Annotations["eks.amazonaws.com/role-arn"] = ss.AWSSecretManager.ServiceAccountRoleARNAnnotation
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
func (d *Deployment) ensureSyncedSecrets(
	ctx context.Context,
) (map[string]string, error) {
	syncedSecrets := map[string]map[string][]byte{}

	schemaRegistrySecret, err := d.syncSchemaRegistrySecret()
	if err != nil {
		return nil, err
	}
	syncedSecrets[schemaRegistrySyncedSecretKey] = schemaRegistrySecret

	kafkaSecret, err := d.syncKafkaSecret()
	if err != nil {
		return nil, err
	}
	syncedSecrets[kafkaSyncedSecretKey] = kafkaSecret

	adminAPISecret, err := d.syncAdminAPISecret()
	if err != nil {
		return nil, err
	}
	syncedSecrets[adminAPISyncedSecretKey] = adminAPISecret

	secretNames := map[string]string{}
	for key, ss := range syncedSecrets {
		name, err := d.store.CreateSyncedSecret(ctx, d.consoleobj, ss, key, d.log)
		if err != nil {
			return nil, err
		}
		secretNames[key] = name
	}

	return secretNames, nil
}

func (d *Deployment) syncSchemaRegistrySecret() (map[string][]byte, error) {
	if !d.clusterobj.IsSchemaRegistryTLSEnabled() {
		return nil, nil
	}

	data := map[string][]byte{}
	if d.clusterobj.IsSchemaRegistryMutualTLSEnabled() {
		clientCert, exists := d.store.GetSchemaRegistryClientCert(d.clusterobj)
		if !exists {
			return nil, fmt.Errorf("get schema registry client certificate: %s", "not found") //nolint:goerr113 // no need to declare new error type
		}
		certfile := getOrEmpty(corev1.TLSCertKey, clientCert.Data)
		keyfile := getOrEmpty(corev1.TLSPrivateKeyKey, clientCert.Data)
		data[corev1.TLSCertKey] = []byte(certfile)
		data[corev1.TLSPrivateKeyKey] = []byte(keyfile)
	}

	// Only write CA cert if not using DefaultCaFilePath
	ca := &SecretTLSCa{
		NodeSecretRef:  d.clusterobj.SchemaRegistryAPITLS().TLS.NodeSecretRef,
		UsePublicCerts: UsePublicCerts,
	}
	if ca.useCaCert() {
		caCert, exists := d.store.GetSchemaRegistryNodeCert(d.clusterobj)
		if !exists {
			return nil, fmt.Errorf("get schema registry node certificate: %s", "not found") //nolint:goerr113 // no need to declare new error type
		}
		cafile := getOrEmpty("ca.crt", caCert.Data)
		data["ca.crt"] = []byte(cafile)
	}
	return data, nil
}

func (d *Deployment) syncKafkaSecret() (map[string][]byte, error) {
	listener := d.clusterobj.KafkaListener()
	if !listener.IsMutualTLSEnabled() {
		return nil, nil
	}

	data := map[string][]byte{}
	clientCert, exists := d.store.GetKafkaClientCert(d.clusterobj)
	if !exists {
		return nil, fmt.Errorf("get kafka client certificate: %s", "not found") //nolint:goerr113 // no need to declare new error type
	}
	certfile := getOrEmpty(corev1.TLSCertKey, clientCert.Data)
	keyfile := getOrEmpty(corev1.TLSPrivateKeyKey, clientCert.Data)
	data[corev1.TLSCertKey] = []byte(certfile)
	data[corev1.TLSPrivateKeyKey] = []byte(keyfile)

	// Only write CA cert if not using DefaultCaFilePath
	ca := &SecretTLSCa{NodeSecretRef: listener.TLS.NodeSecretRef}
	if ca.useCaCert() {
		caCert, exists := d.store.GetKafkaNodeCert(d.clusterobj)
		if !exists {
			return nil, fmt.Errorf("get kafka node certificate: %s", "not found") //nolint:goerr113 // no need to declare new error type
		}
		cafile := getOrEmpty("ca.crt", caCert.Data)
		data["ca.crt"] = []byte(cafile)
	}
	return data, nil
}

func (d *Deployment) syncAdminAPISecret() (map[string][]byte, error) {
	listener := d.clusterobj.AdminAPIListener()
	if !listener.TLS.Enabled {
		return nil, nil
	}

	data := map[string][]byte{}
	if listener.IsMutualTLSEnabled() {
		clientCert, exists := d.store.GetAdminAPIClientCert(d.clusterobj)
		if !exists {
			return nil, fmt.Errorf("get admin api client certificate: %s", "not found") //nolint:goerr113 // no need to declare new error type
		}
		certfile := getOrEmpty(corev1.TLSCertKey, clientCert.Data)
		keyfile := getOrEmpty(corev1.TLSPrivateKeyKey, clientCert.Data)
		data[corev1.TLSCertKey] = []byte(certfile)
		data[corev1.TLSPrivateKeyKey] = []byte(keyfile)
	}

	// Only write CA cert if not using DefaultCaFilePath
	nodeSecretRef := &corev1.ObjectReference{
		Namespace: d.clusterobj.GetNamespace(),
		Name:      fmt.Sprintf("%s-%s", d.clusterobj.GetName(), adminAPINodeCertSuffix),
	}
	ca := &SecretTLSCa{NodeSecretRef: nodeSecretRef}
	if ca.useCaCert() {
		caCert, exists := d.store.GetAdminAPINodeCert(d.clusterobj)
		if !exists {
			return nil, fmt.Errorf("get admin api node certificate: %s", "not found") //nolint:goerr113 // no need to declare new error type
		}
		cafile := getOrEmpty("ca.crt", caCert.Data)
		data["ca.crt"] = []byte(cafile)
	}
	return data, nil
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
	tlsKafkaMountName          = "tls-kafka"
	tlsAdminAPIMountName       = "tls-admin-api"

	schemaRegistryClientCertSuffix = "schema-registry-client"
	kafkaClientCertSuffix          = "operator-client"
	adminAPIClientCertSuffix       = "admin-api-client"

	schemaRegistryNodeCertSuffix = "schema-registry-node"
	kafkaNodeCertSuffix          = "kafka-node"
	adminAPINodeCertSuffix       = "admin-api-node"

	enterpriseRBACMountName     = "enterprise-rbac"
	enterpriseRBACMountPath     = "/etc/console/enterprise/rbac"
	enterpriseGoogleSAMountName = "enterprise-google-sa"
	enterpriseGoogleSAMountPath = "/etc/console/enterprise/google"

	gcpServiceAccountVolume = "gcp-service-account"

	prometheusBasicAuthPasswordEnvVar     = "CLOUD_PROMETHEUSENDPOINT_BASICAUTH_PASSWORD"
	kafkaSASLBasicAuthPasswordEnvVar      = "KAFKA_SASL_PASSWORD"           //nolint:gosec // not a secret
	schemaRegistryBasicAuthPasswordEnvVar = "KAFKA_SCHEMAREGISTRY_PASSWORD" //nolint:gosec // not a secret

	awsAccessKeyIDEnvVar     = "AWS_ACCESS_KEY_ID"
	awsSecretAccessKeyEnvVar = "AWS_SECRET_ACCESS_KEY" //nolint:gosec // not a secret
	awsSessionTokenEnvVar    = "AWS_SESSION_TOKEN"     //nolint:gosec // not a hardcoded credentials

	awsAccessKeyIDKey  = "AccessKeyId"
	awsSecretAccessKey = "SecretAccessKey"
	awsSessionTokenKey = "SessionToken"
)

func (d *Deployment) getVolumes(ss map[string]string) []corev1.Volume {
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

	if secretName, ok := ss[schemaRegistrySyncedSecretKey]; ok && d.clusterobj.IsSchemaRegistryTLSEnabled() {
		volumes = append(volumes, corev1.Volume{
			Name: tlsSchemaRegistryMountName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: secretName,
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

	if ss := d.consoleobj.Spec.SecretStore; ss != nil && ss.GCPSecretManager != nil && ss.GCPSecretManager.CredentialsSecretRef != nil {
		volumes = append(volumes, corev1.Volume{
			Name: gcpServiceAccountVolume,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: ss.GCPSecretManager.CredentialsSecretRef.Name,
				},
			},
		})
	}

	if secretName, ok := ss[kafkaSyncedSecretKey]; ok && d.clusterobj.KafkaListener().IsMutualTLSEnabled() {
		volumes = append(volumes, corev1.Volume{
			Name: tlsKafkaMountName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: secretName,
				},
			},
		})
	}

	if secretName, ok := ss[adminAPISyncedSecretKey]; ok && d.clusterobj.AdminAPIListener().TLS.Enabled {
		volumes = append(volumes, corev1.Volume{
			Name: tlsAdminAPIMountName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: secretName,
				},
			},
		})
	}

	return volumes
}

// ConsoleContainerName is the Console container name
var ConsoleContainerName = "console"

func (d *Deployment) getContainers(ctx context.Context, ss map[string]string) ([]corev1.Container, error) {
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

	if _, ok := ss[schemaRegistrySyncedSecretKey]; ok && d.clusterobj.IsSchemaRegistryTLSEnabled() {
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

	if ss := d.consoleobj.Spec.SecretStore; ss != nil && ss.GCPSecretManager != nil && ss.GCPSecretManager.CredentialsSecretRef != nil {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      gcpServiceAccountVolume,
			ReadOnly:  true,
			MountPath: gcpCredentialsFilepath,
		})
	}

	if _, ok := ss[kafkaSyncedSecretKey]; ok && d.clusterobj.KafkaListener().IsMutualTLSEnabled() {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      tlsKafkaMountName,
			ReadOnly:  true,
			MountPath: KafkaTLSDir,
		})
	}

	if _, ok := ss[adminAPISyncedSecretKey]; ok && d.clusterobj.AdminAPIListener().TLS.Enabled {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      tlsAdminAPIMountName,
			ReadOnly:  true,
			MountPath: AdminAPITLSDir,
		})
	}

	env, err := d.genEnvVars(ctx)
	if err != nil {
		return nil, err
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
			Env:          env,
		},
	}, nil
}

//nolint:funlen // the function could be refactored later
func (d *Deployment) genEnvVars(ctx context.Context) (envars []corev1.EnvVar, err error) {
	if d.clusterobj.IsSASLOnInternalEnabled() {
		envars = append(envars, corev1.EnvVar{
			Name: kafkaSASLBasicAuthPasswordEnvVar,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					Key: corev1.BasicAuthPasswordKey,
					LocalObjectReference: corev1.LocalObjectReference{
						Name: KafkaSASecretKey(d.consoleobj).Name,
					},
				},
			},
		})
	}

	if d.clusterobj.IsSchemaRegistryAuthHTTPBasic() {
		envars = append(envars, corev1.EnvVar{
			Name: schemaRegistryBasicAuthPasswordEnvVar,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					Key: corev1.BasicAuthPasswordKey,
					LocalObjectReference: corev1.LocalObjectReference{
						Name: KafkaSASecretKey(d.consoleobj).Name,
					},
				},
			},
		})
	}

	if d.consoleobj.Spec.Cloud != nil &&
		d.consoleobj.Spec.Cloud.PrometheusEndpoint != nil &&
		d.consoleobj.Spec.Cloud.PrometheusEndpoint.Enabled {
		// the webhook enforces that the secret is in the same namespace as console
		passwordRef := d.consoleobj.Spec.Cloud.PrometheusEndpoint.BasicAuth.PasswordRef
		envars = append(envars, corev1.EnvVar{
			Name: prometheusBasicAuthPasswordEnvVar,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					Key: passwordRef.Key,
					LocalObjectReference: corev1.LocalObjectReference{
						Name: passwordRef.Name,
					},
				},
			},
		})
	}

	if ss := d.consoleobj.Spec.SecretStore; ss != nil && ss.AWSSecretManager != nil && ss.AWSSecretManager.AWSCredentialsRef != nil {
		s := &corev1.Secret{}
		err = d.Get(ctx, client.ObjectKey{Namespace: d.consoleobj.Namespace, Name: ss.AWSSecretManager.AWSCredentialsRef.Name}, s)
		if err != nil {
			return envars, fmt.Errorf("getting aws crential secret: %w", err)
		}

		if _, ok := s.Data[awsAccessKeyIDKey]; !ok {
			return envars, fmt.Errorf("missing %s key in aws crential secret: %w", awsAccessKeyIDKey, err)
		}

		envars = append(envars, corev1.EnvVar{
			Name: awsAccessKeyIDEnvVar,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					Key: awsAccessKeyIDKey,
					LocalObjectReference: corev1.LocalObjectReference{
						Name: ss.AWSSecretManager.AWSCredentialsRef.Name,
					},
				},
			},
		})

		if _, ok := s.Data[awsSecretAccessKey]; !ok {
			return envars, fmt.Errorf("missing %s key in aws crential secret: %w", awsSecretAccessKey, err)
		}

		envars = append(envars, corev1.EnvVar{
			Name: awsSecretAccessKeyEnvVar,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					Key: awsSecretAccessKey,
					LocalObjectReference: corev1.LocalObjectReference{
						Name: ss.AWSSecretManager.AWSCredentialsRef.Name,
					},
				},
			},
		})

		// AWS Session Token is optional
		if _, ok := s.Data[awsSessionTokenKey]; ok {
			envars = append(envars, corev1.EnvVar{
				Name: awsSessionTokenEnvVar,
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						Key: awsSessionTokenKey,
						LocalObjectReference: corev1.LocalObjectReference{
							Name: ss.AWSSecretManager.AWSCredentialsRef.Name,
						},
					},
				},
			})
		}
	}

	return envars, nil
}
