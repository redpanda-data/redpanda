package console

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/go-logr/logr"
	"github.com/redpanda-data/console/backend/pkg/kafka"
	"github.com/redpanda-data/console/backend/pkg/schema"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	adminutils "github.com/redpanda-data/redpanda/src/go/k8s/pkg/admin"
	labels "github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
)

// ConfigMap is a Console resource
type ConfigMap struct {
	client.Client
	scheme        *runtime.Scheme
	consoleobj    *redpandav1alpha1.Console
	clusterobj    *redpandav1alpha1.Cluster
	clusterDomain string
	adminAPI      adminutils.AdminAPIClientFactory
	log           logr.Logger
}

// NewConfigMap instantiates a new ConfigMap
func NewConfigMap(cl client.Client, scheme *runtime.Scheme, consoleobj *redpandav1alpha1.Console, clusterobj *redpandav1alpha1.Cluster, clusterDomain string, adminAPI adminutils.AdminAPIClientFactory, log logr.Logger) *ConfigMap {
	return &ConfigMap{
		Client:        cl,
		scheme:        scheme,
		consoleobj:    consoleobj,
		clusterobj:    clusterobj,
		clusterDomain: clusterDomain,
		adminAPI:      adminAPI,
		log:           log,
	}
}

// Ensure implements Resource interface
func (cm *ConfigMap) Ensure(ctx context.Context) error {
	username, password, err := cm.createServiceAccount(ctx)
	if err != nil {
		return err
	}

	config, err := cm.generateConsoleConfig(username, password)
	if err != nil {
		return err
	}

	obj := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cm.consoleobj.GetName(),
			Namespace: cm.consoleobj.GetNamespace(),
			Labels:    labels.ForConsole(cm.consoleobj),
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		Data: map[string]string{
			"config.yaml": config,
		},
	}

	if err := controllerutil.SetControllerReference(cm.consoleobj, obj, cm.scheme); err != nil {
		return err
	}

	created, err := resources.CreateIfNotExists(ctx, cm.Client, obj, cm.log)
	if err != nil {
		return fmt.Errorf("creating Console configmap: %w", err)
	}

	if !created {
		// Update resource if not created.
		var current corev1.ConfigMap
		err = cm.Get(ctx, types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}, &current)
		if err != nil {
			return fmt.Errorf("fetching Console configmap: %w", err)
		}
		_, err = resources.Update(ctx, &current, obj, cm.Client, cm.log)
		if err != nil {
			return fmt.Errorf("updating Console configmap: %w", err)
		}
	}

	return nil
}

// Key implements Resource interface
func (cm *ConfigMap) Key() types.NamespacedName {
	return types.NamespacedName{Name: cm.consoleobj.GetName(), Namespace: cm.consoleobj.GetNamespace()}
}

func (cm *ConfigMap) createServiceAccount(ctx context.Context) (username, password string, err error) {
	su := resources.NewSuperUsers(cm.Client, cm.consoleobj, cm.scheme, resources.ScramConsoleUsername, resources.ConsoleSuffix, cm.log)
	if err := su.Ensure(ctx); err != nil {
		return username, password, fmt.Errorf("ensuring sasl user secret: %w", err)
	}

	// SuperUsers resource generates a username/password credentials
	var secret corev1.Secret
	err = cm.Get(ctx, su.Key(), &secret)
	if err != nil {
		return username, password, fmt.Errorf("fetching Secret (%s) from namespace (%s): %w", su.Key().Name, su.Key().Namespace, err)
	}
	username = string(secret.Data[corev1.BasicAuthUsernameKey])
	password = string(secret.Data[corev1.BasicAuthPasswordKey])

	// Create an AdminAPIClient to create the Service Account based from credentials above
	adminAPI, err := NewAdminAPI(ctx, cm.Client, cm.scheme, cm.clusterobj, cm.clusterDomain, cm.adminAPI, cm.log)
	if err != nil {
		return username, password, err
	}

	if err := adminAPI.CreateUser(ctx, username, password, admin.ScramSha256); err != nil && !strings.Contains(err.Error(), "already exists") {
		// Don't overwhelm Admin API
		return username, password, &resources.RequeueAfterError{
			RequeueAfter: resources.RequeueDuration,
			Msg:          fmt.Sprintf("could not create user: %v", err),
		}
	}
	return username, password, nil
}

// generateConsoleConfig returns the actual config passed to Console.
// This should match the fields at https://github.com/redpanda-data/console/blob/master/docs/config/console.yaml
// We are copying the fields instead of importing them because (1) they don't have json tags (2) some fields aren't ideal for K8s (e.g. TLS certs shouldn't be file paths but Secret reference)
func (cm *ConfigMap) generateConsoleConfig(username, password string) (string, error) {
	consoleConfig := &ConsoleConfig{}
	consoleConfig.SetDefaults()
	consoleConfig.Server = cm.consoleobj.Spec.Server
	consoleConfig.Kafka = cm.genKafka(username, password)

	config, err := yaml.Marshal(consoleConfig)
	if err != nil {
		return "", err
	}
	return string(config), nil
}

var (
	// Currently issuing TLS certs through LetsEncrypt
	// SchemaRegistry.TLS.NodeSecretRef is set to Secret without CaFile "ca.crt"
	// This flag overrides using NodeSecretRef to mount CaFile
	UsePublicCerts = true

	// Console is creating a NewCertPool() which will not use host default certificates when left blank
	// REF https://github.com/redpanda-data/console/blob/master/backend/pkg/schema/client.go#L60
	DefaultCaFilePath = "/etc/ssl/certs/ca-certificates.crt"

	SchemaRegistryTLSDir          = "/redpanda/schema-registry"
	SchemaRegistryTLSCaFilePath   = fmt.Sprintf("%s/%s", SchemaRegistryTLSDir, "ca.crt")
	SchemaRegistryTLSCertFilePath = fmt.Sprintf("%s/%s", SchemaRegistryTLSDir, "tls.crt")
	SchemaRegistryTLSKeyFilePath  = fmt.Sprintf("%s/%s", SchemaRegistryTLSDir, "tls.key")
)

// SchemaRegistryTLSCa handles mounting CA cert
type SchemaRegistryTLSCa struct {
	NodeSecretRef *corev1.ObjectReference
}

// FilePath returns the CA filepath mount
func (s *SchemaRegistryTLSCa) FilePath() string {
	if !UsePublicCerts && s.NodeSecretRef != nil {
		return SchemaRegistryTLSCaFilePath
	}
	return DefaultCaFilePath
}

// Volume returns mount Volume definition
func (s *SchemaRegistryTLSCa) Volume(name string) *corev1.Volume {
	if !UsePublicCerts && s.NodeSecretRef != nil {
		return &corev1.Volume{
			Name: name,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: s.NodeSecretRef.Name,
					Items:      []corev1.KeyToPath{{Key: "ca.crt", Path: "."}},
				},
			},
		}
	}
	return nil
}

func (cm *ConfigMap) genKafka(username, password string) kafka.Config {
	k := kafka.Config{
		Brokers:  getBrokers(cm.clusterobj),
		ClientID: fmt.Sprintf("redpanda-console-%s-%s", cm.consoleobj.GetNamespace(), cm.consoleobj.GetName()),
	}

	schemaRegistry := schema.Config{Enabled: false}
	if yes := cm.consoleobj.Spec.Schema.Enabled; yes {
		tls := schema.TLSConfig{Enabled: false}
		if yes := cm.clusterobj.IsSchemaRegistryTLSEnabled(); yes {
			ca := &SchemaRegistryTLSCa{
				// SchemaRegistryAPITLS cannot be nil
				cm.clusterobj.SchemaRegistryAPITLS().TLS.NodeSecretRef,
			}
			tls = schema.TLSConfig{
				Enabled:      yes,
				CaFilepath:   ca.FilePath(),
				CertFilepath: SchemaRegistryTLSCertFilePath,
				KeyFilepath:  SchemaRegistryTLSKeyFilePath,
			}
		}
		schemaRegistry = schema.Config{Enabled: yes, URLs: []string{cm.clusterobj.SchemaRegistryAPIURL()}, TLS: tls}
	}
	k.Schema = schemaRegistry

	sasl := kafka.SASLConfig{Enabled: false}
	// Set defaults because Console complains SASL mechanism is not set even if SASL is disabled
	sasl.SetDefaults()
	if yes := cm.clusterobj.Spec.EnableSASL; yes {
		sasl = kafka.SASLConfig{
			Enabled:   yes,
			Username:  username,
			Password:  password,
			Mechanism: admin.ScramSha256,
		}
	}
	k.SASL = sasl

	return k
}

func getBrokers(clusterobj *redpandav1alpha1.Cluster) []string {
	if l := clusterobj.InternalListener(); l != nil {
		brokers := []string{}
		for _, host := range clusterobj.Status.Nodes.Internal {
			port := fmt.Sprintf("%d", l.Port)
			brokers = append(brokers, net.JoinHostPort(host, port))
		}
		return brokers
	}
	// External hosts already have ports in them
	return clusterobj.Status.Nodes.External
}
