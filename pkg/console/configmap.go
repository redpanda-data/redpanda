package console

import (
	"context"
	"fmt"
	"net"

	"github.com/cloudhut/common/rest"
	"github.com/go-logr/logr"
	"github.com/redpanda-data/console/backend/pkg/connect"
	"github.com/redpanda-data/console/backend/pkg/kafka"
	"github.com/redpanda-data/console/backend/pkg/schema"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	labels "github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// ConfigMap is a Console resource
type ConfigMap struct {
	client.Client
	scheme     *runtime.Scheme
	consoleobj *redpandav1alpha1.Console
	clusterobj *redpandav1alpha1.Cluster
	log        logr.Logger
}

// NewConfigMap instantiates a new ConfigMap
func NewConfigMap(
	cl client.Client,
	scheme *runtime.Scheme,
	consoleobj *redpandav1alpha1.Console,
	clusterobj *redpandav1alpha1.Cluster,
	log logr.Logger,
) *ConfigMap {
	return &ConfigMap{
		Client:     cl,
		scheme:     scheme,
		consoleobj: consoleobj,
		clusterobj: clusterobj,
		log:        log,
	}
}

// Ensure implements Resource interface
func (cm *ConfigMap) Ensure(ctx context.Context) error {
	if cm.consoleobj.Status.ConfigMapRef != nil {
		return nil
	}

	// If old ConfigMaps can't be deleted for any reason, it will not continue reconciliation
	// This check is not necessary but it's an additional safeguard to make sure ConfigMaps are not more than expected
	if err := cm.isConfigMapDeleted(ctx); err != nil {
		return fmt.Errorf("old ConfigMaps are not deleted: %w", err)
	}

	// Create new ConfigMap
	// If reconciliation fails, a new ConfigMap will be created again
	// But unused ConfigMaps should be deleted at the beginning of reconciliation via DeleteUnused()

	secret := corev1.Secret{}
	if err := cm.Get(ctx, KafkaSASecretKey(cm.consoleobj), &secret); err != nil {
		return err
	}
	username := string(secret.Data[corev1.BasicAuthUsernameKey])
	password := string(secret.Data[corev1.BasicAuthPasswordKey])

	config, err := cm.generateConsoleConfig(ctx, username, password)
	if err != nil {
		return err
	}
	cm.log.V(debugLogLevel).Info("Creating new ConfigMap", "data", config)

	// Create new ConfigMap instead of updating existing so Deployment will trigger a reconcile
	immutable := true
	obj := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: cm.consoleobj.GetName() + "-",
			Namespace:    cm.consoleobj.GetNamespace(),
			Labels:       labels.ForConsole(cm.consoleobj),
		},
		Data: map[string]string{
			"config.yaml": config,
		},
		Immutable: &immutable,
	}

	if err := controllerutil.SetControllerReference(cm.consoleobj, obj, cm.scheme); err != nil {
		return err
	}
	if err := cm.Create(ctx, obj); err != nil {
		return fmt.Errorf("creating Console configmap: %w", err)
	}

	// This will get updated in the controller main reconcile function
	// Other Resources may set Console status if they are also watching GenerationMatchesObserved()
	cm.consoleobj.Status.ConfigMapRef = &corev1.ObjectReference{Namespace: obj.GetNamespace(), Name: obj.GetName()}

	return nil
}

// Key implements Resource interface
func (cm *ConfigMap) Key() types.NamespacedName {
	return types.NamespacedName{Name: cm.consoleobj.GetName(), Namespace: cm.consoleobj.GetNamespace()}
}

// generateConsoleConfig returns the actual config passed to Console.
// This should match the fields at https://github.com/redpanda-data/console/blob/master/docs/config/console.yaml
// We are copying the fields instead of importing them because (1) they don't have json tags (2) some fields aren't ideal for K8s (e.g. TLS certs shouldn't be file paths but Secret reference)
func (cm *ConfigMap) generateConsoleConfig(
	ctx context.Context, username, password string,
) (string, error) {
	consoleConfig := &ConsoleConfig{
		MetricsNamespace: cm.consoleobj.Spec.MetricsPrefix,
		ServeFrontend:    cm.consoleobj.Spec.ServeFrontend,
		Server:           cm.genServer(),
		Kafka:            cm.genKafka(username, password),
	}

	connectConfig, err := cm.genConnect(ctx)
	if err != nil {
		return "", err
	}
	consoleConfig.Connect = *connectConfig

	config, err := yaml.Marshal(consoleConfig)
	if err != nil {
		return "", err
	}
	return string(config), nil
}

func (cm *ConfigMap) genServer() rest.Config {
	server := cm.consoleobj.Spec.Server
	return rest.Config{
		ServerGracefulShutdownTimeout:   server.ServerGracefulShutdownTimeout.Duration,
		HTTPListenAddress:               server.HTTPListenAddress,
		HTTPListenPort:                  server.HTTPListenPort,
		HTTPServerReadTimeout:           server.HTTPServerReadTimeout.Duration,
		HTTPServerWriteTimeout:          server.HTTPServerWriteTimeout.Duration,
		HTTPServerIdleTimeout:           server.HTTPServerIdleTimeout.Duration,
		CompressionLevel:                server.CompressionLevel,
		BasePath:                        server.BasePath,
		SetBasePathFromXForwardedPrefix: server.SetBasePathFromXForwardedPrefix,
		StripPrefix:                     server.StripPrefix,
	}
}

var (
	// UsePublicCerts defines if certificate is signed publicly
	// Currently issuing TLS certs through LetsEncrypt
	// SchemaRegistry.TLS.NodeSecretRef is set to Secret without CaFile "ca.crt"
	// This flag overrides using NodeSecretRef to mount CaFile
	UsePublicCerts = true

	// DefaultCaFilePath defines the default CA filepath in the host
	// Console is creating a NewCertPool() which will not use host default certificates when left blank
	// REF https://github.com/redpanda-data/console/blob/master/backend/pkg/schema/client.go#L60
	DefaultCaFilePath = "/etc/ssl/certs/ca-certificates.crt"

	SchemaRegistryTLSDir          = "/redpanda/schema-registry" // nolint:revive // readable enough
	SchemaRegistryTLSCaFilePath   = fmt.Sprintf("%s/%s", SchemaRegistryTLSDir, "ca.crt")
	SchemaRegistryTLSCertFilePath = fmt.Sprintf("%s/%s", SchemaRegistryTLSDir, "tls.crt")
	SchemaRegistryTLSKeyFilePath  = fmt.Sprintf("%s/%s", SchemaRegistryTLSDir, "tls.key")

	ConnectTLSDir          = "/redpanda/connect"
	ConnectTLSCaFilePath   = fmt.Sprintf("%s/%%s/%s", ConnectTLSDir, "ca.crt")
	ConnectTLSCertFilePath = fmt.Sprintf("%s/%%s/%s", ConnectTLSDir, "tls.crt")
	ConnectTLSKeyFilePath  = fmt.Sprintf("%s/%%s/%s", ConnectTLSDir, "tls.key")
)

// SchemaRegistryTLSCa handles mounting CA cert
type SchemaRegistryTLSCa struct {
	NodeSecretRef *corev1.ObjectReference
}

// FilePath returns the CA filepath mount
func (s *SchemaRegistryTLSCa) FilePath() string {
	if s.useCaCert() {
		return SchemaRegistryTLSCaFilePath
	}
	return DefaultCaFilePath
}

// Volume returns mount Volume definition
func (s *SchemaRegistryTLSCa) Volume(name string) *corev1.Volume {
	if s.useCaCert() {
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

// useCaCert checks if the CA certificate referenced by NodeSecretRef should be used
func (s *SchemaRegistryTLSCa) useCaCert() bool {
	return !UsePublicCerts && s.NodeSecretRef != nil
}

func (cm *ConfigMap) genKafka(username, password string) kafka.Config {
	k := kafka.Config{
		Brokers:  getBrokers(cm.clusterobj),
		ClientID: fmt.Sprintf("redpanda-console-%s-%s", cm.consoleobj.GetNamespace(), cm.consoleobj.GetName()),
	}

	schemaRegistry := schema.Config{Enabled: false}
	if y := cm.consoleobj.Spec.SchemaRegistry.Enabled; y {
		tls := schema.TLSConfig{Enabled: false}
		if yy := cm.clusterobj.IsSchemaRegistryTLSEnabled(); yy {
			ca := &SchemaRegistryTLSCa{
				// SchemaRegistryAPITLS cannot be nil
				cm.clusterobj.SchemaRegistryAPITLS().TLS.NodeSecretRef,
			}
			tls = schema.TLSConfig{
				Enabled:    y,
				CaFilepath: ca.FilePath(),
			}
			if cm.clusterobj.IsSchemaRegistryMutualTLSEnabled() {
				tls.CertFilepath = SchemaRegistryTLSCertFilePath
				tls.KeyFilepath = SchemaRegistryTLSKeyFilePath
			}
		}
		schemaRegistry = schema.Config{Enabled: y, URLs: []string{cm.clusterobj.SchemaRegistryAPIURL()}, TLS: tls}
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

func (cm *ConfigMap) genConnect(ctx context.Context) (*connect.Config, error) {
	clusters := []connect.ConfigCluster{}
	for _, c := range cm.consoleobj.Spec.Connect.Clusters {
		cluster, err := cm.buildConfigCluster(ctx, c)
		if err != nil {
			return nil, err
		}
		clusters = append(clusters, *cluster)
	}

	return &connect.Config{
		Enabled:        cm.consoleobj.Spec.Connect.Enabled,
		Clusters:       clusters,
		ConnectTimeout: cm.consoleobj.Spec.Connect.ConnectTimeout.Duration,
		ReadTimeout:    cm.consoleobj.Spec.Connect.ReadTimeout.Duration,
		RequestTimeout: cm.consoleobj.Spec.Connect.RequestTimeout.Duration,
	}, nil
}

func getOrEmpty(key string, data map[string][]byte) string {
	if val, ok := data[key]; ok {
		return string(val)
	}
	return ""
}

func (cm *ConfigMap) buildConfigCluster(
	ctx context.Context, c redpandav1alpha1.ConnectCluster,
) (*connect.ConfigCluster, error) {
	cluster := &connect.ConfigCluster{Name: c.Name, URL: c.URL}

	if c.BasicAuthRef != nil {
		ref := corev1.Secret{}
		if err := cm.Get(ctx, types.NamespacedName{Namespace: c.BasicAuthRef.Namespace, Name: c.BasicAuthRef.Name}, &ref); err != nil {
			return nil, err
		}
		// Don't stop reconciliation if key not found, fail in Console instead
		// User shouldn't have knowledge about operator and should just see Console logs
		cluster.Username = getOrEmpty("username", ref.Data)
		cluster.Password = getOrEmpty("password", ref.Data)
	}

	if c.TokenRef != nil {
		ref := corev1.Secret{}
		if err := cm.Get(ctx, types.NamespacedName{Namespace: c.TokenRef.Namespace, Name: c.TokenRef.Name}, &ref); err != nil {
			return nil, err
		}
		cluster.Token = getOrEmpty("token", ref.Data)
	}

	if c.TLS != nil {
		cluster.TLS.Enabled = c.TLS.Enabled
		cluster.TLS.InsecureSkipTLSVerify = c.TLS.InsecureSkipTLSVerify
		if c.TLS.SecretKeyRef != nil {
			cluster.TLS.CaFilepath = fmt.Sprintf(ConnectTLSCaFilePath, c.Name)
			cluster.TLS.CertFilepath = fmt.Sprintf(ConnectTLSCertFilePath, c.Name)
			cluster.TLS.KeyFilepath = fmt.Sprintf(ConnectTLSKeyFilePath, c.Name)
		}
	}

	return cluster, nil
}

// DeleteUnused makes sure that old unreferenced ConfigMaps are deleted
// ConfigMaps are recreated upon Console update, old ones should be cleaned up
func (cm *ConfigMap) DeleteUnused(ctx context.Context) error {
	cms := &corev1.ConfigMapList{}
	if err := cm.List(ctx, cms, client.MatchingLabels(labels.ForConsole(cm.consoleobj)), client.InNamespace(cm.consoleobj.GetNamespace())); err != nil {
		return err
	}
	for _, obj := range cms.Items { // nolint:gocritic // more readable, configmap list is few
		if ref := cm.consoleobj.Status.ConfigMapRef; ref != nil && ref.Name != obj.GetName() {
			obj := obj
			if err := cm.Delete(ctx, &obj); err != nil {
				return err
			}
		}
	}
	return nil
}

// isConfigMapDeleted checks if attached ConfigMap is more than expected
// This prevents the controller to create multiple ConfigMaps until old ones are garbage collected
func (cm *ConfigMap) isConfigMapDeleted(ctx context.Context) error {
	cms := &corev1.ConfigMapList{}
	if err := cm.List(ctx, cms, client.MatchingLabels(labels.ForConsole(cm.consoleobj)), client.InNamespace(cm.consoleobj.GetNamespace())); err != nil {
		return err
	}
	// During reconciliation old ConfigMap might still be present so max expected is two
	count := 2
	if len(cms.Items) > count {
		return fmt.Errorf("attached ConfigMaps is greater than %d", count) // nolint:goerr113 // no need to declare new error type
	}
	return nil
}
