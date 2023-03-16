package console

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/cloudhut/common/rest"
	"github.com/go-logr/logr"
	"github.com/redpanda-data/console/backend/pkg/config"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	labels "github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	if ref := cm.consoleobj.Status.ConfigMapRef; ref != nil {
		cm.log.V(debugLogLevel).Info("config map ref still exist", "config map name", cm.consoleobj.Status.ConfigMapRef.Name, "config map namespace", cm.consoleobj.Status.ConfigMapRef.Namespace)
		// Check ConfigMap is present, in case it is manually deleted
		err := cm.Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, &corev1.ConfigMap{})
		if apierrors.IsNotFound(err) {
			cm.consoleobj.Status.ConfigMapRef = nil
			if updateErr := cm.Status().Update(ctx, cm.consoleobj); updateErr != nil {
				return updateErr
			}
			return &resources.RequeueError{Msg: err.Error()}
		}
		return err
	}

	// If old ConfigMaps can't be deleted for any reason, it will not continue reconciliation
	// This check is not necessary but it's an additional safeguard to make sure ConfigMaps are not more than expected
	if err := cm.isConfigMapDeleted(ctx); err != nil {
		if errors.Is(err, ErrMultipleConfigMap) {
			if deleteErr := cm.delete(ctx, ""); deleteErr != nil {
				return fmt.Errorf("cannot delete all unused ConfigMaps: %w", deleteErr)
			}
		}
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

	configuration, err := cm.generateConsoleConfig(ctx, username)
	if err != nil {
		return err
	}
	cm.log.V(debugLogLevel).Info("Creating new ConfigMap", "data", configuration)

	// Create new ConfigMap instead of updating existing so Deployment will trigger a reconcile
	immutable := true
	obj := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: cm.consoleobj.GetName() + "-",
			Namespace:    cm.consoleobj.GetNamespace(),
			Labels:       labels.ForConsole(cm.consoleobj),
		},
		Data: map[string]string{
			"config.yaml": configuration,
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

	cm.log.Info("config map ref updated", "config map name", cm.consoleobj.Status.ConfigMapRef.Name, "config map namespace", cm.consoleobj.Status.ConfigMapRef.Namespace)
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
	ctx context.Context, username string,
) (configString string, err error) {
	consoleConfig := &ConsoleConfig{
		MetricsNamespace: cm.consoleobj.Spec.MetricsPrefix,
		ServeFrontend:    cm.consoleobj.Spec.ServeFrontend,
		Server:           cm.genServer(),
		Kafka:            cm.genKafka(username),
		Enterprise:       cm.genEnterprise(),
		Redpanda:         cm.genRedpanda(),
	}

	consoleConfig.Connect, err = cm.genConnect(ctx)
	if err != nil {
		return "", err
	}

	consoleConfig.License, err = cm.genLicense(ctx)
	if err != nil {
		return "", err
	}

	// Enterprise features
	consoleConfig.Login, err = cm.genLogin(ctx)
	if err != nil {
		return "", err
	}

	consoleConfig.SecretStore = cm.genSecretStore()

	consoleConfig.Cloud = cm.genCloud()

	configuration, err := yaml.Marshal(consoleConfig)
	if err != nil {
		return "", err
	}
	return string(configuration), nil
}

func (cm *ConfigMap) genRedpanda() (r Redpanda) {
	if rp := cm.consoleobj.Spec.Redpanda; rp != nil {
		if aa := rp.AdminAPI; aa != nil && aa.Enabled {
			r.AdminAPI = cm.buildRedpandaAdmin(aa)
		}
	}
	return r
}

func (cm *ConfigMap) buildRedpandaAdmin(
	aa *redpandav1alpha1.RedpandaAdmin,
) RedpandaAdmin {
	r := RedpandaAdmin{
		Enabled: aa.Enabled,
		URLs:    cm.clusterobj.AdminAPIURLs(),
	}
	if l := cm.clusterobj.AdminAPIListener(); l != nil && l.TLS.Enabled {
		nodeSecretRef := &corev1.ObjectReference{
			Namespace: cm.clusterobj.GetNamespace(),
			Name:      fmt.Sprintf("%s-%s", cm.clusterobj.GetName(), adminAPINodeCertSuffix),
		}
		ca := &SecretTLSCa{NodeSecretRef: nodeSecretRef}
		tls := RedpandaAdminTLS{
			Enabled:    aa.Enabled,
			CaFilepath: ca.FilePath(AdminAPITLSCaFilePath),
		}
		if l.IsMutualTLSEnabled() {
			tls.CertFilepath = AdminAPITLSCertFilePath
			tls.KeyFilepath = AdminAPITLSKeyFilePath
		}
		r.TLS = tls
	}
	return r
}

func (cm *ConfigMap) genEnterprise() (e Enterprise) {
	if enterprise := cm.consoleobj.Spec.Enterprise; enterprise != nil {
		return Enterprise{
			RBAC: EnterpriseRBAC{
				Enabled:              cm.consoleobj.Spec.Enterprise.RBAC.Enabled,
				RoleBindingsFilepath: fmt.Sprintf("%s/%s", enterpriseRBACMountPath, EnterpriseRBACDataKey),
			},
		}
	}
	return e
}

func (cm *ConfigMap) genCloud() CloudConfig {
	if cm.consoleobj.Spec.Cloud == nil ||
		cm.consoleobj.Spec.Cloud.PrometheusEndpoint == nil {
		return CloudConfig{}
	}
	prometheus := cm.consoleobj.Spec.Cloud.PrometheusEndpoint
	cc := CloudConfig{
		PrometheusEndpoint: PrometheusEndpointConfig{
			Enabled: prometheus.Enabled,
			BasicAuth: struct {
				Username string "yaml:\"username\""
			}{
				Username: prometheus.BasicAuth.Username,
			},
		},
	}
	if prometheus.ResponseCacheDuration != nil {
		cc.PrometheusEndpoint.ResponseCacheDuration = prometheus.ResponseCacheDuration.Duration
	}
	if prometheus.Prometheus != nil {
		cc.PrometheusEndpoint.Prometheus = PrometheusConfig{
			Address: prometheus.Prometheus.Address,
		}
		if prometheus.Prometheus.TargetRefreshInterval != nil {
			cc.PrometheusEndpoint.Prometheus.TargetRefreshInterval = prometheus.Prometheus.TargetRefreshInterval.Duration
		}
		for _, promJob := range prometheus.Prometheus.Jobs {
			cc.PrometheusEndpoint.Prometheus.Jobs = append(cc.PrometheusEndpoint.Prometheus.Jobs, PrometheusScraperJobConfig{
				JobName:    promJob.JobName,
				KeepLabels: promJob.KeepLabels,
			})
		}
	}
	return cc
}

var (
	// gcpCredentialsFilepath is the path to the service account JSON file that is
	// used to authenticate API calls with the given service account.
	gcpCredentialsFilepath = "/secret/gcp-service-account.json" //nolint:gosec // not a secret

	// DefaultJWTSecretKey is the default key required in secret referenced by `SecretKeyRef`.
	// The secret should consist of JWT used to authenticate into google SSO.
	DefaultJWTSecretKey = "jwt"

	// EnterpriseRBACDataKey is the required key in Enterprise RBAC
	EnterpriseRBACDataKey = "rbac.yaml"

	// EnterpriseGoogleSADataKey is the required key in EnterpriseLoginGoogle SA
	EnterpriseGoogleSADataKey = "sa.json"

	// EnterpriseGoogleClientIDSecretKey is the required key in EnterpriseLoginGoogle Client ID
	EnterpriseGoogleClientIDSecretKey = "clientId"

	// EnterpriseGoogleClientSecretKey is the required key in EnterpriseLoginGoogle Client secret
	EnterpriseGoogleClientSecretKey = "clientSecret"
)

func (cm *ConfigMap) genLogin(
	ctx context.Context,
) (e EnterpriseLogin, err error) {
	if provider := cm.consoleobj.Spec.Login; provider != nil { //nolint:nestif // login config is complex
		enterpriseLogin := EnterpriseLogin{
			Enabled: provider.Enabled,
		}

		jwtSecret, err := provider.JWTSecretRef.GetSecret(ctx, cm.Client)
		if err != nil {
			return e, err
		}
		jwt, err := provider.JWTSecretRef.GetValue(jwtSecret, DefaultJWTSecretKey)
		if err != nil {
			return e, err
		}
		enterpriseLogin.JWTSecret = string(jwt)

		switch {
		case provider.RedpandaCloud != nil:
			enterpriseLogin.RedpandaCloud = &redpandav1alpha1.EnterpriseLoginRedpandaCloud{
				Enabled:        provider.RedpandaCloud.Enabled,
				Domain:         provider.RedpandaCloud.Domain,
				Audience:       provider.RedpandaCloud.Audience,
				AllowedOrigins: provider.RedpandaCloud.AllowedOrigins,
			}
		case provider.Google != nil:
			cc := redpandav1alpha1.SecretKeyRef{
				Namespace: provider.Google.ClientCredentialsRef.Namespace,
				Name:      provider.Google.ClientCredentialsRef.Name,
			}
			ccSecret, err := cc.GetSecret(ctx, cm.Client)
			if err != nil {
				return e, err
			}
			clientID, err := cc.GetValue(ccSecret, EnterpriseGoogleClientIDSecretKey)
			if err != nil {
				return e, err
			}
			clientSecret, err := cc.GetValue(ccSecret, EnterpriseGoogleClientSecretKey)
			if err != nil {
				return e, err
			}

			enterpriseLogin.Google = &EnterpriseLoginGoogle{
				Enabled:      provider.Google.Enabled,
				ClientID:     string(clientID),
				ClientSecret: string(clientSecret),
			}
			if dir := provider.Google.Directory; dir != nil {
				enterpriseLogin.Google.Directory = &EnterpriseLoginGoogleDirectory{
					ServiceAccountFilepath: fmt.Sprintf("%s/%s", enterpriseGoogleSAMountPath, EnterpriseGoogleSADataKey),
					TargetPrincipal:        provider.Google.Directory.TargetPrincipal,
				}
			}
		}
		return enterpriseLogin, nil
	}
	return e, nil
}

func (cm *ConfigMap) genSecretStore() EnterpriseSecretStore {
	if cm.consoleobj.Spec.SecretStore == nil {
		return EnterpriseSecretStore{}
	}

	ss := cm.consoleobj.Spec.SecretStore
	smGCP := EnterpriseSecretManagerGCP{}
	if ss.GCPSecretManager != nil {
		smGCP = EnterpriseSecretManagerGCP{
			Enabled:   ss.GCPSecretManager.Enabled,
			ProjectID: ss.GCPSecretManager.ProjectID,
			Labels:    ss.GCPSecretManager.Labels,
		}
		if ss.GCPSecretManager.CredentialsSecretRef != nil {
			smGCP.CredentialsFilepath = gcpCredentialsFilepath
		}
	}

	smAWS := EnterpriseSecretManagerAWS{}
	if ss.AWSSecretManager != nil {
		smAWS = EnterpriseSecretManagerAWS{
			Enabled:  ss.AWSSecretManager.Enabled,
			Region:   ss.AWSSecretManager.Region,
			KmsKeyID: ss.AWSSecretManager.KmsKeyID,
			Tags:     ss.AWSSecretManager.Tags,
		}
	}

	kc := EnterpriseSecretStoreKafkaConnect{}
	if ss.KafkaConnect != nil {
		kc = EnterpriseSecretStoreKafkaConnect{
			Enabled: ss.KafkaConnect.Enabled,
		}
		for i := 0; i < len(ss.KafkaConnect.Clusters); i++ {
			c := ss.KafkaConnect.Clusters[i]
			kc.Clusters = append(kc.Clusters, EnterpriseSecretStoreKafkaConnectCluster{
				Name:                   c.Name,
				SecretNamePrefixAppend: c.SecretNamePrefixAppend,
			})
		}
	}
	return EnterpriseSecretStore{
		Enabled:          ss.Enabled,
		SecretNamePrefix: ss.SecretNamePrefix,
		GCPSecretManager: smGCP,
		AWSSecretManager: smAWS,
		KafkaConnect:     kc,
	}
}

func (cm *ConfigMap) genLicense(ctx context.Context) (string, error) {
	if license := cm.consoleobj.Spec.LicenseRef; license != nil {
		licenseSecret, err := license.GetSecret(ctx, cm.Client)
		if err != nil {
			return "", err
		}
		licenseValue, err := license.GetValue(licenseSecret, redpandav1alpha1.DefaultLicenseSecretKey)
		if err != nil {
			return "", err
		}
		return string(licenseValue), nil
	}
	return "", nil
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

	SchemaRegistryTLSDir          = "/redpanda/schema-registry"
	SchemaRegistryTLSCaFilePath   = fmt.Sprintf("%s/%s", SchemaRegistryTLSDir, "ca.crt")
	SchemaRegistryTLSCertFilePath = fmt.Sprintf("%s/%s", SchemaRegistryTLSDir, corev1.TLSCertKey)
	SchemaRegistryTLSKeyFilePath  = fmt.Sprintf("%s/%s", SchemaRegistryTLSDir, corev1.TLSPrivateKeyKey)

	ConnectTLSDir          = "/redpanda/connect"
	ConnectTLSCaFilePath   = fmt.Sprintf("%s/%%s/%s", ConnectTLSDir, "ca.crt")
	ConnectTLSCertFilePath = fmt.Sprintf("%s/%%s/%s", ConnectTLSDir, corev1.TLSCertKey)
	ConnectTLSKeyFilePath  = fmt.Sprintf("%s/%%s/%s", ConnectTLSDir, corev1.TLSPrivateKeyKey)

	KafkaTLSDir          = "/redpanda/kafka"
	KafkaTLSCaFilePath   = fmt.Sprintf("%s/%s", KafkaTLSDir, "ca.crt")
	KafkaTLSCertFilePath = fmt.Sprintf("%s/%s", KafkaTLSDir, corev1.TLSCertKey)
	KafkaTLSKeyFilePath  = fmt.Sprintf("%s/%s", KafkaTLSDir, corev1.TLSPrivateKeyKey)

	AdminAPITLSDir          = "/redpanda/admin-api"
	AdminAPITLSCaFilePath   = fmt.Sprintf("%s/%s", AdminAPITLSDir, "ca.crt")
	AdminAPITLSCertFilePath = fmt.Sprintf("%s/%s", AdminAPITLSDir, corev1.TLSCertKey)
	AdminAPITLSKeyFilePath  = fmt.Sprintf("%s/%s", AdminAPITLSDir, corev1.TLSPrivateKeyKey)
)

// SecretTLSCa handles mounting CA cert
type SecretTLSCa struct {
	NodeSecretRef  *corev1.ObjectReference
	UsePublicCerts bool
}

// FilePath returns the CA filepath mount
func (s *SecretTLSCa) FilePath(defaultCaCert string) string {
	if s.useCaCert() {
		return defaultCaCert
	}
	return DefaultCaFilePath
}

// Volume returns mount Volume definition
func (s *SecretTLSCa) Volume(name string) *corev1.Volume {
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
func (s *SecretTLSCa) useCaCert() bool {
	return !s.UsePublicCerts && s.NodeSecretRef != nil
}

func (cm *ConfigMap) genKafka(username string) config.Kafka {
	k := config.Kafka{
		Brokers:  getBrokers(cm.clusterobj),
		ClientID: fmt.Sprintf("redpanda-console-%s-%s", cm.consoleobj.GetNamespace(), cm.consoleobj.GetName()),
	}

	schemaRegistry := config.Schema{Enabled: false}
	if y := cm.consoleobj.Spec.SchemaRegistry.Enabled; y {
		tls := config.SchemaTLS{Enabled: false}
		if yy := cm.clusterobj.IsSchemaRegistryTLSEnabled(); yy {
			ca := &SecretTLSCa{
				NodeSecretRef:  cm.clusterobj.SchemaRegistryAPITLS().TLS.NodeSecretRef,
				UsePublicCerts: UsePublicCerts,
			}
			tls = config.SchemaTLS{
				Enabled:    y,
				CaFilepath: ca.FilePath(SchemaRegistryTLSCaFilePath),
			}
			if cm.clusterobj.IsSchemaRegistryMutualTLSEnabled() {
				tls.CertFilepath = SchemaRegistryTLSCertFilePath
				tls.KeyFilepath = SchemaRegistryTLSKeyFilePath
			}
		}
		schemaRegistry = config.Schema{Enabled: y, URLs: []string{cm.clusterobj.SchemaRegistryAPIURL()}, TLS: tls}
		if cm.clusterobj.IsSchemaRegistryAuthHTTPBasic() {
			schemaRegistry.Username = username
		}

		// Default protobuf values to enable decoding in SchemaRegistry
		// REF https://app.zenhub.com/workspaces/cloud-62684e2c6635e100149514fd/issues/redpanda-data/cloud/2834
		k.Protobuf = config.Proto{
			Enabled: y,
			SchemaRegistry: config.ProtoSchemaRegistry{
				Enabled:         y,
				RefreshInterval: time.Second * 10,
			},
		}
	}
	k.Schema = schemaRegistry

	tls := config.KafkaTLS{Enabled: false}
	if l := cm.clusterobj.KafkaListener(); l.IsMutualTLSEnabled() {
		ca := &SecretTLSCa{NodeSecretRef: l.TLS.NodeSecretRef}
		tls = config.KafkaTLS{
			Enabled:      true,
			CaFilepath:   ca.FilePath(KafkaTLSCaFilePath),
			CertFilepath: KafkaTLSCertFilePath,
			KeyFilepath:  KafkaTLSKeyFilePath,
		}
	}
	k.TLS = tls

	sasl := config.KafkaSASL{Enabled: false}
	// Set defaults because Console complains SASL mechanism is not set even if SASL is disabled
	sasl.SetDefaults()
	if yes := cm.clusterobj.IsSASLOnInternalEnabled(); yes {
		sasl = config.KafkaSASL{
			Enabled:   yes,
			Username:  username,
			Mechanism: admin.ScramSha256,
		}
	}
	k.SASL = sasl

	return k
}

func getBrokers(clusterobj *redpandav1alpha1.Cluster) []string {
	if l := clusterobj.KafkaListener(); !l.External.Enabled {
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

func (cm *ConfigMap) genConnect(
	ctx context.Context,
) (conn config.Connect, err error) {
	clusters := []config.ConnectCluster{}
	for _, c := range cm.consoleobj.Spec.Connect.Clusters {
		cluster, err := cm.buildConfigCluster(ctx, c)
		if err != nil {
			return conn, err
		}
		clusters = append(clusters, *cluster)
	}

	return config.Connect{
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
) (*config.ConnectCluster, error) {
	cluster := &config.ConnectCluster{Name: c.Name, URL: c.URL}

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
	if ref := cm.consoleobj.Status.ConfigMapRef; ref != nil {
		cm.log.Info("delete unused config map reference", "config map name", ref.Name, "config map namespace", ref.Namespace)
		if err := cm.delete(ctx, ref.Name); err != nil {
			return err
		}
	}
	return nil
}

func (cm *ConfigMap) delete(ctx context.Context, skip string) error {
	cms := &corev1.ConfigMapList{}
	if err := cm.List(ctx, cms, client.MatchingLabels(labels.ForConsole(cm.consoleobj)), client.InNamespace(cm.consoleobj.GetNamespace())); err != nil {
		return err
	}
	for _, obj := range cms.Items { //nolint:gocritic // more readable, configmap list is few
		if skip != "" && skip == obj.GetName() {
			continue
		}
		obj := obj
		if err := cm.Delete(ctx, &obj); err != nil {
			return err
		}
	}
	return nil
}

var (
	// During reconciliation old ConfigMap might still be present so max expected is two
	expectedConfigMapCount = 2

	// ErrMultipleConfigMap error when attached ConfigMaps is greater than expected
	ErrMultipleConfigMap = fmt.Errorf("attached ConfigMaps is greater than %d", expectedConfigMapCount)
)

// isConfigMapDeleted checks if attached ConfigMap is more than expected
// This prevents the controller to create multiple ConfigMaps until old ones are garbage collected
func (cm *ConfigMap) isConfigMapDeleted(ctx context.Context) error {
	cms := &corev1.ConfigMapList{}
	if err := cm.List(ctx, cms, client.MatchingLabels(labels.ForConsole(cm.consoleobj)), client.InNamespace(cm.consoleobj.GetNamespace())); err != nil {
		return err
	}
	if len(cms.Items) > expectedConfigMapCount {
		return ErrMultipleConfigMap
	}
	return nil
}
