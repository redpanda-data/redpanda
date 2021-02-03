package resources

import (
	"context"
	"path/filepath"
	"strconv"

	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/labels"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	baseSuffix	= "-base"
	dataDirectory	= "/var/lib/redpanda/data"
	fsGroup		= 101

	configDir		= "/etc/redpanda"
	configuratorDir		= "/mnt/operator"
	configuratorScript	= "configurator.sh"
)

var (
	configPath		= filepath.Join(configDir, "redpanda.yaml")
	configuratorPath	= filepath.Join(configuratorDir, configuratorScript)
)

var _ Resource = &ConfigMapResource{}

type ConfigMapResource struct {
	client.Client
	scheme		*runtime.Scheme
	pandaCluster	*redpandav1alpha1.Cluster
}

func NewConfigMap(
	client client.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
) *ConfigMapResource {
	return &ConfigMapResource{
		client, scheme, pandaCluster,
	}
}

func (r *ConfigMapResource) Ensure(ctx context.Context) error {
	var cfgm corev1.ConfigMap

	err := r.Get(ctx, r.Key(), &cfgm)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	if errors.IsNotFound(err) {
		obj, err := r.Obj()
		if err != nil {
			return err
		}
		return r.Create(ctx, obj)
	}
	return nil
}

func (r *ConfigMapResource) Obj() (client.Object, error) {
	serviceAddress := r.pandaCluster.Name + "." + r.pandaCluster.Namespace + ".svc.cluster.local"
	cfg := config.Default()
	cfg.Redpanda = copyConfig(&r.pandaCluster.Spec.Configuration, &cfg.Redpanda)
	cfg.Redpanda.Id = 0
	cfg.Redpanda.AdvertisedKafkaApi.Port = cfg.Redpanda.KafkaApi.Port
	cfg.Redpanda.AdvertisedRPCAPI.Port = cfg.Redpanda.RPCServer.Port
	cfg.Redpanda.Directory = dataDirectory
	cfg.Redpanda.SeedServers = []config.SeedServer{
		{
			Host: config.SocketAddress{
				// Example address: cluster-sample-0.cluster-sample.default.svc.cluster.local
				Address:	r.pandaCluster.Name + "-0." + serviceAddress,
				Port:		cfg.Redpanda.AdvertisedRPCAPI.Port,
			},
		},
	}

	cfgBytes, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	script :=
		`set -xe;
		CONFIG=` + configPath + `;
		ORDINAL_INDEX=${HOSTNAME##*-};
		SERVICE_NAME=${HOSTNAME}.` + serviceAddress + `
		cp /mnt/operator/redpanda.yaml $CONFIG;
		rpk --config $CONFIG config set redpanda.node_id $ORDINAL_INDEX;
		if [ "$ORDINAL_INDEX" = "0" ]; then
			rpk --config $CONFIG config set redpanda.seed_servers '[]' --format yaml;
		fi;
		rpk --config $CONFIG config set redpanda.advertised_rpc_api.address $SERVICE_NAME;
		rpk --config $CONFIG config set redpanda.advertised_rpc_api.port ` + strconv.Itoa(cfg.Redpanda.AdvertisedRPCAPI.Port) + `;
		rpk --config $CONFIG config set redpanda.advertised_kafka_api.address $SERVICE_NAME;
		rpk --config $CONFIG config set redpanda.advertised_kafka_api.port ` + strconv.Itoa(cfg.Redpanda.AdvertisedKafkaApi.Port) + `;
		cat $CONFIG`

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:	r.Key().Namespace,
			Name:		r.Key().Name,
			Labels:		labels.ForCluster(r.pandaCluster),
		},
		Data: map[string]string{
			"redpanda.yaml":	string(cfgBytes),
			"configurator.sh":	script,
		},
	}
	err = controllerutil.SetControllerReference(r.pandaCluster, cm, r.scheme)
	if err != nil {
		return nil, err
	}
	return cm, nil
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

func (r *ConfigMapResource) Key() types.NamespacedName {
	return ConfigMapKey(r.pandaCluster)
}

func ConfigMapKey(pandaCluster *redpandav1alpha1.Cluster) types.NamespacedName {
	return types.NamespacedName{Name: pandaCluster.Name + baseSuffix, Namespace: pandaCluster.Namespace}
}

func (r *ConfigMapResource) Kind() string {
	var cfg corev1.ConfigMap
	return cfg.Kind
}
