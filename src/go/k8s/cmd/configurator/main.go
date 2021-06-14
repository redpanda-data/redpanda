// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/spf13/afero"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	hostNameEnvVar                      = "HOSTNAME"
	svcFQDNEnvVar                       = "SERVICE_FQDN"
	configSourceDirEnvVar               = "CONFIG_SOURCE_DIR"
	configDestinationEnvVar             = "CONFIG_DESTINATION"
	redpandaRPCPortEnvVar               = "REDPANDA_RPC_PORT"
	nodeNameEnvVar                      = "NODE_NAME"
	externalConnectivityEnvVar          = "EXTERNAL_CONNECTIVITY"
	externalConnectivitySubDomainEnvVar = "EXTERNAL_CONNECTIVITY_SUBDOMAIN"
	hostPortEnvVar                      = "HOST_PORT"
	proxyHostPortEnvVar                 = "PROXY_HOST_PORT"
	ordinalPortPerBroker                = "ORDINAL_PORT_PER_BROKER"
	ordinalBrokerHostname               = "ORDINAL_BROKER_HOSTNAME"
	basePort                            = "BASE_PORT"
)

type brokerID int

type configuratorConfig struct {
	hostName              string
	svcFQDN               string
	configSourceDir       string
	configDestination     string
	nodeName              string
	subdomain             string
	externalConnectivity  bool
	redpandaRPCPort       int
	hostPort              int
	proxyHostPort         int
	ordinalPortPerBroker  bool
	ordinalBrokerHostname bool
	basePort              int
}

func (c *configuratorConfig) String() string {
	return fmt.Sprintf("The configuration:\n"+
		"hostName: %s\n"+
		"svcFQDN: %s\n"+
		"configSourceDir: %s\n"+
		"configDestination: %s\n"+
		"nodeName: %s\n"+
		"externalConnectivity: %t\n"+
		"externalConnectivitySubdomain: %s\n"+
		"redpandaRPCPort: %d\n"+
		"hostPort: %d\n"+
		"proxyHostPort: %d\n"+
		"ordinalBrokerHostname: %t\n"+
		"basePort: %d\n",
		c.hostName,
		c.svcFQDN,
		c.configSourceDir,
		c.configDestination,
		c.nodeName,
		c.externalConnectivity,
		c.subdomain,
		c.redpandaRPCPort,
		c.hostPort,
		c.proxyHostPort,
		c.ordinalBrokerHostname,
		c.basePort,
	)
}

var errorMissingEnvironmentVariable = errors.New("missing environment variable")

func main() {
	log.Print("The redpanda configurator is starting")

	c, err := checkEnvVars()
	if err != nil {
		log.Fatalf("%s", fmt.Errorf("unable to get the environment variables: %w", err))
	}

	log.Print(c.String())

	fs := afero.NewOsFs()
	mgr := config.NewManager(fs)
	cfg, err := mgr.Read(path.Join(c.configSourceDir, "redpanda.yaml"))
	if err != nil {
		log.Fatalf("%s", fmt.Errorf("unable to read the redpanda configuration file: %w", err))
	}

	kafkaAPIPort, err := getInternalKafkaAPIPort(cfg)
	if err != nil {
		log.Fatal(err)
	}
	hostIndex, err := hostIndex(c.hostName)
	if err != nil {
		log.Fatalf("%s", fmt.Errorf("unable to extract host index: %w", err))
	}

	log.Printf("Host index calculated %d", hostIndex)

	err = registerAdvertisedKafkaAPI(&c, cfg, hostIndex, kafkaAPIPort)
	if err != nil {
		log.Fatalf("%s", fmt.Errorf("unable to register advertised Kafka API: %w", err))
	}

	if cfg.Pandaproxy != nil && len(cfg.Pandaproxy.PandaproxyAPI) > 0 {
		proxyAPIPort := getInternalProxyAPIPort(cfg)
		err = registerAdvertisedPandaproxyAPI(&c, cfg, hostIndex, proxyAPIPort)
		if err != nil {
			log.Fatalf("%s", fmt.Errorf("unable to register advertised Pandaproxy API: %w", err))
		}
	}

	cfg.Redpanda.Id = int(hostIndex)

	// First Redpanda node need to have cleared seed servers in order
	// to form raft group 0
	if hostIndex == 0 {
		cfg.Redpanda.SeedServers = []config.SeedServer{}
	}

	cfgBytes, err := yaml.Marshal(cfg)
	if err != nil {
		log.Fatalf("%s", fmt.Errorf("unable to marshal the configuration: %w", err))
	}

	log.Printf("Config: %s", string(cfgBytes))

	if err := ioutil.WriteFile(c.configDestination, cfgBytes, 0600); err != nil {
		log.Fatalf("%s", fmt.Errorf("unable to write the destination configuration file: %w", err))
	}

	log.Printf("Configuration saved to: %s", c.configDestination)
}

var errInternalPortMissing = errors.New("port configration is missing internal port")

func getInternalKafkaAPIPort(cfg *config.Config) (int, error) {
	for _, l := range cfg.Redpanda.KafkaApi {
		if l.Name == "kafka" {
			return l.Port, nil
		}
	}
	return 0, fmt.Errorf("%w %v", errInternalPortMissing, cfg.Redpanda.KafkaApi)
}

func getInternalProxyAPIPort(cfg *config.Config) int {
	for _, l := range cfg.Pandaproxy.PandaproxyAPI {
		if l.Name == "proxy" {
			return l.Port
		}
	}
	return 0
}

func getNode(nodeName string) (*corev1.Node, error) {
	k8sconfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to create in cluster config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(k8sconfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create clientset: %w", err)
	}

	node, err := clientset.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve node: %w", err)
	}
	return node, nil
}

func registerAdvertisedKafkaAPI(
	c *configuratorConfig, cfg *config.Config, index brokerID, kafkaAPIPort int,
) error {

	internalIndex, _ := strconv.Atoi(fmt.Sprintf("%d", index))

	cfg.Redpanda.AdvertisedKafkaApi = []config.NamedSocketAddress{
		{
			SocketAddress: config.SocketAddress{
				Address: c.hostName + "." + c.svcFQDN,
				Port:    kafkaAPIPort,
			},
			Name: "kafka",
		},
	}

	if !c.externalConnectivity {
		return nil
	}

	if len(c.subdomain) > 0 {
		if c.ordinalBrokerHostname {
			cfg.Redpanda.AdvertisedKafkaApi = append(cfg.Redpanda.AdvertisedKafkaApi, config.NamedSocketAddress{
				SocketAddress: config.SocketAddress{
					Address: fmt.Sprintf("%d.%s", index, c.subdomain),
					Port:    c.hostPort,
				},
				Name: "kafka-external",
			})
		} else {
			port := c.hostPort
			address := fmt.Sprintf("%d.%s", index, c.subdomain)
			if !c.ordinalPortPerBroker && c.basePort > 0 {
				port = c.basePort + internalIndex
				address = fmt.Sprintf("%s.%s", hostBaseName(c.hostName), c.subdomain)
			}

			cfg.Redpanda.AdvertisedKafkaApi = append(cfg.Redpanda.AdvertisedKafkaApi, config.NamedSocketAddress{
				SocketAddress: config.SocketAddress{
					Address: address,
					Port:    port,
				},
				Name: "kafka-external",
			})
		}

		return nil
	}

	node, err := getNode(c.nodeName)
	if err != nil {
		return fmt.Errorf("unable to retrieve node: %w", err)
	}

	port := c.hostPort
	if !c.ordinalPortPerBroker && c.basePort > 0 {
		port = c.basePort + internalIndex
	}

	cfg.Redpanda.AdvertisedKafkaApi = append(cfg.Redpanda.AdvertisedKafkaApi, config.NamedSocketAddress{
		SocketAddress: config.SocketAddress{
			Address: getExternalIP(node),
			Port:    port,
		},
		Name: "kafka-external",
	})

	return nil
}

func registerAdvertisedPandaproxyAPI(
	c *configuratorConfig, cfg *config.Config, index brokerID, proxyAPIPort int,
) error {
	cfg.Pandaproxy.AdvertisedPandaproxyAPI = []config.NamedSocketAddress{
		{
			SocketAddress: config.SocketAddress{
				Address: c.hostName + "." + c.svcFQDN,
				Port:    proxyAPIPort,
			},
			Name: "proxy",
		},
	}

	if c.proxyHostPort == 0 {
		return nil
	}

	internalIndex, _ := strconv.Atoi(fmt.Sprintf("%d", index))

	// Pandaproxy uses the Kafka API subdomain.
	if len(c.subdomain) > 0 {
		if c.ordinalBrokerHostname {
			cfg.Pandaproxy.AdvertisedPandaproxyAPI = append(cfg.Pandaproxy.AdvertisedPandaproxyAPI, config.NamedSocketAddress{
				SocketAddress: config.SocketAddress{
					Address: fmt.Sprintf("%d.%s", index, c.subdomain),
					Port:    c.proxyHostPort,
				},
				Name: "proxy-external",
			})
		} else {
			port := c.hostPort
			address := fmt.Sprintf("%d.%s", index, c.subdomain)
			if !c.ordinalPortPerBroker && c.basePort > 0 {
				port = c.basePort + internalIndex
				address = fmt.Sprintf("%s.%s", hostBaseName(c.hostName), c.subdomain)
			}
			cfg.Pandaproxy.AdvertisedPandaproxyAPI = append(cfg.Pandaproxy.AdvertisedPandaproxyAPI, config.NamedSocketAddress{
				SocketAddress: config.SocketAddress{
					Address: address,
					Port:    port,
				},
				Name: "proxy-external",
			})
		}

		return nil
	}

	node, err := getNode(c.nodeName)
	if err != nil {
		return fmt.Errorf("unable to retrieve node: %w", err)
	}
	port := c.hostPort
	if !c.ordinalPortPerBroker && c.basePort > 0 {
		port = c.basePort + internalIndex
	}
	cfg.Pandaproxy.AdvertisedPandaproxyAPI = append(cfg.Pandaproxy.AdvertisedPandaproxyAPI, config.NamedSocketAddress{
		SocketAddress: config.SocketAddress{
			Address: getExternalIP(node),
			Port:    port,
		},
		Name: "proxy-external",
	})

	return nil
}

func getExternalIP(node *corev1.Node) string {
	if node == nil {
		return ""
	}
	for _, address := range node.Status.Addresses {
		if address.Type == corev1.NodeExternalIP {
			return address.Address
		}
	}
	return ""
}

func checkEnvVars() (configuratorConfig, error) {
	var result error
	var extCon string
	var rpcPort string
	var hostPort string

	c := configuratorConfig{}

	envVarList := []struct {
		value *string
		name  string
	}{
		{
			value: &c.hostName,
			name:  hostNameEnvVar,
		},
		{
			value: &c.svcFQDN,
			name:  svcFQDNEnvVar,
		},
		{
			value: &c.configSourceDir,
			name:  configSourceDirEnvVar,
		},
		{
			value: &c.configDestination,
			name:  configDestinationEnvVar,
		},
		{
			value: &c.nodeName,
			name:  nodeNameEnvVar,
		},
		{
			value: &c.subdomain,
			name:  externalConnectivitySubDomainEnvVar,
		},
		{
			value: &extCon,
			name:  externalConnectivityEnvVar,
		},
		{
			value: &rpcPort,
			name:  redpandaRPCPortEnvVar,
		},
		{
			value: &hostPort,
			name:  hostPortEnvVar,
		},
	}
	for _, envVar := range envVarList {
		v, exist := os.LookupEnv(envVar.name)
		if !exist {
			result = multierror.Append(result, fmt.Errorf("%s %w", envVar.name, errorMissingEnvironmentVariable))
		}
		*envVar.value = v
	}

	extCon, exist := os.LookupEnv(externalConnectivityEnvVar)
	if !exist {
		result = multierror.Append(result, fmt.Errorf("%s %w", externalConnectivityEnvVar, errorMissingEnvironmentVariable))
	}

	var err error
	c.externalConnectivity, err = strconv.ParseBool(extCon)
	if err != nil {
		result = multierror.Append(result, fmt.Errorf("unable to parse bool: %w", err))
	}

	c.redpandaRPCPort, err = strconv.Atoi(rpcPort)
	if err != nil {
		result = multierror.Append(result, fmt.Errorf("unable to convert rpc port from string to int: %w", err))
	}

	c.hostPort, err = strconv.Atoi(hostPort)
	if err != nil && c.externalConnectivity {
		result = multierror.Append(result, fmt.Errorf("unable to convert host port from string to int: %w", err))
	}

	// Providing proxy host port is optional
	proxyHostPort, exist := os.LookupEnv(proxyHostPortEnvVar)
	if exist && proxyHostPort != "" {
		c.proxyHostPort, err = strconv.Atoi(proxyHostPort)
		if err != nil {
			result = multierror.Append(result, fmt.Errorf("unable to convert proxy host port from string to int: %w", err))
		}
	}

	ordinalPortPerBrokerVar, exist := os.LookupEnv(ordinalPortPerBroker)
	if exist {
		c.ordinalPortPerBroker, err = strconv.ParseBool(ordinalPortPerBrokerVar)
		if err != nil {
			result = multierror.Append(result, fmt.Errorf("unable to convert ordinal port per broker from string to bool: %w", err))
		}
	}

	ordinalBrokerHostnameVar, exist := os.LookupEnv(ordinalBrokerHostname)
	if exist {
		c.ordinalBrokerHostname, err = strconv.ParseBool(ordinalBrokerHostnameVar)
		if err != nil {
			result = multierror.Append(result, fmt.Errorf("unable to convert ordinal broker hostname from string to bool: %w", err))
		}
	}

	basePortVar, exist := os.LookupEnv(basePort)
	if exist {
		c.basePort, err = strconv.Atoi(basePortVar)
		if err != nil {
			result = multierror.Append(result, fmt.Errorf("unable to convert base port from string to int: %w", err))
		}
	}
	return c, result
}

// hostIndex takes advantage of pod naming convention in Kubernetes StatfulSet
// the last number is the index of replica. This index is then propagated
// to redpanda.node_id.
func hostIndex(hostName string) (brokerID, error) {
	s := strings.Split(hostName, "-")
	last := len(s) - 1
	i, err := strconv.Atoi(s[last])
	return brokerID(i), err
}

func hostBaseName(hostName string) string {
	s := strings.Split(hostName, "-")
	last := len(s) - 1
	return strings.Join(s[:last], "-")
}
