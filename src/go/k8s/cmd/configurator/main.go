// Copyright 2021 Redpanda Data, Inc.
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
	"log"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/networking"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	hostNameEnvVar                        = "HOSTNAME"
	svcFQDNEnvVar                         = "SERVICE_FQDN"
	configSourceDirEnvVar                 = "CONFIG_SOURCE_DIR"
	configDestinationEnvVar               = "CONFIG_DESTINATION"
	redpandaRPCPortEnvVar                 = "REDPANDA_RPC_PORT"
	nodeNameEnvVar                        = "NODE_NAME"
	externalConnectivityEnvVar            = "EXTERNAL_CONNECTIVITY"
	externalConnectivitySubDomainEnvVar   = "EXTERNAL_CONNECTIVITY_SUBDOMAIN"
	externalConnectivityAddressTypeEnvVar = "EXTERNAL_CONNECTIVITY_ADDRESS_TYPE"
	hostPortEnvVar                        = "HOST_PORT"
	proxyHostPortEnvVar                   = "PROXY_HOST_PORT"
	dataDirPath                           = "DATA_DIR_PATH"
)

type brokerID int

type configuratorConfig struct {
	hostName                        string
	svcFQDN                         string
	configSourceDir                 string
	configDestination               string
	nodeName                        string
	subdomain                       string
	externalConnectivity            bool
	externalConnectivityAddressType corev1.NodeAddressType
	redpandaRPCPort                 int
	hostPort                        int
	proxyHostPort                   int
	dataDirPath                     string
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
		"externalConnectivityAddressType: %s\n"+
		"redpandaRPCPort: %d\n"+
		"hostPort: %d\n"+
		"proxyHostPort: %d\n"+
		"dataDirPath: %s\n",
		c.hostName,
		c.svcFQDN,
		c.configSourceDir,
		c.configDestination,
		c.nodeName,
		c.externalConnectivity,
		c.subdomain,
		c.externalConnectivityAddressType,
		c.redpandaRPCPort,
		c.hostPort,
		c.proxyHostPort,
		c.dataDirPath)
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

	err = calculateRedpandaID(cfg, c, hostIndex)
	if err != nil {
		log.Fatalf("%s", fmt.Errorf("unable to register node Redpanda ID: %w", err))
	}

	err = initializeSeedSeverList(cfg, c, hostIndex)
	if err != nil {
		log.Fatalf("%s", fmt.Errorf("unable to determine seed server list: %w", err))
	}

	cfgBytes, err := yaml.Marshal(cfg)
	if err != nil {
		log.Fatalf("%s", fmt.Errorf("unable to marshal the configuration: %w", err))
	}

	if err := os.WriteFile(c.configDestination, cfgBytes, 0o600); err != nil {
		log.Fatalf("%s", fmt.Errorf("unable to write the destination configuration file: %w", err))
	}

	log.Printf("Configuration saved to: %s", c.configDestination)
}

func initializeSeedSeverList(
	cfg *config.Config, c configuratorConfig, index brokerID,
) error {
	empty, err := IsRedpandaDataFolderEmpty(c.dataDirPath)
	if err != nil {
		return fmt.Errorf("checking Redpanda data folder content (%s): %w", c.dataDirPath, err)
	}

	if index == 0 && empty {
		cfg.Redpanda.SeedServers = []config.SeedServer{}
	}

	return nil
}

func calculateRedpandaID(
	cfg *config.Config, c configuratorConfig, initialID brokerID,
) error {
	redpandaIDFile := filepath.Join(c.dataDirPath, ".redpanda_id")

	_, err := os.Stat(redpandaIDFile)
	if errors.Is(err, os.ErrNotExist) {
		empty, err := IsRedpandaDataFolderEmpty(c.dataDirPath)
		if err != nil {
			return fmt.Errorf("checking Redpanda data folder content (%s): %w", c.dataDirPath, err)
		}

		if !empty {
			err = os.WriteFile(redpandaIDFile, []byte(strconv.Itoa(int(initialID))), 0o666)
			if err != nil {
				return fmt.Errorf("storing redpanda ID in data folder (%s): %w", c.dataDirPath, err)
			}
			cfg.Redpanda.ID = int(initialID)
			return nil
		}

		redpandaID := int(rand.NewSource(time.Now().Unix()).Int63())

		err = os.WriteFile(redpandaIDFile, []byte(strconv.Itoa(redpandaID)), 0o666)
		if err != nil {
			return fmt.Errorf("storing redpanda ID in data folder (%s): %w", c.dataDirPath, err)
		}
		cfg.Redpanda.ID = redpandaID
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat redpanda ID file: %w", err)
	}

	redpandaID, err := os.ReadFile(redpandaIDFile)
	if err != nil {
		return fmt.Errorf("reading redpanda id file: %w", err)
	}

	rID, err := strconv.Atoi(string(redpandaID))
	if err != nil {
		return fmt.Errorf("converting content of redpanda id file to int: %w", err)
	}
	cfg.Redpanda.ID = rID

	return nil
}

func IsRedpandaDataFolderEmpty(dataDirPath string) (bool, error) {
	de, err := os.ReadDir(dataDirPath)
	if err != nil {
		return false, fmt.Errorf("reading volume content: %w", err)
	}
	return len(de) == 0, nil
}

var errInternalPortMissing = errors.New("port configration is missing internal port")

func getInternalKafkaAPIPort(cfg *config.Config) (int, error) {
	for _, l := range cfg.Redpanda.KafkaAPI {
		if l.Name == "kafka" {
			return l.Port, nil
		}
	}
	return 0, fmt.Errorf("%w %v", errInternalPortMissing, cfg.Redpanda.KafkaAPI)
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
	cfg.Redpanda.AdvertisedKafkaAPI = []config.NamedSocketAddress{
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
		cfg.Redpanda.AdvertisedKafkaAPI = append(cfg.Redpanda.AdvertisedKafkaAPI, config.NamedSocketAddress{
			SocketAddress: config.SocketAddress{
				Address: fmt.Sprintf("%d.%s", index, c.subdomain),
				Port:    c.hostPort,
			},
			Name: "kafka-external",
		})
		return nil
	}

	node, err := getNode(c.nodeName)
	if err != nil {
		return fmt.Errorf("unable to retrieve node: %w", err)
	}

	cfg.Redpanda.AdvertisedKafkaAPI = append(cfg.Redpanda.AdvertisedKafkaAPI, config.NamedSocketAddress{
		SocketAddress: config.SocketAddress{
			Address: networking.GetPreferredAddress(node, c.externalConnectivityAddressType),
			Port:    c.hostPort,
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

	// Pandaproxy uses the Kafka API subdomain.
	if len(c.subdomain) > 0 {
		cfg.Pandaproxy.AdvertisedPandaproxyAPI = append(cfg.Pandaproxy.AdvertisedPandaproxyAPI, config.NamedSocketAddress{
			SocketAddress: config.SocketAddress{
				Address: fmt.Sprintf("%d.%s", index, c.subdomain),
				Port:    c.proxyHostPort,
			},
			Name: "proxy-external",
		})
		return nil
	}

	node, err := getNode(c.nodeName)
	if err != nil {
		return fmt.Errorf("unable to retrieve node: %w", err)
	}

	cfg.Pandaproxy.AdvertisedPandaproxyAPI = append(cfg.Pandaproxy.AdvertisedPandaproxyAPI, config.NamedSocketAddress{
		SocketAddress: config.SocketAddress{
			Address: getExternalIP(node),
			Port:    c.proxyHostPort,
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
		{
			value: &c.dataDirPath,
			name:  dataDirPath,
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

	// Providing the address type is optional.
	addressType, exists := os.LookupEnv(externalConnectivityAddressTypeEnvVar)
	if exists {
		c.externalConnectivityAddressType = corev1.NodeAddressType(addressType)
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

	return c, result
}

// hostIndex takes advantage of pod naming convention in Kubernetes StatfulSet
// the last number is the index of replica.
func hostIndex(hostName string) (brokerID, error) {
	s := strings.Split(hostName, "-")
	last := len(s) - 1
	i, err := strconv.Atoi(s[last])
	return brokerID(i), err
}
