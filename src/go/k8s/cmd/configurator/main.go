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
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/moby/sys/mountinfo"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/networking"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/utils"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
)

const (
	configDestinationEnvVar                              = "CONFIG_DESTINATION"
	configSourceDirEnvVar                                = "CONFIG_SOURCE_DIR"
	externalConnectivityAddressTypeEnvVar                = "EXTERNAL_CONNECTIVITY_ADDRESS_TYPE"
	externalConnectivityEnvVar                           = "EXTERNAL_CONNECTIVITY"
	externalConnectivityKafkaEndpointTemplateEnvVar      = "EXTERNAL_CONNECTIVITY_KAFKA_ENDPOINT_TEMPLATE"
	externalConnectivityPandaProxyEndpointTemplateEnvVar = "EXTERNAL_CONNECTIVITY_PANDA_PROXY_ENDPOINT_TEMPLATE"
	externalConnectivitySubDomainEnvVar                  = "EXTERNAL_CONNECTIVITY_SUBDOMAIN"
	hostIPEnvVar                                         = "HOST_IP_ADDRESS"
	hostNameEnvVar                                       = "HOSTNAME"
	hostPortEnvVar                                       = "HOST_PORT"
	nodeNameEnvVar                                       = "NODE_NAME"
	proxyHostPortEnvVar                                  = "PROXY_HOST_PORT"
	rackAwarenessEnvVar                                  = "RACK_AWARENESS"
	validateMountedVolumeEnvVar                          = "VALIDATE_MOUNTED_VOLUME"
	redpandaRPCPortEnvVar                                = "REDPANDA_RPC_PORT"
	svcFQDNEnvVar                                        = "SERVICE_FQDN"
)

type brokerID int

type configuratorConfig struct {
	configDestination                              string
	configSourceDir                                string
	externalConnectivity                           bool
	externalConnectivityAddressType                corev1.NodeAddressType
	externalConnectivityKafkaEndpointTemplate      string
	externalConnectivityPandaProxyEndpointTemplate string
	hostIP                                         string
	hostName                                       string
	hostPort                                       int
	nodeName                                       string
	proxyHostPort                                  int
	rackAwareness                                  bool
	validateMountedVolume                          bool
	redpandaRPCPort                                int
	subdomain                                      string
	svcFQDN                                        string
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
		"rackAwareness: %t\n"+
		"validateMountedVolume: %t\n",
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
		c.rackAwareness,
		c.validateMountedVolume)
}

var errorMissingEnvironmentVariable = errors.New("missing environment variable")

func main() {
	log.Print("The redpanda configurator is starting")

	c, err := checkEnvVars()
	if err != nil {
		log.Fatalf("%s", fmt.Errorf("unable to get the environment variables: %w", err))
	}

	log.Print(c.String())

	p := path.Join(c.configSourceDir, "redpanda.yaml")
	cf, err := os.ReadFile(p)
	if err != nil {
		log.Fatalf("%s", fmt.Errorf("unable to read the redpanda configuration file, %q: %w", p, err))
	}
	cfg := &config.Config{}
	err = yaml.Unmarshal(cf, cfg)
	if err != nil {
		log.Fatalf("%s", fmt.Errorf("unable to parse the redpanda configuration file, %q: %w", p, err))
	}

	err = validateMountedVolume(cfg, c.validateMountedVolume)
	if err != nil {
		log.Fatalf("%s", fmt.Errorf("unable to pass validation for the mounted volume: %w", err))
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

	// New bootstrap with v22.3, if redpanda.empty_seed_starts_cluster is false redpanda automatically
	// generated IDs and forms clusters using the full set of nodes.
	if cfg.Redpanda.EmptySeedStartsCluster != nil && !*cfg.Redpanda.EmptySeedStartsCluster {
		cfg.Redpanda.ID = nil
	} else {
		cfg.Redpanda.ID = new(int)
		*cfg.Redpanda.ID = int(hostIndex)

		// In case of a single seed server, the list should contain the current node itself.
		// Normally the cluster is able to recognize it's talking to itself, except when the cluster is
		// configured to use mutual TLS on the Kafka API (see Helm test).
		// So, we clear the list of seeds to help Redpanda.
		if len(cfg.Redpanda.SeedServers) == 1 {
			cfg.Redpanda.SeedServers = []config.SeedServer{}
		}
	}

	if c.rackAwareness {
		zone, zoneID, errZone := getZoneLabels(c.nodeName)
		if errZone != nil {
			log.Fatalf("%s", fmt.Errorf("unable to retrieve zone labels: %w", errZone))
		}
		populateRack(cfg, zone, zoneID)
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

func validateMountedVolume(cfg *config.Config, validate bool) error {
	if !validate {
		return nil
	}
	dir, err := os.Open(cfg.Redpanda.Directory)
	if err != nil {
		return fmt.Errorf("unable to open Redpanda directory (%s): %w", cfg.Redpanda.Directory, err)
	}
	defer func() {
		if errClose := dir.Close(); errClose != nil {
			log.Printf("Error closing file: %s, %s\n", cfg.Redpanda.Directory, errClose)
		}
	}()

	stat, err := dir.Stat()
	if err != nil {
		return fmt.Errorf("unable to stat the dir: %s: %w", cfg.Redpanda.Directory, err)
	}

	if !stat.IsDir() {
		return fmt.Errorf("%s is not a directory", cfg.Redpanda.Directory) //nolint:goerr113 // Error will not be validated, but rather returned to the end user of configurator
	}

	info, err := mountinfo.GetMounts(mountinfo.FSTypeFilter("xfs"))
	if err != nil {
		return fmt.Errorf("%s must have an xfs formatted filesystem. unable to find xfs file system in /proc/self/mountinfo: %w", cfg.Redpanda.Directory, err)
	}

	if len(info) == 0 {
		return fmt.Errorf("%s must have an xfs formatted filesystem. returned mount info (/proc/self/mountinfo) does not have any xfs file system", cfg.Redpanda.Directory) //nolint:goerr113 // Error will not be validated, but rather returned to the end user of configurator
	}

	found := false
	for _, fs := range info {
		if fs.Mountpoint == cfg.Redpanda.Directory {
			found = true
		}
	}

	if !found {
		return fmt.Errorf("returned XFS mount info list (/proc/self/mountinfo) does not have Redpanda directory (%s)", cfg.Redpanda.Directory) //nolint:goerr113 // Error will not be validated, but rather returned to the end user of configurator
	}

	file := filepath.Join(cfg.Redpanda.Directory, "testing.file")
	err = os.WriteFile(file, []byte("test-content"), 0o600)
	if err != nil {
		return fmt.Errorf("unable to write to test file (%s): %w", file, err)
	}

	err = os.Remove(file)
	if err != nil {
		return fmt.Errorf("unable to remove test file (%s): %w", file, err)
	}

	return nil
}

var errInternalPortMissing = errors.New("port configuration is missing internal port")

func getZoneLabels(nodeName string) (zone, zoneID string, err error) {
	node, err := getNode(nodeName)
	if err != nil {
		return "", "", fmt.Errorf("unable to retrieve node: %w", err)
	}
	zone = node.Labels["topology.kubernetes.io/zone"]
	zoneID = node.Labels["topology.cloud.redpanda.com/zone-id"]
	return zone, zoneID, nil
}

func populateRack(cfg *config.Config, zone, zoneID string) {
	cfg.Redpanda.Rack = zoneID
	if zoneID == "" {
		cfg.Redpanda.Rack = zone
	}
}

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
			Address: c.hostName + "." + c.svcFQDN,
			Port:    kafkaAPIPort,
			Name:    "kafka",
		},
	}

	if !c.externalConnectivity {
		return nil
	}

	if len(c.subdomain) > 0 {
		data := utils.NewEndpointTemplateData(int(index), c.hostIP)
		ep, err := utils.ComputeEndpoint(c.externalConnectivityKafkaEndpointTemplate, data)
		if err != nil {
			return err
		}

		cfg.Redpanda.AdvertisedKafkaAPI = append(cfg.Redpanda.AdvertisedKafkaAPI, config.NamedSocketAddress{
			Address: fmt.Sprintf("%s.%s", ep, c.subdomain),
			Port:    c.hostPort,
			Name:    "kafka-external",
		})
		return nil
	}

	node, err := getNode(c.nodeName)
	if err != nil {
		return fmt.Errorf("unable to retrieve node: %w", err)
	}

	cfg.Redpanda.AdvertisedKafkaAPI = append(cfg.Redpanda.AdvertisedKafkaAPI, config.NamedSocketAddress{
		Address: networking.GetPreferredAddress(node, c.externalConnectivityAddressType),
		Port:    c.hostPort,
		Name:    "kafka-external",
	})

	return nil
}

func registerAdvertisedPandaproxyAPI(
	c *configuratorConfig, cfg *config.Config, index brokerID, proxyAPIPort int,
) error {
	cfg.Pandaproxy.AdvertisedPandaproxyAPI = []config.NamedSocketAddress{
		{
			Address: c.hostName + "." + c.svcFQDN,
			Port:    proxyAPIPort,
			Name:    "proxy",
		},
	}

	if c.proxyHostPort == 0 {
		return nil
	}

	// Pandaproxy uses the Kafka API subdomain.
	if len(c.subdomain) > 0 {
		data := utils.NewEndpointTemplateData(int(index), c.hostIP)
		ep, err := utils.ComputeEndpoint(c.externalConnectivityPandaProxyEndpointTemplate, data)
		if err != nil {
			return err
		}

		cfg.Pandaproxy.AdvertisedPandaproxyAPI = append(cfg.Pandaproxy.AdvertisedPandaproxyAPI, config.NamedSocketAddress{
			Address: fmt.Sprintf("%s.%s", ep, c.subdomain),
			Port:    c.proxyHostPort,
			Name:    "proxy-external",
		})
		return nil
	}

	node, err := getNode(c.nodeName)
	if err != nil {
		return fmt.Errorf("unable to retrieve node: %w", err)
	}

	cfg.Pandaproxy.AdvertisedPandaproxyAPI = append(cfg.Pandaproxy.AdvertisedPandaproxyAPI, config.NamedSocketAddress{
		Address: getExternalIP(node),
		Port:    c.proxyHostPort,
		Name:    "proxy-external",
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

//nolint:funlen // envs are many
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
			value: &c.externalConnectivityKafkaEndpointTemplate,
			name:  externalConnectivityKafkaEndpointTemplateEnvVar,
		},
		{
			value: &c.externalConnectivityPandaProxyEndpointTemplate,
			name:  externalConnectivityPandaProxyEndpointTemplateEnvVar,
		},
		{
			value: &c.hostIP,
			name:  hostIPEnvVar,
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

	rackAwareness, exist := os.LookupEnv(rackAwarenessEnvVar)
	if !exist {
		result = multierror.Append(result, fmt.Errorf("%s %w", rackAwarenessEnvVar, errorMissingEnvironmentVariable))
	}
	c.rackAwareness, err = strconv.ParseBool(rackAwareness)
	if err != nil {
		result = multierror.Append(result, fmt.Errorf("unable to parse bool: %w", err))
	}

	validateMountedVolume, exist := os.LookupEnv(validateMountedVolumeEnvVar)
	if !exist {
		result = multierror.Append(result, fmt.Errorf("%s %w", validateMountedVolumeEnvVar, errorMissingEnvironmentVariable))
	}
	c.validateMountedVolume, err = strconv.ParseBool(validateMountedVolume)
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
// the last number is the index of replica. This index is then propagated
// to redpanda.node_id.
func hostIndex(hostName string) (brokerID, error) {
	s := strings.Split(hostName, "-")
	last := len(s) - 1
	i, err := strconv.Atoi(s[last])
	return brokerID(i), err
}
