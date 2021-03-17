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
	"strconv"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/afero"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	hostNameEnvVar             = "HOSTNAME"
	svcFQDNEnvVar              = "SERVICE_FQDN"
	configSourceDirEnvVar      = "CONFIG_SOURCE_DIR"
	configDestinationEnvVar    = "CONFIG_DESTINATION"
	redpandaRPCPortEnvVar      = "REDPANDA_RPC_PORT"
	kafkaAPIEnvVar             = "KAFKA_API_PORT"
	nodeNameEnvVar             = "NODE_NAME"
	externalConnectivityEnvVar = "EXTERNAL_CONNECTIVITY"
	hostPortEnvVar             = "HOST_PORT"
)

type configuratorConfig struct {
	hostName             string
	svcFQDN              string
	configSourceDir      string
	configDestination    string
	nodeName             string
	externalConnectivity bool
	kafkaAPIPort         int
	redpandaRPCPort      int
	hostPort             int
}

func (c *configuratorConfig) String() string {
	return fmt.Sprintf("The configuration:\n"+
		"hostName: %s\n"+
		"svcFQDN: %s\n"+
		"configSourceDir: %s\n"+
		"configDestination: %s\n"+
		"nodeName: %s\n"+
		"externalConnectivity: %t\n"+
		"kafkaAPIPort: %d\n"+
		"redpandaRPCPort: %d\n"+
		"hostPort: %d\n",
		c.hostName,
		c.svcFQDN,
		c.configSourceDir,
		c.configDestination,
		c.nodeName,
		c.externalConnectivity,
		c.kafkaAPIPort,
		c.redpandaRPCPort,
		c.hostPort)
}

var errorMissingEnvironmentVariable = errors.New("missing environment variable")

func main() {
	log.Print("The redpanda configurator is starting")

	c, err := checkEnvVars()
	if err != nil {
		log.Fatalf("%s", fmt.Errorf("unable to get the environment variables: %w", err))
	}

	log.Print(c)

	fs := afero.NewOsFs()
	v := config.InitViper(fs)
	v.AddConfigPath(c.configSourceDir)

	if err = v.ReadInConfig(); err != nil {
		log.Fatalf("%s", fmt.Errorf("unable to read the redpanda configuration file: %w", err))
	}

	cfg := &config.Config{}
	decoderConfig := mapstructure.DecoderConfig{
		Result: cfg,
		// Sometimes viper will save int values as strings (i.e.
		// through BindPFlag) so we have to allow mapstructure
		// to cast them.
		WeaklyTypedInput: true,
	}

	decoder, err := mapstructure.NewDecoder(&decoderConfig)
	if err != nil {
		log.Fatalf("%s", fmt.Errorf("unable to create decoder config: %w", err))
	}

	err = decoder.Decode(v.AllSettings())
	if err != nil {
		log.Fatalf("%s", fmt.Errorf("unable to decode: %w", err))
	}

	log.Print("Decode done")

	hostIndex, err := hostIndex(c.hostName)
	if err != nil {
		log.Fatalf("%s", fmt.Errorf("unable to extract host index: %w", err))
	}

	log.Printf("Host index calculated %d", hostIndex)

	err = registerAdvertisedKafkaAPI(&c, cfg)
	if err != nil {
		log.Fatalf("%s", fmt.Errorf("unable to register advertised kafka API: %w", err))
	}

	registerKafkaAPI(&c, cfg)

	cfg.Redpanda.Id = hostIndex

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

func registerKafkaAPI(c *configuratorConfig, cfg *config.Config) {
	cfg.Redpanda.KafkaApi = []config.NamedSocketAddress{
		{
			SocketAddress: config.SocketAddress{
				Address: "0.0.0.0",
				Port:    c.kafkaAPIPort,
			},
			Name: "Internal",
		},
	}

	if !c.externalConnectivity {
		return
	}

	cfg.Redpanda.KafkaApi = append(cfg.Redpanda.KafkaApi, config.NamedSocketAddress{
		SocketAddress: config.SocketAddress{
			Address: "0.0.0.0",
			Port:    resources.CalculateExternalPort(c.kafkaAPIPort),
		},
		Name: "External",
	})
}

func registerAdvertisedKafkaAPI(
	c *configuratorConfig, cfg *config.Config,
) error {
	cfg.Redpanda.AdvertisedKafkaApi = []config.NamedSocketAddress{
		{
			SocketAddress: config.SocketAddress{
				Address: c.hostName + "." + c.svcFQDN,
				Port:    c.kafkaAPIPort,
			},
			Name: "Internal",
		},
	}

	if !c.externalConnectivity {
		return nil
	}

	k8sconfig, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("unable to create in cluster config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(k8sconfig)
	if err != nil {
		return fmt.Errorf("unable to create clientset: %w", err)
	}

	node, err := clientset.CoreV1().Nodes().Get(context.Background(), c.nodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("unable to retrieve node: %w", err)
	}

	cfg.Redpanda.AdvertisedKafkaApi = append(cfg.Redpanda.AdvertisedKafkaApi, config.NamedSocketAddress{
		SocketAddress: config.SocketAddress{
			Address: getExternalIP(node),
			Port:    c.hostPort,
		},
		Name: "External",
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
	var kafkaAPIPort string
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
			value: &extCon,
			name:  externalConnectivityEnvVar,
		},
		{
			value: &rpcPort,
			name:  redpandaRPCPortEnvVar,
		},
		{
			value: &kafkaAPIPort,
			name:  kafkaAPIEnvVar,
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

	c.kafkaAPIPort, err = strconv.Atoi(kafkaAPIPort)
	if err != nil {
		result = multierror.Append(result, fmt.Errorf("unable to convert kafka api port from string to int: %w", err))
	}

	c.hostPort, err = strconv.Atoi(hostPort)
	if err != nil && c.externalConnectivity {
		result = multierror.Append(result, fmt.Errorf("unable to convert host port from string to int: %w", err))
	}

	return c, result
}

// hostIndex takes advantage of pod naming convention in Kubernetes StatfulSet
// the last number is the index of replica. This index is then propagated
// to redpanda.node_id.
func hostIndex(hostName string) (int, error) {
	s := strings.Split(hostName, "-")
	last := len(s) - 1
	return strconv.Atoi(s[last])
}
