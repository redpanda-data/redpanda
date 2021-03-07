// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// main package. This line should be remove after merging the following
// https://github.com/vectorizedio/redpanda/pull/753
package main

import (
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
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"gopkg.in/yaml.v3"
)

const (
	hostNameEnvVar          = "HOSTNAME"
	svcFQDNEnvVar           = "SERVICE_FQDN"
	configSourceDirEnvVar   = "CONFIG_SOURCE_DIR"
	configDestinationEnvVar = "CONFIG_DESTINATION"
	redpandaRPCPortEnvVar   = "REDPANDA_RPC_PORT"
)

type configuratorConfig struct {
	hostName          string
	svcFQDN           string
	configSourceDir   string
	configDestination string
	redpandaRPCPort   int
}

var errorMissingEnvironmentVariable = errors.New("missing environment variable")

func main() {
	log.Printf("The redpanda configurator is starting")

	c, err := checkEnvVars()
	if err != nil {
		log.Fatalf("%s", fmt.Errorf("unable to get the environment variables: %w", err))
	}

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

	log.Printf("Decode done")

	hostIndex, err := hostIndex(c.hostName)
	if err != nil {
		log.Fatalf("%s", fmt.Errorf("unable to extract host index: %w", err))
	}

	log.Printf("Host index calculated %d", hostIndex)

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

func checkEnvVars() (configuratorConfig, error) {
	var exist bool
	var result error
	c := configuratorConfig{}
	c.hostName, exist = os.LookupEnv(hostNameEnvVar)
	if !exist {
		result = multierror.Append(result, fmt.Errorf("HOSTNAME %w", errorMissingEnvironmentVariable))
	}

	c.svcFQDN, exist = os.LookupEnv(svcFQDNEnvVar)
	if !exist {
		result = multierror.Append(result, fmt.Errorf("SERVICE_FQDN %w", errorMissingEnvironmentVariable))
	}

	c.configSourceDir, exist = os.LookupEnv(configSourceDirEnvVar)
	if !exist {
		result = multierror.Append(result, fmt.Errorf("CONFIG_SOURCE_DIR %w", errorMissingEnvironmentVariable))
	}

	c.configDestination, exist = os.LookupEnv(configDestinationEnvVar)
	if !exist {
		result = multierror.Append(result, fmt.Errorf("CONFIG_DESTINATION %w", errorMissingEnvironmentVariable))
	}

	rpcPort, exist := os.LookupEnv(redpandaRPCPortEnvVar)
	if !exist {
		result = multierror.Append(result, fmt.Errorf("REDPANDA_RPC_PORT %w", errorMissingEnvironmentVariable))
	}

	var err error
	c.redpandaRPCPort, err = strconv.Atoi(rpcPort)
	if err != nil {
		result = multierror.Append(result, fmt.Errorf("unable to convert rpc port from string to int: %w", err))
	}

	log.Printf("The configuration:\n"+
		"hostName: %s\n"+
		"svcFQDN: %s\n"+
		"configSourceDir: %s\n"+
		"configDestination: %s\n"+
		"redpandaRPCPort: %d\n",
		c.hostName,
		c.svcFQDN,
		c.configSourceDir,
		c.configDestination,
		c.redpandaRPCPort)

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
