// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package common

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/docker/docker/client"
	"github.com/opencontainers/go-digest"
)

// clientFromDockerContext reads the current docker context and parse it to
// obtain the docker host, and creates a client with it.
func clientFromDockerContext() (*client.Client, error) {
	currCtx, err := currentContext()
	if err != nil {
		return nil, err
	}
	host, err := hostFromContext(currCtx.CurrentContext)
	if err != nil {
		return nil, err
	}
	return client.NewClientWithOpts(client.WithHost(host))
}

type dockerConfig struct {
	CurrentContext string `json:"currentContext"`
}

const (
	dockerConfigDir   = ".docker"
	dockerConfigFile  = "config.json"
	dockerContextDir  = "contexts"
	dockerContextEnv  = "DOCKER_CONTEXT"
	dockerEndpoint    = "docker"
	dockerMetaFile    = "meta.json"
	dockerMetadataDir = "meta"
)

// CurrentContext returns the current docker context name based on the docker
// environment variables or the cli configuration file, in the following
// order of preference:
//
//  1. The "DOCKER_CONTEXT" environment variable.
//  2. The current context as configured through the in "currentContext"
//     field in the CLI configuration file ("~/.docker/config.json").
//
// It errors if no context is configured. This is matching what the docker CLI
// does for context handling.
func currentContext() (*dockerConfig, error) {
	if curCtx, ok := os.LookupEnv(dockerContextEnv); ok {
		return &dockerConfig{curCtx}, nil
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	configPath := filepath.Join(homeDir, dockerConfigDir, dockerConfigFile)
	file, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("unable to find your docker configuration file: %v, please make sure you have Docker installed and running", err)
	}
	var cfg dockerConfig
	err = json.Unmarshal(file, &cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to read your docker config: %v", err)
	}
	if cfg.CurrentContext == "" {
		return nil, fmt.Errorf("unable to get current docker context, please make sure you have Docker installed and running")
	}
	return &cfg, nil
}

type dockerMetadataFile struct {
	Endpoints map[string]dockerEndpointMeta `json:"Endpoints,omitempty"`
}
type dockerEndpointMeta struct {
	Host string `json:"Host"`
}

func hostFromContext(context string) (string, error) {
	// https://github.com/docker/cli/blob/master/cli/context/store/doc.go#L30
	contextDir := digest.FromString(context).Encoded()

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	metaFilePath := filepath.Join(homeDir, dockerConfigDir, dockerContextDir, dockerMetadataDir, contextDir, dockerMetaFile)
	file, err := os.ReadFile(metaFilePath)
	if err != nil {
		return "", fmt.Errorf("unable to find your docker context file: %v, please make sure you have Docker installed and running", err)
	}
	var mFile dockerMetadataFile
	err = json.Unmarshal(file, &mFile)
	if err != nil {
		return "", fmt.Errorf("unable to read your docker metadata file: %v", err)
	}
	ep, ok := mFile.Endpoints[dockerEndpoint]
	if !ok {
		return "", fmt.Errorf("unable to find docker endpoint in your metadata file: %v", err)
	}
	return ep.Host, nil
}
