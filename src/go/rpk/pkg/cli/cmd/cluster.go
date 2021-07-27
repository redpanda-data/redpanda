// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cmd

import (
	"github.com/Shopify/sarama"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/cluster"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
)

func NewClusterCommand(fs afero.Fs, mgr config.Manager) *cobra.Command {
	var (
		brokers        []string
		configFile     string
		user           string
		password       string
		mechanism      string
		enableTLS      bool
		certFile       string
		keyFile        string
		truststoreFile string
	)
	command := &cobra.Command{
		Use:   "cluster",
		Short: "Interact with a Redpanda cluster",
	}
	common.AddKafkaFlags(
		command,
		&configFile,
		&user,
		&password,
		&mechanism,
		&enableTLS,
		&certFile,
		&keyFile,
		&truststoreFile,
		&brokers,
	)
	// The ideal way to pass common (global flags') values would be to
	// declare PersistentPreRun hooks on each command root (such as rpk
	// api), validating them there and them passing them down to its
	// subcommands. However, Cobra only executes the last hook defined in
	// the command chain. Since NewTopicCommand requires a PersistentPreRun
	// hook to initialize the sarama Client and Admin, it overrides whatever
	// PersistentPreRun hook was declared in a parent command.
	// An alternative would be to declare a global var to hold the global
	// flag's value, but this would require flattening out the package
	// hierarchy to avoid import cycles (parent command imports child
	// command's package, child cmd import parent cmd's package to access
	// the flag's value), but this leads to entangled code.
	// As a cleaner workaround, the brokers value has to be gotten through a
	// closure with references to the required values (the config file
	// path, the list of brokers passed through --brokers) to deduce the
	// actual brokers list to be used.
	configClosure := common.FindConfigFile(mgr, &configFile)
	brokersClosure := common.DeduceBrokers(
		common.CreateDockerClient,
		configClosure,
		&brokers,
	)
	kAuthClosure := common.KafkaAuthConfig(&user, &password, &mechanism, configClosure)
	tlsClosure := common.BuildKafkaTLSConfig(fs, &enableTLS, &certFile, &keyFile, &truststoreFile, configClosure)
	adminClosure := common.CreateAdmin(brokersClosure, configClosure, tlsClosure, kAuthClosure)
	command.AddCommand(cluster.NewInfoCommand(adminClosure))

	// NewOffsetsCommand takes client and admin factories so we can mock both
	clientClosure := common.CreateClient(brokersClosure, configClosure, tlsClosure, kAuthClosure)
	adminWrapperClosure := func(client sarama.Client) (sarama.ClusterAdmin, error) {
		return sarama.NewClusterAdminFromClient(client)
	}
	command.AddCommand(cluster.NewOffsetsCommand(clientClosure, adminWrapperClosure))

	return command
}
