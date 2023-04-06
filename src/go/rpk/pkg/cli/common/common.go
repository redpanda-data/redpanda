// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package common

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/cobra"
)

const FeedbackMsg = `We'd love to hear about your experience with Redpanda:
https://redpanda.com/feedback`

func Deprecated(newCmd *cobra.Command, newUse string) *cobra.Command {
	newCmd.Deprecated = fmt.Sprintf("use %q instead", newUse)
	newCmd.Hidden = true

	if children := newCmd.Commands(); len(children) > 0 {
		for _, child := range children {
			Deprecated(child, newUse+" "+child.Name())
		}
	}
	return newCmd
}

func AddKafkaFlags(
	command *cobra.Command,
	configFile, user, password, saslMechanism *string,
	enableTLS *bool,
	certFile, keyFile, truststoreFile *string,
	brokers *[]string,
) *cobra.Command {
	command.PersistentFlags().StringSliceVar(
		brokers,
		"brokers",
		[]string{},
		"Comma-separated list of broker ip:port pairs (e.g."+
			" --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092')."+
			" Alternatively, you may set the REDPANDA_BROKERS environment"+
			" variable with the comma-separated list of broker addresses",
	)
	command.PersistentFlags().StringVar(
		configFile,
		"config",
		"",
		"Redpanda config file, if not set the file will be searched for"+
			" in $PWD or /etc/redpanda/redpanda.yaml",
	)
	command.PersistentFlags().StringVar(
		user,
		"user",
		"",
		"SASL user to be used for authentication",
	)
	command.PersistentFlags().StringVar(
		password,
		"password",
		"",
		"SASL password to be used for authentication",
	)
	command.PersistentFlags().StringVar(
		saslMechanism,
		config.FlagSASLMechanism,
		"",
		"The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512",
	)

	AddTLSFlags(command, enableTLS, certFile, keyFile, truststoreFile)

	return command
}

func AddTLSFlags(
	command *cobra.Command,
	enableTLS *bool,
	certFile, keyFile, truststoreFile *string,
) *cobra.Command {
	command.PersistentFlags().BoolVar(
		enableTLS,
		config.FlagEnableTLS,
		false,
		"Enable TLS for the Kafka API (not necessary if specifying custom certs)",
	)
	command.PersistentFlags().StringVar(
		certFile,
		config.FlagTLSCert,
		"",
		"The certificate to be used for TLS authentication with the broker",
	)
	command.PersistentFlags().StringVar(
		keyFile,
		config.FlagTLSKey,
		"",
		"The certificate key to be used for TLS authentication with the broker",
	)
	command.PersistentFlags().StringVar(
		truststoreFile,
		config.FlagTLSCA,
		"",
		"The truststore to be used for TLS communication with the broker",
	)

	return command
}

func AddAdminAPITLSFlags(
	command *cobra.Command,
	enableTLS *bool,
	certFile, keyFile, truststoreFile *string,
) *cobra.Command {
	command.PersistentFlags().BoolVar(
		enableTLS,
		config.FlagEnableAdminTLS,
		false,
		"Enable TLS for the Admin API (not necessary if specifying custom certs)",
	)
	command.PersistentFlags().StringVar(
		certFile,
		config.FlagAdminTLSCert,
		"",
		"The certificate to be used for TLS authentication with the Admin API",
	)
	command.PersistentFlags().StringVar(
		keyFile,
		config.FlagAdminTLSKey,
		"",
		"The certificate key to be used for TLS authentication with the Admin API",
	)
	command.PersistentFlags().StringVar(
		truststoreFile,
		config.FlagAdminTLSCA,
		"",
		"The truststore to be used for TLS communication with the Admin API",
	)

	return command
}
