// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package types contains useful types for dealing with resources
package types

import (
	"context"
	"crypto/tls"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// StatefulsetTLSVolumeProvider returns volumes and volume mounts that
// tatefulset needs to be able to support TLS config specified by cluster custom
// resource
type StatefulsetTLSVolumeProvider interface {
	Volumes() ([]corev1.Volume, []corev1.VolumeMount)
}

// BrokerTLSConfigProvider provides broker client config for kafka api
type BrokerTLSConfigProvider interface {
	KafkaClientBrokerTLS(mountPoints *TLSMountPoints) *config.ServerTLS
}

// AdminTLSConfigProvider returns TLS config for admin API
type AdminTLSConfigProvider interface {
	GetTLSConfig(ctx context.Context, k8sClient client.Reader) (*tls.Config, error)
}

// TLSMountPoint defines paths to be mounted
// We need 2 secrets and 2 mount points for each API endpoint that supports TLS and mTLS:
// 1. The Node certs used by the API endpoint to sign requests
// 2. The CA used to sign mTLS client certs which will be use by the endpoint to validate mTLS client certs
//
// Node certs might be signed by a provided Issuer (i.e. when using Letsencrypt), in which case
// the operator won't generate a CA to sign the node certs. But, if at the same time mTLS is enabled
// a CA will be created to sign the mTLS client certs, and it will be stored in a separate secret.
type TLSMountPoint struct {
	NodeCertMountDir string
	ClientCAMountDir string
}

// TLSMountPoints are mount points per API
type TLSMountPoints struct {
	KafkaAPI          *TLSMountPoint
	AdminAPI          *TLSMountPoint
	PandaProxyAPI     *TLSMountPoint
	SchemaRegistryAPI *TLSMountPoint
}

// GetTLSMountPoints returns configuration for all TLS mount paths for all
// redpanda APIs
func GetTLSMountPoints() *TLSMountPoints {
	return &TLSMountPoints{
		KafkaAPI: &TLSMountPoint{
			NodeCertMountDir: "/etc/tls/certs",
			ClientCAMountDir: "/etc/tls/certs/ca",
		},
		AdminAPI: &TLSMountPoint{
			NodeCertMountDir: "/etc/tls/certs/admin",
			ClientCAMountDir: "/etc/tls/certs/admin/ca",
		},
		PandaProxyAPI: &TLSMountPoint{
			NodeCertMountDir: "/etc/tls/certs/pandaproxy",
			ClientCAMountDir: "/etc/tls/certs/pandaproxy/ca",
		},
		SchemaRegistryAPI: &TLSMountPoint{
			NodeCertMountDir: "/etc/tls/certs/schema-registry",
			ClientCAMountDir: "/etc/tls/certs/schema-registry/ca",
		},
	}
}
