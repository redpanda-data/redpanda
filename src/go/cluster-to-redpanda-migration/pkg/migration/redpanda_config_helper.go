// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package migration

import (
	"fmt"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	vectorizedv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/vectorized/v1alpha1"
	"k8s.io/utils/pointer"
)

var (
	kafkaExternalNamesTmpl = "kafka-external-%d"
	kafkaInternalNamesTmpl = "kafka-internal-%d"

	kafkaDefaultListener = "kafka-default"

	schemaRegistryInternalNamesTmpl = "schemaregistry-internal-%d"

	adminAPIInternalNamesTmpl = "adminapi-internal-%d"
	adminAPIExternalNamesTmpl = "adminapi-external-%d"

	httpInternalNamesTmpl = "http-internal-%d"
	httpExternalNamesTmpl = "http-external-%d"
)

func MigrateKafkaAPI(oldKafka *vectorizedv1alpha1.KafkaAPI,
	migratedKafkaAPI *redpandav1alpha1.Kafka,
	tls *redpandav1alpha1.TLS,
	rpExt *redpandav1alpha1.External,
) {
	if migratedKafkaAPI == nil {
		return
	}

	if tls.Certs == nil {
		tls.Certs = make(map[string]*redpandav1alpha1.Certificate, 0)
	}

	oldExternal := oldKafka.External
	if !oldExternal.Enabled {
		migratedKafkaAPI.Port = pointer.Int(oldKafka.Port)

		authenticationMethod := oldKafka.AuthenticationMethod
		if authenticationMethod == "" {
			authenticationMethod = "none"
		}

		migratedKafkaAPI.AuthenticationMethod = &authenticationMethod

		if oldKafka.TLS.Enabled {
			cert := fmt.Sprintf(kafkaInternalNamesTmpl, 0)
			tls.Certs[cert] = GetCertificateFromKafkaTLS(oldKafka.TLS)
			migratedKafkaAPI.TLS = &redpandav1alpha1.ListenerTLS{
				Cert:              pointer.String(cert),
				Enabled:           pointer.Bool(oldKafka.TLS.Enabled),
				RequireClientAuth: pointer.Bool(oldKafka.TLS.RequireClientAuth),
			}
		}

		// we return since we are done with internal case
		return
	}

	// TODO: what if there are listeners with different subdomains
	if oldExternal.Subdomain != "" {
		rpExt.Domain = pointer.String(oldExternal.Subdomain)
	}

	count := len(migratedKafkaAPI.External)

	// create new external object to add
	name := fmt.Sprintf(kafkaExternalNamesTmpl, count)
	if count == 0 {
		name = kafkaDefaultListener
	}

	var rpExtListener *redpandav1alpha1.ExternalListener

	if oldKafka.TLS.Enabled {
		rpExtListener = &redpandav1alpha1.ExternalListener{
			Port: pointer.Int(oldKafka.Port),
			TLS: &redpandav1alpha1.ListenerTLS{
				Cert:              &name,
				Enabled:           pointer.Bool(true),
				RequireClientAuth: pointer.Bool(oldKafka.TLS.RequireClientAuth),
			},
			AdvertisedPorts: make([]int, 0),
		}

		tls.Certs[name] = GetCertificateFromKafkaTLS(oldKafka.TLS)

	} else {
		rpExtListener = &redpandav1alpha1.ExternalListener{
			Port: pointer.Int(oldKafka.Port),
			TLS: &redpandav1alpha1.ListenerTLS{
				Cert:              pointer.String(""),
				Enabled:           pointer.Bool(false),
				RequireClientAuth: pointer.Bool(oldKafka.TLS.RequireClientAuth),
			},
			AdvertisedPorts: make([]int, 0),
		}
	}

	migratedKafkaAPI.External[name] = rpExtListener
}

func GetCertificateFromKafkaTLS(tls vectorizedv1alpha1.KafkaAPITLS) *redpandav1alpha1.Certificate {
	var issuerRef *redpandav1alpha1.IssuerRef = nil
	if tls.IssuerRef != nil {
		issuerRef = &redpandav1alpha1.IssuerRef{
			Name: tls.IssuerRef.Name,
			Kind: tls.IssuerRef.Kind,
		}
	}

	var secretRef *redpandav1alpha1.SecretRef = nil
	if tls.NodeSecretRef != nil {
		secretRef = &redpandav1alpha1.SecretRef{
			Name: tls.NodeSecretRef.Name,
		}
	}

	return &redpandav1alpha1.Certificate{
		IssuerRef: issuerRef,
		SecretRef: secretRef,
		// TODO: verify that you are required to have a CA always
		CAEnabled: true,
	}
}

// MigrateSchemaRegistry sets listener settings
// Schema Registry seems to only allow for one set of internal and external settings,
// Both exist in the same object, so instead of going through a list we only migrate
// one item that contains both pieces of information.
func MigrateSchemaRegistry(oldSchemaRegistry *vectorizedv1alpha1.SchemaRegistryAPI,
	migratedSchemaRegistry *redpandav1alpha1.SchemaRegistry,
	tls *redpandav1alpha1.TLS,
	rpExt *redpandav1alpha1.External,
) {
	if oldSchemaRegistry == nil {
		return
	}

	if tls.Certs == nil {
		tls.Certs = make(map[string]*redpandav1alpha1.Certificate, 0)
	}

	migratedSchemaRegistry.Port = pointer.Int(oldSchemaRegistry.Port)
	migratedSchemaRegistry.Enabled = pointer.Bool(true) // does not exist in old schema

	authenticationMethod := oldSchemaRegistry.AuthenticationMethod
	if authenticationMethod == "" {
		authenticationMethod = "none"
	}
	migratedSchemaRegistry.AuthenticationMethod = &authenticationMethod

	migratedSchemaRegistry.KafkaEndpoint = &kafkaDefaultListener
	// update internal certificate
	if oldSchemaRegistry.TLS != nil {
		if oldSchemaRegistry.TLS.Enabled {
			cert := fmt.Sprintf(schemaRegistryInternalNamesTmpl, 0)
			tls.Certs[cert] = GetCertificateFromSchemaRegistryAPITLS(oldSchemaRegistry.TLS)

			migratedSchemaRegistry.TLS = &redpandav1alpha1.ListenerTLS{
				Cert:              &cert,
				Enabled:           pointer.Bool(oldSchemaRegistry.TLS.Enabled),
				RequireClientAuth: pointer.Bool(oldSchemaRegistry.TLS.RequireClientAuth),
			}
		}
	}

	// we can exit here early since we are not going through a list here
	oldExternal := oldSchemaRegistry.External
	if oldExternal == nil || !oldExternal.Enabled {
		return
	}

	// TODO: what if there are listeners with different subdomains
	if oldExternal.Subdomain != "" {
		rpExt.Domain = pointer.String(oldExternal.Subdomain)
	}

	// TODO: There does not seem to be external here that we can translate
}

// TODO: This may require two certificate objects to be created but it is not clear
func GetCertificateFromSchemaRegistryAPITLS(tls *vectorizedv1alpha1.SchemaRegistryAPITLS) *redpandav1alpha1.Certificate {
	var issuerRef *redpandav1alpha1.IssuerRef = nil
	if tls.IssuerRef != nil {
		issuerRef = &redpandav1alpha1.IssuerRef{
			Name: tls.IssuerRef.Name,
			Kind: tls.IssuerRef.Kind,
		}
	}

	var secretRef *redpandav1alpha1.SecretRef = nil
	if tls.NodeSecretRef != nil {
		secretRef = &redpandav1alpha1.SecretRef{
			Name: tls.NodeSecretRef.Name,
		}
	}

	// TODO: verify if this is the right approach, since this will contain only a CA while the
	//if tls.RequireClientAuth && tls.ClientCACertRef != nil {
	//	secretRef = &redpandav1alpha1.SecretRef{
	//		Name: tls.ClientCACertRef.Name,
	//	}
	//}

	return &redpandav1alpha1.Certificate{
		IssuerRef: issuerRef,
		SecretRef: secretRef,
		// TODO: verify that you are required to have a CA always
		CAEnabled: true,
	}
}

func MigrateAdminAPI(clusterName string,
	oldAdminAPI *vectorizedv1alpha1.AdminAPI,
	migratedAdminAPI *redpandav1alpha1.Admin,
	tls *redpandav1alpha1.TLS,
	rpExt *redpandav1alpha1.External,
) {
	if oldAdminAPI == nil {
		return
	}

	if tls.Certs == nil {
		tls.Certs = make(map[string]*redpandav1alpha1.Certificate, 0)
	}

	if !oldAdminAPI.External.Enabled {
		migratedAdminAPI.Port = pointer.Int(oldAdminAPI.Port)

		// update internal certificate
		if oldAdminAPI.TLS.Enabled {
			cert := fmt.Sprintf(adminAPIInternalNamesTmpl, 0)
			tls.Certs[cert] = GetCertificateFromAdminAPITLS(clusterName, &oldAdminAPI.TLS)

			if cert != "" {
				migratedAdminAPI.TLS = &redpandav1alpha1.ListenerTLS{
					Cert:              &cert,
					Enabled:           pointer.Bool(oldAdminAPI.TLS.Enabled),
					RequireClientAuth: pointer.Bool(oldAdminAPI.TLS.RequireClientAuth),
				}
			}
		}

		// we return since we are done with internal case
		return
	}

	if oldAdminAPI.External.Subdomain != "" {
		rpExt.Domain = pointer.String(oldAdminAPI.External.Subdomain)
	}
	// TODO: how do we determine which item is default?
	count := len(migratedAdminAPI.External)

	// create new external object to add
	name := fmt.Sprintf(adminAPIExternalNamesTmpl, count)
	if count == 0 {
		name = "adminapi-default"
	}

	var rpExternalListeners *redpandav1alpha1.ExternalListener

	if oldAdminAPI.TLS.Enabled {
		rpExternalListeners = &redpandav1alpha1.ExternalListener{
			Port: pointer.Int(oldAdminAPI.Port),
			TLS: &redpandav1alpha1.ListenerTLS{
				Cert:              &name,
				Enabled:           pointer.Bool(oldAdminAPI.TLS.Enabled),
				RequireClientAuth: pointer.Bool(oldAdminAPI.TLS.RequireClientAuth),
			},
			AdvertisedPorts: make([]int, 0),
		}

		tls.Certs[name] = GetCertificateFromAdminAPITLS(clusterName, &oldAdminAPI.TLS)

	} else {
		rpExternalListeners = &redpandav1alpha1.ExternalListener{
			Port: pointer.Int(oldAdminAPI.Port),
			TLS: &redpandav1alpha1.ListenerTLS{
				Cert:              pointer.String(""),
				Enabled:           pointer.Bool(false),
				RequireClientAuth: pointer.Bool(oldAdminAPI.TLS.RequireClientAuth),
			},
			AdvertisedPorts: make([]int, 0),
		}
	}

	migratedAdminAPI.External[name] = rpExternalListeners
}

func GetCertificateFromAdminAPITLS(clusterName string, tls *vectorizedv1alpha1.AdminAPITLS) *redpandav1alpha1.Certificate {
	var secretRef *redpandav1alpha1.SecretRef = nil
	if tls.Enabled {
		secretRef = &redpandav1alpha1.SecretRef{
			Name: fmt.Sprintf("%s-admin-api-node", clusterName),
		}
	}

	// TODO: verify if this is the right approach, since this will contain only a CA while the
	if tls.RequireClientAuth {
		secretRef = &redpandav1alpha1.SecretRef{
			Name: fmt.Sprintf("%s-admin-api-client", clusterName),
		}
	}

	return &redpandav1alpha1.Certificate{
		SecretRef: secretRef,
		// TODO: verify that you are required to have a CA always
		CAEnabled: true,
	}
}

func MigratePandaProxy(oldProxyAPI *vectorizedv1alpha1.PandaproxyAPI,
	migratedHTTP *redpandav1alpha1.HTTP,
	tls *redpandav1alpha1.TLS,
	rpExt *redpandav1alpha1.External,
) {
	if oldProxyAPI == nil {
		return
	}

	if tls.Certs == nil {
		tls.Certs = make(map[string]*redpandav1alpha1.Certificate, 0)
	}

	if !oldProxyAPI.External.Enabled {
		migratedHTTP.Port = pointer.Int(oldProxyAPI.Port)
		migratedHTTP.Enabled = pointer.Bool(true) // does not exist in old schema

		authenticationMethod := oldProxyAPI.AuthenticationMethod
		if authenticationMethod == "" {
			authenticationMethod = "none"
		}

		migratedHTTP.AuthenticationMethod = &authenticationMethod
		migratedHTTP.KafkaEndpoint = &kafkaDefaultListener

		// update internal certificate
		if oldProxyAPI.TLS.Enabled {
			cert := fmt.Sprintf(httpInternalNamesTmpl, 0)
			tls.Certs[cert] = GetCertificateFromPandaProxy(&oldProxyAPI.TLS)

			migratedHTTP.TLS = &redpandav1alpha1.ListenerTLS{
				Cert:              &cert,
				Enabled:           pointer.Bool(oldProxyAPI.TLS.Enabled),
				RequireClientAuth: pointer.Bool(oldProxyAPI.TLS.RequireClientAuth),
			}
		}

		// we return since we are done with internal case
		return
	}

	if oldProxyAPI.External.Subdomain != "" {
		rpExt.Domain = pointer.String(oldProxyAPI.External.Subdomain)
	}

	// TODO: how do we determine which item is default?
	count := len(migratedHTTP.External)

	// create new external object to add
	name := fmt.Sprintf(httpExternalNamesTmpl, count)
	if count == 0 {
		name = "http-default"
	}

	var rpExternalListeners *redpandav1alpha1.ExternalListener

	if oldProxyAPI.TLS.Enabled {
		rpExternalListeners = &redpandav1alpha1.ExternalListener{
			Port: pointer.Int(oldProxyAPI.Port),
			TLS: &redpandav1alpha1.ListenerTLS{
				Cert:              &name,
				Enabled:           pointer.Bool(true),
				RequireClientAuth: pointer.Bool(oldProxyAPI.TLS.RequireClientAuth),
			},
			AdvertisedPorts: make([]int, 0),
		}

		tls.Certs[name] = GetCertificateFromPandaProxy(&oldProxyAPI.TLS)

	} else {
		rpExternalListeners = &redpandav1alpha1.ExternalListener{
			Port: pointer.Int(oldProxyAPI.Port),
			TLS: &redpandav1alpha1.ListenerTLS{
				Cert:              pointer.String(""),
				Enabled:           pointer.Bool(false),
				RequireClientAuth: pointer.Bool(oldProxyAPI.TLS.RequireClientAuth),
			},
			AdvertisedPorts: make([]int, 0),
		}
	}

	migratedHTTP.External[name] = rpExternalListeners
}

func GetCertificateFromPandaProxy(tls *vectorizedv1alpha1.PandaproxyAPITLS) *redpandav1alpha1.Certificate {
	var issuerRef *redpandav1alpha1.IssuerRef = nil
	if tls.IssuerRef != nil {
		issuerRef = &redpandav1alpha1.IssuerRef{
			Name: tls.IssuerRef.Name,
			Kind: tls.IssuerRef.Kind,
		}
	}

	var secretRef *redpandav1alpha1.SecretRef = nil
	if tls.NodeSecretRef != nil {
		secretRef = &redpandav1alpha1.SecretRef{
			Name: tls.NodeSecretRef.Name,
		}
	}

	// TODO: verify if this is the right approach, since this will contain only a CA
	//if tls.RequireClientAuth && tls.ClientCACertRef != nil {
	//	secretRef = &redpandav1alpha1.SecretRef{
	//		Name: tls.ClientCACertRef.Name,
	//	}
	//}

	return &redpandav1alpha1.Certificate{
		IssuerRef: issuerRef,
		SecretRef: secretRef,
		// TODO: verify that you are required to have a CA always
		CAEnabled: true,
	}
}

func MigrateRPCServer(oldRPCServer *vectorizedv1alpha1.SocketAddress, migratedRPC *redpandav1alpha1.RPC,
	tls *redpandav1alpha1.TLS,
) {
	if oldRPCServer == nil {
		return
	}

	migratedRPC.Port = pointer.Int(oldRPCServer.Port)
}
