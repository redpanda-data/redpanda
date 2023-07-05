package redpanda

import (
	"fmt"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	vectorizedv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/vectorized/v1alpha1"
	"k8s.io/utils/pointer"
	"strings"
)

var (
	kafkaExternalCertNamesTmpl = "kafka-external-%d"
	kafkaInternalCertNamesTmpl = "kafka-internal-%d"

	schemaRegistryExternalCertNamesTmpl = "schemaregistry-external-%d"
	schemaRegistryInternalCertNamesTmpl = "schemaregistry-internal-%d"

	AdminAPICertNamesTmpl = "admin-external-%d"
)

func migrateTLS(rpConfig *vectorizedv1alpha1.RedpandaConfig, rpTLS *redpandav1alpha1.TLS) {
	if rpTLS == nil {
		return
	}

}

func migrateKafkaAPI(oldKafka vectorizedv1alpha1.KafkaAPI, migratedKafkaAPI *redpandav1alpha1.Kafka, tls *redpandav1alpha1.TLS) {
	if migratedKafkaAPI == nil {
		return
	}

	if tls.Certs == nil {
		tls.Certs = make(map[string]*redpandav1alpha1.Certificate, 0)
	}

	// TODO: how do we know which api is internal?
	oldExternal := oldKafka.External
	if oldExternal.Enabled == false && strings.HasPrefix(oldExternal.PreferredAddressType, "Internal") {
		migratedKafkaAPI.Port = oldKafka.Port
		migratedKafkaAPI.AuthenticationMethod = &oldKafka.AuthenticationMethod

		if migratedKafkaAPI.TLS == nil {
			migratedKafkaAPI.TLS = &redpandav1alpha1.ListenerTLS{
				Cert:              pointer.String(fmt.Sprintf(kafkaInternalCertNamesTmpl, 0)),
				Enabled:           pointer.Bool(oldKafka.TLS.Enabled),
				RequireClientAuth: oldKafka.TLS.RequireClientAuth,
			}
		}

		// update internal certificate
		tls.Certs[fmt.Sprintf(kafkaInternalCertNamesTmpl, 0)] = getCertificateFromKafkaTLS(oldKafka.TLS)

		migratedKafkaAPI.TLS.RequireClientAuth = oldKafka.TLS.RequireClientAuth
		// we return since we are done with internal case
		return
	}

	// TODO: how do we determine which item is default?
	count := len(migratedKafkaAPI.External)
	if migratedKafkaAPI.External == nil || count == 0 {
		migratedKafkaAPI.External = make(map[string]*redpandav1alpha1.ExternalListener, 0)
	}

	// create new external object to add
	name := fmt.Sprintf(kafkaExternalCertNamesTmpl, count)
	if count == 0 {
		name = "kafka-default"
	}
	rpExternal := &redpandav1alpha1.ExternalListener{
		Port: oldKafka.Port,
		TLS: &redpandav1alpha1.ListenerTLS{
			Cert:              &name,
			Enabled:           pointer.Bool(oldKafka.TLS.Enabled),
			RequireClientAuth: oldKafka.TLS.RequireClientAuth,
		},
		AdvertisedPorts: make([]int, 0),
	}

	tls.Certs[name] = getCertificateFromKafkaTLS(oldKafka.TLS)

	migratedKafkaAPI.External[name] = rpExternal
}

func getCertificateFromKafkaTLS(tls vectorizedv1alpha1.KafkaAPITLS) *redpandav1alpha1.Certificate {
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

func migrateSchemaRegistry(oldSchemaRegistry *vectorizedv1alpha1.SchemaRegistryAPI,
	migratedSchemaRegistry *redpandav1alpha1.SchemaRegistry, tls *redpandav1alpha1.TLS) {
	if oldSchemaRegistry == nil {
		return
	}

	if tls.Certs == nil {
		tls.Certs = make(map[string]*redpandav1alpha1.Certificate, 0)
	}

	// TODO: how do we know which api is internal?
	oldExternal := oldSchemaRegistry.External
	if oldExternal.Enabled == false && strings.HasPrefix(oldExternal.PreferredAddressType, "Internal") {
		migratedSchemaRegistry.Port = oldSchemaRegistry.Port
		migratedSchemaRegistry.AuthenticationMethod = &oldSchemaRegistry.AuthenticationMethod

		if migratedSchemaRegistry.TLS == nil {
			migratedSchemaRegistry.TLS = &redpandav1alpha1.ListenerTLS{
				Cert:              pointer.String(fmt.Sprintf(kafkaInternalCertNamesTmpl, 0)),
				Enabled:           pointer.Bool(oldSchemaRegistry.TLS.Enabled),
				RequireClientAuth: oldSchemaRegistry.TLS.RequireClientAuth,
			}
		}

		// update internal certificate
		if oldSchemaRegistry.TLS != nil {
			tls.Certs[fmt.Sprintf(schemaRegistryInternalCertNamesTmpl, 0)] = getCertificateFromSchemaRegistryAPITLS(oldSchemaRegistry.TLS)
		}

		migratedSchemaRegistry.TLS.RequireClientAuth = oldSchemaRegistry.TLS.RequireClientAuth
		// we return since we are done with internal case
		return
	}

	// TODO: how do we determine which item is default?
	count := len(migratedSchemaRegistry.External)
	if migratedSchemaRegistry.External == nil || count == 0 {
		migratedSchemaRegistry.External = make(map[string]*redpandav1alpha1.ExternalListener, 0)
	}

	// create new external object to add
	name := fmt.Sprintf(schemaRegistryExternalCertNamesTmpl, count)
	if count == 0 {
		name = "schemaregistry-default"
	}
	rpExternal := &redpandav1alpha1.ExternalListener{
		Port: oldSchemaRegistry.Port,
		TLS: &redpandav1alpha1.ListenerTLS{
			Cert:              &name,
			Enabled:           pointer.Bool(oldSchemaRegistry.TLS.Enabled),
			RequireClientAuth: oldSchemaRegistry.TLS.RequireClientAuth,
		},
		AdvertisedPorts: make([]int, 0),
	}

	if oldSchemaRegistry.TLS != nil {
		tls.Certs[name] = getCertificateFromSchemaRegistryAPITLS(oldSchemaRegistry.TLS)
	}

	migratedSchemaRegistry.External[name] = rpExternal

}

// TODO: This may require two certificate objects to be created but it is not clear
func getCertificateFromSchemaRegistryAPITLS(tls *vectorizedv1alpha1.SchemaRegistryAPITLS) *redpandav1alpha1.Certificate {
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
	if tls.RequireClientAuth && tls.ClientCACertRef != nil {
		secretRef = &redpandav1alpha1.SecretRef{
			Name: tls.ClientCACertRef.Name,
		}
	}

	return &redpandav1alpha1.Certificate{
		IssuerRef: issuerRef,
		SecretRef: secretRef,
		// TODO: verify that you are required to have a CA always
		CAEnabled: true,
	}
}

func migrateAdminAPI(oldAdminAPI *vectorizedv1alpha1.AdminAPI, tls *redpandav1alpha1.TLS) {
	if oldAdminAPI == nil {
		return
	}

	return
}

func migratePandaProxy(oldProxyAPI *vectorizedv1alpha1.PandaproxyAPI, tls *redpandav1alpha1.TLS) {
	if oldProxyAPI == nil {
		return
	}

	return
}

func migrateRPCServer(oldRPCServer *vectorizedv1alpha1.SocketAddress, tls *redpandav1alpha1.TLS) {
	if oldRPCServer == nil {
		return
	}

	return
}
