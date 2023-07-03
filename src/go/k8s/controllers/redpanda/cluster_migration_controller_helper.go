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

	AdminAPICertNamesTmpl = "admin-external-%d"
)

func migrateTLS(rpConfig *vectorizedv1alpha1.RedpandaConfig, rpTLS *redpandav1alpha1.TLS) {
	if rpTLS == nil {
		return
	}

}

func migrateKafkaAPI(oldKafka vectorizedv1alpha1.KafkaAPI, migratedKafkaApi *redpandav1alpha1.Kafka, tls *redpandav1alpha1.TLS) {
	if migratedKafkaApi == nil {
		return
	}

	// TODO: how do we know which api is internal?
	isExternal := true
	oldExternal := oldKafka.External
	if oldExternal.Enabled == false && strings.HasPrefix(oldExternal.PreferredAddressType, "Internal") {
		isExternal = false

		migratedKafkaApi.Port = oldKafka.Port
		migratedKafkaApi.AuthenticationMethod = &oldKafka.AuthenticationMethod

		if migratedKafkaApi.TLS == nil {
			migratedKafkaApi.TLS = &redpandav1alpha1.ListenerTLS{
				Cert:              pointer.String(fmt.Sprintf(kafkaInternalCertNamesTmpl, 0)),
				Enabled:           pointer.Bool(oldKafka.TLS.Enabled),
				RequireClientAuth: oldKafka.TLS.RequireClientAuth,
			}
		}

		migratedKafkaApi.TLS.RequireClientAuth = oldKafka.TLS.RequireClientAuth
	}

	// --- External ---
	if !isExternal {
		return
	}

	// TODO: how do we determine which item is default?
	count := len(migratedKafkaApi.External)
	if migratedKafkaApi.External == nil || count == 0 {
		migratedKafkaApi.External = make(map[string]*redpandav1alpha1.ExternalListener, 0)
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
	}

	migratedKafkaApi.External[name] = rpExternal
}

func migrateSchemaRegistry(oldSchemaRegistry *vectorizedv1alpha1.SchemaRegistryAPI, tls *redpandav1alpha1.TLS) {
	if oldSchemaRegistry == nil {
		return
	}

	return
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
