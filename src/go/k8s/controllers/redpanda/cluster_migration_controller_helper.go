package redpanda

import (
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	vectorizedv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/vectorized/v1alpha1"
)

func migrateKafkaAPI(oldKafka *vectorizedv1alpha1.KafkaAPI) *redpandav1alpha1.Kafka {
	if oldKafka == nil {
		return nil
	}

	rpKafka := &redpandav1alpha1.Kafka{
		AuthenticationMethod: &oldKafka.AuthenticationMethod,
	}

	// --- External ---
	oldExternal := oldKafka.External
	rpExternal := &redpandav1alpha1.External{
		Enabled: oldExternal.Enabled,
		Domain:  oldExternal.Subdomain,
	}

	rpKafka.External = rpExternal

	return rpKafka

}

func migrateSchemaRegistry(oldSchemaRegistry *vectorizedv1alpha1.SchemaRegistryAPI) *redpandav1alpha1.SchemaRegistry {
	if oldSchemaRegistry == nil {
		return nil
	}

	return nil
}

func migrateAdminAPI(oldAdminAPI *vectorizedv1alpha1.AdminAPI) *redpandav1alpha1.Admin {
	if oldAdminAPI == nil {
		return nil
	}

	return nil
}

func migratePandaProxy(oldProxyAPI *vectorizedv1alpha1.PandaproxyAPI) *redpandav1alpha1.HTTP {
	if oldProxyAPI == nil {
		return nil
	}

	return nil
}

func migrateRPCServer(oldRPCServer *vectorizedv1alpha1.SocketAddress) *redpandav1alpha1.RPC {
	if oldRPCServer == nil {
		return nil
	}

	return nil
}
