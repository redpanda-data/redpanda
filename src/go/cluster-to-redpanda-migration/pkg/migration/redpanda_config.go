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

	"k8s.io/utils/pointer"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	vectorizedv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/vectorized/v1alpha1"
)

func MigrateRedpandaConfig(cluster *vectorizedv1alpha1.Cluster, rp *v1alpha1.Redpanda) {
	oldConfig := cluster.Spec.Configuration

	rpSpec := &v1alpha1.RedpandaClusterSpec{}
	if rp.Spec.ClusterSpec != nil {
		rpSpec = rp.Spec.ClusterSpec
	}

	rpStatefulset := &v1alpha1.Statefulset{}
	if rpSpec.Statefulset != nil {
		rpStatefulset = rpSpec.Statefulset
	}

	rpListeners := &v1alpha1.Listeners{}
	if rpSpec.Listeners != nil {
		rpListeners = rpSpec.Listeners
	}

	rpExternal := &v1alpha1.External{}
	if rpSpec.External != nil {
		rpExternal = rpSpec.External
	}

	// --- Additional Command line ---
	if len(oldConfig.AdditionalCommandlineArguments) > 0 {
		if rpStatefulset.AdditionalRedpandaCmdFlags == nil {
			rpStatefulset.AdditionalRedpandaCmdFlags = make([]string, 0)
		}
		for k, v := range oldConfig.AdditionalCommandlineArguments {
			if v != "" {
				rpStatefulset.AdditionalRedpandaCmdFlags = append(rpStatefulset.AdditionalRedpandaCmdFlags, fmt.Sprintf("--%s=%s", k, v))
			} else {
				rpStatefulset.AdditionalRedpandaCmdFlags = append(rpStatefulset.AdditionalRedpandaCmdFlags, fmt.Sprintf("--%s", k))
			}
		}
	}

	// --- Developer mode set ---
	var rpLog *v1alpha1.Logging = nil
	if oldConfig.DeveloperMode {
		rpLog = &v1alpha1.Logging{}
		if rpSpec.Logging != nil {
			rpLog = rpSpec.Logging
		}
		rpLog.LogLevel = "trace"
	}

	// --- Listeners ---

	// Migrate Certificates required here:
	rpTLS := &v1alpha1.TLS{}
	if rpSpec.TLS != nil {
		rpTLS = rpSpec.TLS
	}

	if oldConfig.KafkaAPI != nil && len(oldConfig.KafkaAPI) > 0 {
		kafkaListener := &v1alpha1.Kafka{}
		if rpListeners.Kafka != nil {
			kafkaListener = rpSpec.Listeners.Kafka
		}

		if kafkaListener.External == nil {
			kafkaListener.External = make(map[string]*v1alpha1.ExternalListener, 0)
		}

		for i := range oldConfig.KafkaAPI {
			toMigrate := oldConfig.KafkaAPI[i]
			MigrateKafkaAPI(&toMigrate, kafkaListener, rpTLS, rpExternal)
		}

		rpListeners.Kafka = kafkaListener

	}

	if oldConfig.SchemaRegistry != nil {
		schemaRegistryListener := &v1alpha1.SchemaRegistry{}
		if rpListeners.SchemaRegistry != nil {
			schemaRegistryListener = rpSpec.Listeners.SchemaRegistry
		}

		if schemaRegistryListener.External == nil {
			schemaRegistryListener.External = make(map[string]*v1alpha1.ExternalListener, 0)
		}

		MigrateSchemaRegistry(oldConfig.SchemaRegistry, schemaRegistryListener, rpTLS, rpExternal)

		rpListeners.SchemaRegistry = schemaRegistryListener
	}

	if oldConfig.AdminAPI != nil && len(oldConfig.AdminAPI) > 0 {
		adminAPIListener := &v1alpha1.Admin{}
		if rpListeners.Admin != nil {
			adminAPIListener = rpSpec.Listeners.Admin
		}

		if adminAPIListener.External == nil {
			adminAPIListener.External = make(map[string]*v1alpha1.ExternalListener, 0)
		}

		for i := range oldConfig.AdminAPI {
			toMigrate := oldConfig.AdminAPI[i]
			MigrateAdminAPI(cluster.Name, &toMigrate, adminAPIListener, rpTLS, rpExternal)
		}

		rpListeners.Admin = adminAPIListener
	}

	if oldConfig.PandaproxyAPI != nil && len(oldConfig.PandaproxyAPI) > 0 {
		httpListener := &v1alpha1.HTTP{}
		if rpListeners.HTTP != nil {
			httpListener = rpSpec.Listeners.HTTP
		}

		if httpListener.External == nil {
			httpListener.External = make(map[string]*v1alpha1.ExternalListener, 0)
		}

		for i := range oldConfig.PandaproxyAPI {
			toMigrate := oldConfig.PandaproxyAPI[i]
			MigratePandaProxy(&toMigrate, httpListener, rpTLS, rpExternal)
		}

		rpListeners.HTTP = httpListener
	}

	rpcListener := &v1alpha1.RPC{}
	if rpListeners.RPC != nil {
		rpcListener = rpSpec.Listeners.RPC
	}

	MigrateRPCServer(&oldConfig.RPCServer, rpcListener, rpTLS)

	if rpcListener.Port != nil && *rpcListener.Port != 0 {
		rpListeners.RPC = rpcListener
	} else {
		rpListeners.RPC = nil
	}

	// -- Putting everything together ---
	rpSpec.Statefulset = rpStatefulset
	rpSpec.Logging = rpLog

	// add TLS if it is not equal to the empty case.

	if rpTLS != nil {
		if rpTLS.Certs != nil && len(rpTLS.Certs) > 0 {
			rpTLS.Enabled = pointer.Bool(true)
			rpSpec.TLS = rpTLS
		} else if rpTLS.Certs != nil && len(rpTLS.Certs) == 0 {
			rpTLS.Enabled = pointer.Bool(false)
			rpSpec.TLS = rpTLS
		}
	}

	if rpListeners.Kafka == nil && rpListeners.Admin == nil && rpListeners.RPC == nil && rpListeners.HTTP == nil && rpListeners.SchemaRegistry == nil {
		rpListeners = nil
	}

	if rpExternal.Domain != nil && len(rpExternal.Addresses) == 0 {
		rp.Spec.ClusterSpec.External = rpExternal
	}

	rpSpec.Listeners = rpListeners
	rp.Spec.ClusterSpec = rpSpec
}
