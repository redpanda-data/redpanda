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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
)

func MigrateConfigurations(configs map[string]string, rp *v1alpha1.Redpanda) {
	if len(configs) > 0 {
		migrateTieredConfigs(configs, rp)
		migrateClusterConfigs(configs, rp)
	}
}

func migrateTieredConfigs(configs map[string]string, rp *v1alpha1.Redpanda) {
	storage := &v1alpha1.Storage{}
	if rp.Spec.ClusterSpec.Storage != nil {
		storage = rp.Spec.ClusterSpec.Storage
	}

	if storage.TieredConfig == nil {
		storage.TieredConfig = &v1alpha1.TieredConfig{}
	}

	tieredConfig := &v1alpha1.TieredConfig{}
	tieredMap := make(map[string]interface{}, 0)
	for k, v := range configs {
		if strings.Contains(k, "cloud_storage") {
			key := strings.TrimPrefix(k, "redpanda.")
			if val, err := strconv.ParseBool(v); err == nil {
				tieredMap[key] = val
				continue
			}

			if val, err := strconv.ParseInt(v, 10, 64); err == nil {
				tieredMap[key] = val
				continue
			}

			tieredMap[key] = v
		}
	}

	// Convert the map to JSON
	jsonData, err := json.Marshal(tieredMap)
	if err != nil {
		fmt.Printf("error in marshalling data: %s\n", err)
	}

	// Convert the JSON to a struct
	err = json.Unmarshal(jsonData, &tieredConfig)
	if err != nil {
		fmt.Printf("error in unmarshalling data: %s\n", err)
	}

	storage.TieredConfig = tieredConfig
	rp.Spec.ClusterSpec.Storage = storage
}

func migrateClusterConfigs(configs map[string]string, rp *v1alpha1.Redpanda) {
	rpConfigs := &v1alpha1.Config{}
	if rp.Spec.ClusterSpec.Config != nil {
		rpConfigs = rp.Spec.ClusterSpec.Config
	}

	clusterConfigs := make(map[string]interface{}, 0)
	for k, v := range configs {
		if !strings.Contains(k, "cloud_storage") {
			key := strings.TrimPrefix(k, "redpanda.")
			if val, err := strconv.ParseBool(v); err == nil {
				clusterConfigs[key] = val
				continue
			}

			if val, err := strconv.ParseInt(v, 10, 64); err == nil {
				clusterConfigs[key] = val
				continue
			}

			clusterConfigs[key] = v
		}
	}

	// Convert the map to JSON
	jsonData, err := json.Marshal(clusterConfigs)
	if err != nil {
		fmt.Printf("error in marshalling data: %s\n", err)
	}

	jsonRuntime := &runtime.RawExtension{}
	err = jsonRuntime.UnmarshalJSON(jsonData)
	if err != nil {
		fmt.Printf("error in unmarshalling data: %s\n", err)
	}
	rpConfigs.Cluster = jsonRuntime
	rp.Spec.ClusterSpec.Config = rpConfigs
}
