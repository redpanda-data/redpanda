package migration

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
)

func MigrateConfigurations(configs map[string]string, rp *v1alpha1.Redpanda) {
	if len(configs) > 0 {
		migrateTieredConfigs(configs, rp)
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
