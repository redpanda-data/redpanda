package patch

import (
	"fmt"

	"github.com/banzaicloud/k8s-objectmatcher/patch"
	json "github.com/json-iterator/go"
)

// IgnoreClusterIPFields ...
func IgnoreClusterIPFields() patch.CalculateOption {
	return func(current, modified []byte) ([]byte, []byte, error) {
		current, err := deleteClusterIP(current)
		if err != nil {
			return []byte{}, []byte{}, fmt.Errorf("could not delete clusterip field from current byte sequence: %w", err)
		}

		modified, err = deleteClusterIP(modified)
		if err != nil {
			return []byte{}, []byte{}, fmt.Errorf("could not delete clusterip field from modified byte sequence. %w", err)
		}

		return current, modified, nil
	}
}

func deleteClusterIP(obj []byte) ([]byte, error) {
	resource := map[string]interface{}{}
	err := json.Unmarshal(obj, &resource)
	if err != nil {
		return []byte{}, fmt.Errorf("could not unmarshal byte sequence: %w", err)
	}

	if spec, ok := resource["spec"]; ok {
		if spec, ok := spec.(map[string]interface{}); ok {
			spec["clusterIp"] = ""
		}
	}
	obj, err = json.ConfigCompatibleWithStandardLibrary.Marshal(resource)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal byte sequence: %w", err)
	}

	return obj, nil
}
