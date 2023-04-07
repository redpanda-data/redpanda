// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package utils

import (
	"encoding/json"
	"fmt"

	"github.com/cisco-open/k8s-objectmatcher/patch"
	jsoniter "github.com/json-iterator/go"
)

// IgnoreAnnotation is a banzaicloud k8s-objectmatcher plugin that allows ignoring a specific annotation when computing a patch
// between two objects.
func IgnoreAnnotation(name string) patch.CalculateOption {
	return func(current, modified []byte) ([]byte, []byte, error) {
		var currentResource map[string]interface{}
		if err := json.Unmarshal(current, &currentResource); err != nil {
			return []byte{}, []byte{}, fmt.Errorf("could not unmarshal byte sequence for current: %w", err)
		}

		var modifiedResource map[string]interface{}
		if err := json.Unmarshal(modified, &modifiedResource); err != nil {
			return []byte{}, []byte{}, fmt.Errorf("could not unmarshal byte sequence for modified: %w", err)
		}

		if removeElement(currentResource, "metadata", "annotations", name) ||
			removeElement(currentResource, "spec", "template", "metadata", "annotations", name) {
			marsh, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(currentResource)
			if err != nil {
				return current, modified, fmt.Errorf("could not marshal current resource: %w", err)
			}
			current = marsh
		}

		if removeElement(modifiedResource, "metadata", "annotations", name) ||
			removeElement(modifiedResource, "spec", "template", "metadata", "annotations", name) {
			marsh, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(modifiedResource)
			if err != nil {
				return current, modified, fmt.Errorf("could not marshal modified resource: %w", err)
			}
			modified = marsh
		}

		return current, modified, nil
	}
}

func removeElement(m map[string]interface{}, path ...string) bool {
	if len(path) == 0 {
		return false
	}
	cur := m
	for i := 0; i < len(path)-1; i++ {
		if child, ok := cur[path[i]]; ok {
			childMap, convOk := child.(map[string]interface{})
			if !convOk {
				return false
			}
			cur = childMap
		}
	}
	lastKey := path[len(path)-1]
	_, exists := cur[lastKey]
	delete(cur, lastKey)
	return exists
}
