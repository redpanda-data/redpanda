// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package configuration

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
)

const (
	floatPrecision = 1e-6

	schemaMismatchInfoLog = "Warning: property values do not match their type"
)

// CentralConfigurationPatch represents a patch for the redpanda admin API
type CentralConfigurationPatch struct {
	Upsert map[string]interface{}
	Remove []string
}

// String gives a concise representation of the patch
func (p CentralConfigurationPatch) String() string {
	upsert := make([]string, 0, len(p.Upsert))
	for k := range p.Upsert {
		upsert = append(upsert, fmt.Sprintf("+%s", k))
	}
	remove := make([]string, 0, len(p.Remove))
	for _, r := range p.Remove {
		remove = append(remove, fmt.Sprintf("-%s", r))
	}
	sort.Strings(upsert)
	sort.Strings(remove)
	return strings.Join(append(upsert, remove...), " ")
}

// Empty tells if there's nothing to patch
func (p CentralConfigurationPatch) Empty() bool {
	return len(p.Upsert) == 0 && len(p.Remove) == 0
}

// ThreeWayMerge computes a patch given the new set of properties that the user wants to
// *apply* to the cluster, the *current* state of the cluster configuration and
// the *lastApplied* configuration in a previous iteration.
//
// Since the current configuration returned by the admin API does not include invalid properties,
// we require the list of invalid properties as fourth argument to compute the right patch.
//
// The schema will be used to do informed comparison of properties when we know they are of a certain type.
func ThreeWayMerge(
	log logr.Logger,
	apply map[string]interface{},
	current map[string]interface{},
	lastApplied map[string]interface{},
	invalidProperties []string,
	schema map[string]admin.ConfigPropertyMetadata,
) CentralConfigurationPatch {
	patch := CentralConfigurationPatch{
		// Initialize them early since nil values are rejected by the server
		Upsert: make(map[string]interface{}),
		Remove: make([]string, 0),
	}
	for k, v := range apply {
		if oldValue, ok := current[k]; !ok || !PropertiesEqual(log, v, oldValue, schema[k]) {
			patch.Upsert[k] = v
		}
	}
	invalidSet := make(map[string]bool, len(invalidProperties))
	for _, p := range invalidProperties {
		invalidSet[p] = true
	}
	for k := range lastApplied {
		_, isPresent := apply[k]
		_, wasPresent := current[k]
		wasPresent = wasPresent || invalidSet[k]
		if !isPresent && wasPresent {
			patch.Remove = append(patch.Remove, k)
		}
	}
	sort.Strings(patch.Remove)
	return patch
}

// PropertiesEqual tries to compare two property values using metadata information about the schema,
// falling back to loose comparison in case of missing data (e.g. it happens with unknown properties).
//
//nolint:gocritic // code more readable
func PropertiesEqual(
	log logr.Logger, v1, v2 interface{}, metadata admin.ConfigPropertyMetadata,
) bool {
	switch metadata.Type {
	case "number":
		if f1, f2, ok := bothFloat64(v1, v2); ok {
			return math.Abs(f1-f2) < floatPrecision
		}
		log.Info(schemaMismatchInfoLog, "type", metadata.Type, "v1", v1, "v2", v2)
	case "integer":
		if i1, i2, ok := bothInt64(v1, v2); ok {
			return i1 == i2
		}
		log.Info(schemaMismatchInfoLog, "type", metadata.Type, "v1", v1, "v2", v2)
	}
	// Other cases are correctly managed by LooseEqual
	return LooseEqual(v1, v2)
}

// LooseEqual try to determine if two given values are equal using loose comparison.
// Some example of problems:
// - Integers and their related string representations should be considered equal
// - Floating point values should match their integer or string representations if close to an integer number
// - JSON Number objects (e.g. obtained from JSON APIs) should match their string, integer or float representations
func LooseEqual(v1, v2 interface{}) bool {
	if i1, i2, ok := bothInt64(v1, v2); ok {
		return i1 == i2
	}
	if f1, f2, ok := bothFloat64(v1, v2); ok {
		return math.Abs(f1-f2) < floatPrecision
	}
	sv1 := fmt.Sprintf("%v", toComparableLooseEquivalent(v1))
	sv2 := fmt.Sprintf("%v", toComparableLooseEquivalent(v2))
	return sv1 == sv2
}

func bothInt64(v1, v2 interface{}) (i1, i2 int64, success bool) {
	i1, ok1 := convertibleToInt64(v1)
	i2, ok2 := convertibleToInt64(v2)
	if ok1 && ok2 {
		return i1, i2, true
	}
	return 0, 0, false
}

func bothFloat64(v1, v2 interface{}) (f1, f2 float64, success bool) {
	f1, ok1 := convertibleToFloat64(v1)
	f2, ok2 := convertibleToFloat64(v2)
	if ok1 && ok2 {
		return f1, f2, true
	}
	return 0, 0, false
}

func convertibleToInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case float32:
		if i, ok := float64ToInt64(float64(n)); ok {
			return i, true
		}
		return 0, false
	case float64:
		if i, ok := float64ToInt64(n); ok {
			return i, true
		}
		return 0, false
	case string:
		if i, err := strconv.ParseInt(n, 10, 64); err == nil {
			return i, true
		}
		return 0, false
	default:
		return 0, false
	}
}

func convertibleToFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case string:
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return f, true
		}
		return 0, false
	default:
		return 0, false
	}
}

func float64ToInt64(f float64) (int64, bool) {
	intNum := int64(math.Round(f))
	if math.Abs(float64(intNum)-f) < floatPrecision {
		return intNum, true
	}
	return 0, false
}

// toComparableLooseEquivalent converts special data into equivalent representation
// to make it loosely comparable.
func toComparableLooseEquivalent(v interface{}) interface{} {
	tryFloatToIntConvertion := func(f float64) interface{} {
		if i, ok := float64ToInt64(f); ok {
			return i
		}
		return f
	}
	switch d := v.(type) {
	case float32:
		return tryFloatToIntConvertion(float64(d))
	case float64:
		return tryFloatToIntConvertion(d)
	case json.Number:
		if intNum, err := d.Int64(); err == nil {
			return intNum
		}
		if floatNum, err := d.Float64(); err == nil {
			return tryFloatToIntConvertion(floatNum)
		}
	}
	return v
}
