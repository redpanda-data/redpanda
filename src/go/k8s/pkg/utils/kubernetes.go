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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	separator = "-"
)

// IsPodReady tells if a given pod is ready looking at its status.
func IsPodReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}

	return false
}

// ResourceNameTrim builds a resource name out of a base name and a suffix, respecting k8s length constraints
func ResourceNameTrim(baseName, suffix string) string {
	suffixLength := len(suffix)
	maxNameLength := validation.DNS1123SubdomainMaxLength - suffixLength - len(separator)
	if len(baseName) > maxNameLength {
		baseName = baseName[:maxNameLength]
	}
	return fmt.Sprintf("%s%s%s", baseName, separator, suffix)
}
