// Copyright 2022 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package networking

import corev1 "k8s.io/api/core/v1"

const defaultType = corev1.NodeExternalIP

// GetPreferredAddress returns the preferred node address. If a preference
// is provided it finds that address. If there is no preference, the default
// preference is used. The default preference is set to ExternalIP for
// backwards compatibility. If the preferred address is missing an empty
// address is returned (and does not return an alternative address).
func GetPreferredAddress(
	node *corev1.Node, preferredType corev1.NodeAddressType,
) string {
	if node == nil {
		return ""
	}
	if preferredType == "" {
		preferredType = defaultType
	}
	// Return the address of the preferred type, if found
	if s := findAddressType(node, preferredType); s != "" {
		return s
	}
	return ""
}

func findAddressType(
	node *corev1.Node, addressType corev1.NodeAddressType,
) string {
	for _, address := range node.Status.Addresses {
		if address.Type == addressType {
			return address.Address
		}
	}
	return ""
}
