// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package role

import (
	"k8s.io/apimachinery/pkg/util/validation"
)

// validateRoleNameForK8s reports whether the role name is a valid DNS-1123
// subdomain. Returns nil if the name is compliant; otherwise returns the
// validation messages from k8s.io/apimachinery so the caller can surface them
// to the user.
//
// The Redpanda Kubernetes operator binds a RedpandaRole CR's role name to its
// metadata.name, which the Kubernetes API server constrains to DNS-1123. Names
// that don't satisfy this constraint cannot be adopted by a RedpandaRole CR
// and will block migration to operator-managed roles.
func validateRoleNameForK8s(name string) []string {
	return validation.IsDNS1123Subdomain(name)
}
