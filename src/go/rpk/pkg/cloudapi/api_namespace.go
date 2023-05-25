// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cloudapi

import (
	"context"
)

const namespacePath = "/api/v1/namespaces"

type (
	// Namespace contains all information for a namespace (not much).
	Namespace struct {
		NameID
	}

	// Namespaces is a set of Redpanda namespaces.
	Namespaces []Namespace
)

// Namespaces returns the list of namespaces in a Redpanda org.
func (cl *Client) Namespaces(ctx context.Context) (Namespaces, error) {
	var ns Namespaces
	err := cl.cl.Get(ctx, namespacePath, nil, &ns)
	return ns, err
}
