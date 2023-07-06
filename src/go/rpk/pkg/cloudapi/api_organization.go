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

const orgPath = "/api/v1/organization"

type (
	// Organization contains all information for a namespace (not much).
	Organization struct {
		NameID
	}
)

// Organization returns the current organization.
func (cl *Client) Organization(ctx context.Context) (Organization, error) {
	var o Organization
	err := cl.cl.Get(ctx, orgPath, nil, &o)
	return o, err
}
