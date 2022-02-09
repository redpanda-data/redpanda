// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package admin

import "net/http"

func (a *AdminAPI) PrometheusMetrics() ([]byte, error) {
	var res []byte
	err := a.sendOne(http.MethodGet, "/metrics", nil, &res, false)
	return res, err
}
