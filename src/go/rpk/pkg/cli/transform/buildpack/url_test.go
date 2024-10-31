/*
* Copyright 2024 Redpanda Data, Inc.
*
* Use of this software is governed by the Business Source License
* included in the file licenses/BSL.md
*
* As of the Change Date specified in that file, in accordance with
* the Business Source License, use of this software will be governed
* by the Apache License, Version 2.0
 */

package buildpack

import "testing"

func TestURLIsValidTemplate(t *testing.T) {
	// Just ensure these functions don't panic
	// as the templates are valid.
	t.Log(Tinygo.URL())
	t.Log(JavaScript.URL())
}
